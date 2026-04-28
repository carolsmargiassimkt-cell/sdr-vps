import json
import re
import unicodedata
import zipfile
import xml.etree.ElementTree as ET
import requests
from pathlib import Path
from collections import Counter

CFG = Path('/root/sdr-vps/config/system_config.json')
XLSX = Path('/root/BASE_LEADS_COM_LINKEDIN.xlsx')
STATE_JSON = Path('/root/sdr-vps/runtime/fill_39_missing_persons_from_sheet_cnpj_state.json')
RESULT_JSON = Path('/root/sdr-vps/runtime/fill_39_missing_persons_from_sheet_cnpj_result.json')

PIPELINE_ID = 2
LOST_STAGE_ID = 50
NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"

def load_tokens():
    cfg = json.load(open(CFG))
    pipedrive = cfg.get('pipedrive_token') or cfg.get('pipedrive_api_token') or ''
    oportun = ''
    try:
        txt = Path('/root/sdr-vps/super_minas_public_links_to_crm.py').read_text(encoding='utf-8', errors='ignore')
        m = re.search(r'OPORTUNIDADOS_TOKEN\s*=\s*"([^"]+)"', txt)
        if m:
            oportun = m.group(1).strip()
    except:
        pass
    return pipedrive, oportun

def strip_accents(v):
    s = str(v or '').strip()
    s = unicodedata.normalize('NFKD', s)
    return ''.join(ch for ch in s if not unicodedata.combining(ch))

def norm(v):
    s = strip_accents(v).upper()
    s = re.sub(r'\s*-\s*LEAD\s*$', '', s, flags=re.I)
    s = re.sub(r'\s*-\s*DEAL\s*$', '', s, flags=re.I)
    s = re.sub(r'\s*LEADSTER\s*$', '', s, flags=re.I)
    s = re.sub(r'\s+', ' ', s).strip(' -')
    return s

def norm_key(v):
    return re.sub(r'[^A-Z0-9]+', '', norm(v))

def norm_cnpj(v):
    d = re.sub(r'\D+', '', str(v or ''))
    return d if len(d) == 14 else ''

def norm_email(v):
    s = str(v or '').strip().lower()
    return s if '@' in s else ''

def norm_phone(v):
    d = re.sub(r'\D+', '', str(v or ''))
    if d.startswith('55') and len(d) > 11:
        d = d[2:]
    if len(d) not in {10, 11}:
        return ''
    if len(set(d)) <= 2:
        return ''
    return d

def dedupe(vals):
    out, seen = [], set()
    for v in vals:
        if v and v not in seen:
            seen.add(v)
            out.append(v)
    return out

def extract_id(v):
    if isinstance(v, dict):
        try:
            return int(v.get('value') or v.get('id') or 0)
        except:
            return 0
    try:
        return int(v or 0)
    except:
        return 0

def load_state():
    if not STATE_JSON.exists():
        return {'done': []}
    return json.load(open(STATE_JSON))

def save_state(state):
    STATE_JSON.parent.mkdir(parents=True, exist_ok=True)
    STATE_JSON.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding='utf-8')

def parse_xlsx(path):
    with zipfile.ZipFile(path) as z:
        root = ET.fromstring(z.read('xl/worksheets/sheet1.xml'))
    rows_raw = []
    for row in root.iter(NS + 'row'):
        vals = []
        for c in row:
            t = c.find('.//' + NS + 't')
            v = c.find('.//' + NS + 'v')
            vals.append(t.text if t is not None else (v.text if v is not None else ''))
        if vals:
            rows_raw.append(vals)
    header = rows_raw[0]
    out = []
    for vals in rows_raw[1:]:
        if len(vals) < len(header):
            vals += [''] * (len(header) - len(vals))
        out.append(dict(zip(header, vals)))
    return out

def api_get_all_deals(token, status='open'):
    out = []
    start = 0
    while True:
        r = requests.get(
            'https://api.pipedrive.com/v1/deals',
            params={'api_token': token, 'status': status, 'start': start, 'limit': 500},
            timeout=60,
        )
        r.raise_for_status()
        payload = r.json()
        batch = payload.get('data') or []
        out.extend(batch)
        pag = payload.get('additional_data', {}).get('pagination', {})
        if not pag.get('more_items_in_collection', False) or not batch:
            break
        start += 500
    return out

def api_create_person(token, payload):
    r = requests.post(
        'https://api.pipedrive.com/v1/persons',
        params={'api_token': token},
        json=payload,
        timeout=60,
    )
    data = {}
    try:
        data = r.json().get('data') or {}
    except:
        pass
    return r.status_code, r.text, data

def api_update_deal_person(token, deal_id, person_id):
    r = requests.put(
        f'https://api.pipedrive.com/v1/deals/{deal_id}',
        params={'api_token': token},
        json={'person_id': person_id},
        timeout=60,
    )
    return r.status_code, r.text

def fetch_oportunidados(token, cnpj):
    if not token or not cnpj:
        return 0, {}
    r = requests.get(
        f'https://app.oportunidados.com.br/api/v1/brazilian_companies/{cnpj}/company',
        headers={'Authorization': f'Bearer {token}'},
        timeout=60,
    )
    payload = {}
    try:
        payload = r.json()
    except:
        pass
    return r.status_code, payload

def collect_oportunidados(payload):
    data = payload.get('data') if isinstance(payload, dict) else None
    root = data if isinstance(data, dict) else payload if isinstance(payload, dict) else {}
    emails, phones = [], []
    person_name = ''

    for key in ['email', 'company_email']:
        e = norm_email(root.get(key))
        if e: emails.append(e)
    for key in ['phone', 'company_phone', 'phone_number', 'telephone']:
        p = norm_phone(root.get(key))
        if p: phones.append(p)

    for item in root.get('emails', []) if isinstance(root.get('emails'), list) else []:
        e = norm_email(item.get('email') if isinstance(item, dict) else item)
        if e: emails.append(e)

    for item in root.get('phones', []) if isinstance(root.get('phones'), list) else []:
        p = norm_phone(item.get('number') if isinstance(item, dict) else item)
        if p: phones.append(p)

    contacts = []
    for key in ['contacts', 'people', 'decision_makers', 'owners', 'partners']:
        val = root.get(key)
        if isinstance(val, list):
            contacts.extend(val)

    chosen = None
    for c in contacts:
        if not isinstance(c, dict):
            continue
        name = str(c.get('name') or c.get('full_name') or '').strip()
        email = norm_email(c.get('email'))
        phone = norm_phone(c.get('phone') or c.get('mobile') or c.get('telephone'))
        if name or email or phone:
            chosen = c
            break

    if chosen:
        person_name = str(chosen.get('name') or chosen.get('full_name') or '').strip()
        e = norm_email(chosen.get('email'))
        p = norm_phone(chosen.get('phone') or chosen.get('mobile') or chosen.get('telephone'))
        if e: emails.append(e)
        if p: phones.append(p)

    return {
        'person_name': person_name,
        'emails': dedupe(emails),
        'phones': dedupe(phones),
    }

pipedrive_token, oportun_token = load_tokens()
state = load_state()
done = set(state.get('done', []))

sheet = parse_xlsx(XLSX)
sheet_by_company = {}
sheet_by_title = {}
for r in sheet:
    item = {
        'company': r.get('Organização - Nome') or '',
        'title': r.get('Negócio - Título') or '',
        'person_name': str(r.get('Pessoa - Nome') or '').strip(),
        'cnpj': norm_cnpj(r.get('cnpj_formatado')),
        'emails': dedupe([norm_email(r.get('API_EMAIL'))]),
        'phones': dedupe([
            norm_phone(r.get('Pessoa - Telefone - Trabalho')),
            norm_phone(r.get('Pessoa - Telefone - Celular')),
            norm_phone(r.get('API_TEL_OFICIAL')),
        ]),
    }
    ck = norm_key(item['company'])
    tk = norm_key(item['title'])
    if ck and ck not in sheet_by_company:
        sheet_by_company[ck] = item
    if tk and tk not in sheet_by_title:
        sheet_by_title[tk] = item

open_p2 = [d for d in api_get_all_deals(pipedrive_token, 'open') if int(d.get('pipeline_id') or 0) == 2 and int(d.get('stage_id') or 0) != LOST_STAGE_ID]
missing = [d for d in open_p2 if not extract_id(d.get('person_id'))]

results = []
counts = Counter()

print(f'[START] missing_person={len(missing)}', flush=True)

for d in missing:
    deal_id = int(d.get('id') or 0)
    key = str(deal_id)
    if key in done:
        continue

    title = d.get('title') or ''
    org_name = d.get('org_name') or ''

    src = sheet_by_company.get(norm_key(org_name)) or sheet_by_title.get(norm_key(title)) or {}
    emails = src.get('emails', [])
    phones = src.get('phones', [])
    person_name = src.get('person_name') or org_name or title
    cnpj = src.get('cnpj') or ''

    if cnpj and oportun_token:
        sc, payload = fetch_oportunidados(oportun_token, cnpj)
        if sc and sc < 400 and payload:
            ext = collect_oportunidados(payload)
            emails = dedupe(emails + ext.get('emails', []))
            phones = dedupe(phones + ext.get('phones', []))
            if ext.get('person_name'):
                person_name = ext.get('person_name')

    row = {'deal_id': deal_id, 'title': title, 'org_name': org_name, 'status': ''}

    if emails or phones:
        payload = {
            'name': person_name or org_name or f'Contato {deal_id}',
            'org_id': extract_id(d.get('org_id')),
        }
        if emails:
            payload['email'] = [{'value': e, 'primary': i == 0} for i, e in enumerate(emails)]
        if phones:
            payload['phone'] = [{'value': p, 'primary': i == 0} for i, p in enumerate(phones)]

        sc1, body1, created = api_create_person(pipedrive_token, payload)
        if 200 <= sc1 < 300 and created.get('id'):
            new_person_id = int(created.get('id') or 0)
            sc2, body2 = api_update_deal_person(pipedrive_token, deal_id, new_person_id)
            if 200 <= sc2 < 300:
                row['status'] = 'fixed'
                row['person_id'] = new_person_id
                print(f'[FIXED] deal={deal_id} person={new_person_id}', flush=True)
            else:
                row['status'] = 'error'
                row['reason'] = 'link_failed'
        else:
            row['status'] = 'error'
            row['reason'] = 'create_person_failed'
    else:
        row['status'] = 'needs_review'
        row['reason'] = 'no_contact_data'

    results.append(row)
    done.add(key)
    save_state({'done': sorted(done)})
    counts[row['status']] += 1

RESULT_JSON.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding='utf-8')
print('[FINAL_COUNTS]', flush=True)
for k, v in sorted(counts.items()):
    print(f'{k}={v}', flush=True)
print(f'[RESULT_JSON] {RESULT_JSON}', flush=True)
