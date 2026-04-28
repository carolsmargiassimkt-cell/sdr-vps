import json
import re
import unicodedata
import zipfile
import xml.etree.ElementTree as ET
import requests
import time
from pathlib import Path
from collections import Counter

CFG = Path('/root/sdr-vps/config/system_config.json')
MATCHED_JSON = Path('/root/sdr-vps/runtime/final_reconcile_remaining_95_super_matched.json')
XLSX = Path('/root/BASE_LEADS_COM_LINKEDIN.xlsx')
STATE_JSON = Path('/root/sdr-vps/runtime/apply_10_reconciled_matches_state.json')
RESULT_JSON = Path('/root/sdr-vps/runtime/apply_10_reconciled_matches_result.json')

NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
BATCH_LIMIT = 10
BATCH_DELAY_SEC = 5

def load_token():
    cfg = json.load(open(CFG))
    return cfg.get('pipedrive_token') or cfg.get('pipedrive_api_token') or ''

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

def api_get_deal(token, deal_id):
    r = requests.get(
        f'https://api.pipedrive.com/v1/deals/{deal_id}',
        params={'api_token': token},
        timeout=60,
    )
    if r.status_code >= 400:
        return {}
    return r.json().get('data') or {}

def api_update_org(token, org_id, payload):
    r = requests.put(
        f'https://api.pipedrive.com/v1/organizations/{org_id}',
        params={'api_token': token},
        json=payload,
        timeout=60,
    )
    return r.status_code, r.text

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

def api_update_person(token, person_id, payload):
    r = requests.put(
        f'https://api.pipedrive.com/v1/persons/{person_id}',
        params={'api_token': token},
        json=payload,
        timeout=60,
    )
    return r.status_code, r.text

def api_update_deal_person(token, deal_id, person_id):
    r = requests.put(
        f'https://api.pipedrive.com/v1/deals/{deal_id}',
        params={'api_token': token},
        json={'person_id': person_id},
        timeout=60,
    )
    return r.status_code, r.text

token = load_token()
state = load_state()
done = set(state.get('done', []))

rows = parse_xlsx(XLSX)
sheet_by_company = {}
sheet_by_title = {}

for row in rows:
    company = row.get('Organização - Nome') or ''
    title = row.get('Negócio - Título') or ''
    item = {
        'company': company,
        'title': title,
        'person_name': str(row.get('Pessoa - Nome') or '').strip(),
        'phones': dedupe([
            norm_phone(row.get('Pessoa - Telefone - Trabalho')),
            norm_phone(row.get('Pessoa - Telefone - Celular')),
            norm_phone(row.get('API_TEL_OFICIAL')),
        ]),
        'emails': dedupe([
            norm_email(row.get('API_EMAIL')),
        ]),
    }
    ck = norm_key(company)
    tk = norm_key(title)
    if ck and ck not in sheet_by_company:
        sheet_by_company[ck] = item
    if tk and tk not in sheet_by_title:
        sheet_by_title[tk] = item

matched = json.load(open(MATCHED_JSON))
results = []
counts = Counter()
processed = 0

print(f'[START] matched={len(matched)}', flush=True)

for m in matched:
    company = m.get('company') or ''
    deal_id = int(m.get('deal_id') or 0)
    key = f'{company}|{deal_id}'

    if key in done:
        continue

    result = {'company': company, 'deal_id': deal_id, 'status': ''}

    src = sheet_by_company.get(norm_key(company))
    if not src:
        result['status'] = 'needs_review'
        result['reason'] = 'no_sheet_company_match'
        results.append(result)
        done.add(key)
        save_state({'done': sorted(done)})
        counts[result['status']] += 1
        continue

    deal = api_get_deal(token, deal_id)
    if not deal:
        result['status'] = 'error'
        result['reason'] = 'deal_not_found'
        results.append(result)
        done.add(key)
        save_state({'done': sorted(done)})
        counts[result['status']] += 1
        continue

    org_id = extract_id(deal.get('org_id'))
    person_id = extract_id(deal.get('person_id'))

    canonical_name = src.get('company') or company
    phones = src.get('phones') or []
    emails = src.get('emails') or []
    person_name = src.get('person_name') or canonical_name

    if org_id and canonical_name:
        sc, body = api_update_org(token, org_id, {'name': canonical_name})
        if not (200 <= sc < 300):
            result['org_update_error'] = sc

    if person_id:
        payload = {}
        if phones:
            payload['phone'] = [{'value': p, 'primary': i == 0} for i, p in enumerate(phones)]
        if emails:
            payload['email'] = [{'value': e, 'primary': i == 0} for i, e in enumerate(emails)]
        if payload:
            sc, body = api_update_person(token, person_id, payload)
            if 200 <= sc < 300:
                result['status'] = 'updated_existing_person'
                result['person_id'] = person_id
                print(f'[UPDATED_EXISTING] deal={deal_id} person={person_id}', flush=True)
            else:
                result['status'] = 'error'
                result['reason'] = 'update_existing_person_failed'
                result['http_status'] = sc
        else:
            result['status'] = 'needs_review'
            result['reason'] = 'no_contact_data'
    else:
        if not phones and not emails:
            result['status'] = 'needs_review'
            result['reason'] = 'no_contact_data'
        else:
            payload = {'name': person_name, 'org_id': org_id}
            if phones:
                payload['phone'] = [{'value': p, 'primary': i == 0} for i, p in enumerate(phones)]
            if emails:
                payload['email'] = [{'value': e, 'primary': i == 0} for i, e in enumerate(emails)]
            sc1, body1, created = api_create_person(token, payload)
            if 200 <= sc1 < 300 and created.get('id'):
                new_person_id = int(created.get('id') or 0)
                sc2, body2 = api_update_deal_person(token, deal_id, new_person_id)
                if 200 <= sc2 < 300:
                    result['status'] = 'created_and_linked_person'
                    result['person_id'] = new_person_id
                    print(f'[CREATED_AND_LINKED] deal={deal_id} person={new_person_id}', flush=True)
                else:
                    result['status'] = 'error'
                    result['reason'] = 'link_new_person_failed'
                    result['http_status'] = sc2
            else:
                result['status'] = 'error'
                result['reason'] = 'create_person_failed'
                result['http_status'] = sc1

    result['phones'] = phones
    result['emails'] = emails
    results.append(result)
    done.add(key)
    save_state({'done': sorted(done)})
    counts[result['status']] += 1

    processed += 1
    if processed >= BATCH_LIMIT:
        print(f'[PAUSA] {BATCH_DELAY_SEC}s', flush=True)
        time.sleep(BATCH_DELAY_SEC)
        processed = 0

RESULT_JSON.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding='utf-8')
print('[FINAL_COUNTS]', flush=True)
for k, v in sorted(counts.items()):
    print(f'{k}={v}', flush=True)
print(f'[RESULT_JSON] {RESULT_JSON}', flush=True)
