import json
import re
import unicodedata
import zipfile
import xml.etree.ElementTree as ET
import requests
import time
from pathlib import Path
from collections import defaultdict, Counter

CFG = Path('/root/sdr-vps/config/system_config.json')
XLSX = Path('/root/BASE_LEADS_COM_LINKEDIN.xlsx')
STATE_JSON = Path('/root/sdr-vps/runtime/final_safe_finish_today_state.json')
RESULT_JSON = Path('/root/sdr-vps/runtime/final_safe_finish_today_result.json')
SUMMARY_JSON = Path('/root/sdr-vps/runtime/final_safe_finish_today_summary.json')

PIPELINE_ID = 2
LOST_STAGE_ID = 50
BATCH_LIMIT = 10
BATCH_DELAY_SEC = 5

NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"

SKIP_DUP_TITLES = {
    'DROGAMARYS',
    'FARMACIA PRECO BAIXO',
    'FUNDO',
}

def log(msg):
    print(msg, flush=True)

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
    s = re.sub(r'\s*-\s*SORTEIO/VB\s*$', '', s, flags=re.I)
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

def api_get_all(endpoint, token, params_extra=None):
    out = []
    start = 0
    while True:
        params = {'api_token': token, 'start': start, 'limit': 500}
        if params_extra:
            params.update(params_extra)
        r = requests.get(f'https://api.pipedrive.com/v1/{endpoint}', params=params, timeout=60)
        r.raise_for_status()
        payload = r.json()
        batch = payload.get('data') or []
        out.extend(batch)
        pag = payload.get('additional_data', {}).get('pagination', {})
        if not pag.get('more_items_in_collection', False) or not batch:
            break
        start += 500
    return out

def api_get_deal(token, deal_id):
    r = requests.get(f'https://api.pipedrive.com/v1/deals/{deal_id}', params={'api_token': token}, timeout=60)
    if r.status_code >= 400:
        return {}
    return r.json().get('data') or {}

def api_get_org(token, org_id):
    r = requests.get(f'https://api.pipedrive.com/v1/organizations/{org_id}', params={'api_token': token}, timeout=60)
    if r.status_code >= 400:
        return {}
    return r.json().get('data') or {}

def api_get_person(token, person_id):
    r = requests.get(f'https://api.pipedrive.com/v1/persons/{person_id}', params={'api_token': token}, timeout=60)
    if r.status_code >= 400:
        return {}
    return r.json().get('data') or {}

def api_get_deal_participants(token, deal_id):
    r = requests.get(f'https://api.pipedrive.com/v1/deals/{deal_id}/participants', params={'api_token': token}, timeout=60)
    if r.status_code >= 400:
        return []
    return r.json().get('data') or []

def api_update_deal(token, deal_id, payload):
    r = requests.put(
        f'https://api.pipedrive.com/v1/deals/{deal_id}',
        params={'api_token': token},
        json=payload,
        timeout=60,
    )
    return r.status_code, r.text

def api_update_person(token, person_id, payload):
    r = requests.put(
        f'https://api.pipedrive.com/v1/persons/{person_id}',
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

def fetch_oportunidados(token, cnpj):
    if not token or not cnpj:
        return 0, {}
    r = requests.get(
        f'https://app.oportunidados.com.br/api/v1/brazilian_companies/{cnpj}/company',
        headers={'Authorization': f'Bearer {token}'},
        timeout=60
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

def build_sheet_index(rows):
    by_company = {}
    by_title = {}
    for r in rows:
        company = r.get('Organização - Nome') or ''
        title = r.get('Negócio - Título') or ''
        item = {
            'company': company,
            'title': title,
            'person_name': str(r.get('Pessoa - Nome') or '').strip(),
            'cnpj': norm_cnpj(r.get('cnpj_formatado')),
            'phones': dedupe([
                norm_phone(r.get('Pessoa - Telefone - Trabalho')),
                norm_phone(r.get('Pessoa - Telefone - Celular')),
                norm_phone(r.get('API_TEL_OFICIAL')),
            ]),
            'emails': dedupe([
                norm_email(r.get('API_EMAIL')),
            ]),
        }
        ck = norm_key(company)
        tk = norm_key(title)
        if ck and ck not in by_company:
            by_company[ck] = item
        if tk and tk not in by_title:
            by_title[tk] = item
    return by_company, by_title

pipedrive_token, oportun_token = load_tokens()
state = load_state()
done = set(state.get('done', []))

sheet_rows = parse_xlsx(XLSX)
sheet_by_company, sheet_by_title = build_sheet_index(sheet_rows)

all_deals = api_get_all('deals', pipedrive_token, {'status': 'all_not_deleted'})
open_deals = [d for d in all_deals if int(d.get('pipeline_id') or 0) == PIPELINE_ID and int(d.get('stage_id') or 0) != LOST_STAGE_ID]

results = []
counts = Counter()
processed = 0

# PHASE 1: fix missing persons safely
missing_person = [d for d in open_deals if not extract_id(d.get('person_id'))]
log(f'[PHASE1] missing_person={len(missing_person)}')

for d in missing_person:
    deal_id = int(d.get('id') or 0)
    key = f'missing_person|{deal_id}'
    if key in done:
        continue

    title = d.get('title') or ''
    org_id = extract_id(d.get('org_id'))
    org_name = d.get('org_name') or ''
    row = {'phase': 'missing_person', 'deal_id': deal_id, 'title': title, 'status': ''}

    # 1) single participant
    parts = api_get_deal_participants(pipedrive_token, deal_id)
    candidate_ids = []
    for p in parts:
        pid = extract_id(p.get('person_id') or p.get('person'))
        if pid:
            candidate_ids.append(pid)
    candidate_ids = sorted(set(candidate_ids))

    fixed = False
    if len(candidate_ids) == 1:
        sc, body = api_update_deal(pipedrive_token, deal_id, {'person_id': candidate_ids[0]})
        row['status'] = 'fixed' if 200 <= sc < 300 else 'error'
        row['person_id'] = candidate_ids[0]
        fixed = 200 <= sc < 300
        log(f"[MISSING_PERSON_{row['status'].upper()}] deal={deal_id} person={candidate_ids[0]}")

    if not fixed:
        # 2) sheet / oportunidades
        src = sheet_by_company.get(norm_key(org_name)) or sheet_by_title.get(norm_key(title)) or {}
        emails = src.get('emails', [])
        phones = src.get('phones', [])
        person_name = src.get('person_name') or org_name or title
        cnpj = src.get('cnpj') or ''

        if cnpj and oportun_token:
            sc_opp, payload = fetch_oportunidados(oportun_token, cnpj)
            if sc_opp and sc_opp < 400 and payload:
                ext = collect_oportunidados(payload)
                emails = dedupe(emails + ext.get('emails', []))
                phones = dedupe(phones + ext.get('phones', []))
                if ext.get('person_name'):
                    person_name = ext.get('person_name')

        if emails or phones:
            payload = {'name': person_name or org_name or f'Contato {deal_id}', 'org_id': org_id}
            if emails:
                payload['email'] = [{'value': e, 'primary': i == 0} for i, e in enumerate(emails)]
            if phones:
                payload['phone'] = [{'value': p, 'primary': i == 0} for i, p in enumerate(phones)]
            sc1, body1, created = api_create_person(pipedrive_token, payload)
            if 200 <= sc1 < 300 and created.get('id'):
                new_person_id = int(created.get('id') or 0)
                sc2, body2 = api_update_deal(pipedrive_token, deal_id, {'person_id': new_person_id})
                row['status'] = 'fixed' if 200 <= sc2 < 300 else 'error'
                row['person_id'] = new_person_id
                log(f"[MISSING_PERSON_{row['status'].upper()}] deal={deal_id} person={new_person_id}")
            else:
                row['status'] = 'error'
                row['reason'] = 'create_person_failed'
        else:
            row['status'] = 'needs_review'
            row['reason'] = 'no_contact_data'

    results.append(row)
    done.add(key)
    save_state({'done': sorted(done)})
    counts[f"{row['phase']}:{row['status']}"] += 1
    processed += 1
    if processed >= BATCH_LIMIT:
        log(f'[PAUSA] {BATCH_DELAY_SEC}s')
        time.sleep(BATCH_DELAY_SEC)
        processed = 0

# refresh
all_deals = api_get_all('deals', pipedrive_token, {'status': 'all_not_deleted'})
open_deals = [d for d in all_deals if int(d.get('pipeline_id') or 0) == PIPELINE_ID and int(d.get('stage_id') or 0) != LOST_STAGE_ID]

# PHASE 2: fix simple duplicates only
by_title = defaultdict(list)
for d in open_deals:
    title_base = norm(d.get('title'))
    if title_base:
        by_title[title_base].append(d)

dup_groups = [(k, v) for k, v in by_title.items() if len(v) > 1 and k not in SKIP_DUP_TITLES]
log(f'[PHASE2] duplicate_groups={len(dup_groups)}')

for title_base, deals in dup_groups:
    deals = sorted(deals, key=lambda x: int(x.get('id') or 10**12))
    # only safe if exactly 2 deals and same org or one clearly newer duplicate
    if len(deals) != 2:
        continue

    d1, d2 = deals
    org1 = extract_id(d1.get('org_id'))
    org2 = extract_id(d2.get('org_id'))
    if org1 != org2 and org1 != 0 and org2 != 0:
        continue

    dup = d2
    dup_id = int(dup.get('id') or 0)
    key = f'dup_to_lost|{dup_id}'
    if key in done:
        continue

    row = {'phase': 'dup_to_lost', 'deal_id': dup_id, 'title': dup.get('title'), 'master_deal_id': int(d1.get('id') or 0)}
    sc, body = api_update_deal(pipedrive_token, dup_id, {'stage_id': LOST_STAGE_ID})
    row['status'] = 'fixed' if 200 <= sc < 300 else 'error'
    log(f"[DUP_TO_LOST_{row['status'].upper()}] dup={dup_id} master={row['master_deal_id']}")
    results.append(row)
    done.add(key)
    save_state({'done': sorted(done)})
    counts[f"{row['phase']}:{row['status']}"] += 1
    processed += 1
    if processed >= BATCH_LIMIT:
        log(f'[PAUSA] {BATCH_DELAY_SEC}s')
        time.sleep(BATCH_DELAY_SEC)
        processed = 0

# final audit
all_deals = api_get_all('deals', pipedrive_token, {'status': 'all_not_deleted'})
open_deals = [d for d in all_deals if int(d.get('pipeline_id') or 0) == PIPELINE_ID and int(d.get('stage_id') or 0) != LOST_STAGE_ID]

without_org = []
without_person = []
by_title_final = defaultdict(list)
by_org_final = defaultdict(list)

for d in open_deals:
    did = int(d.get('id') or 0)
    title = d.get('title')
    tbase = norm(title)
    org_id = extract_id(d.get('org_id'))
    person_id = extract_id(d.get('person_id'))

    by_title_final[tbase].append(did)
    if org_id:
        by_org_final[org_id].append(did)
    else:
        without_org.append({'deal_id': did, 'title': title})
    if not person_id:
        without_person.append({'deal_id': did, 'title': title})

summary = {
    'apply_counts': dict(counts),
    'final_open': {
        'total': len(open_deals),
        'without_org': len(without_org),
        'without_person': len(without_person),
        'duplicate_title_bases': len({k:v for k,v in by_title_final.items() if k and len(v) > 1}),
        'orgs_with_multiple_open_deals': len({k:v for k,v in by_org_final.items() if len(v) > 1}),
    },
    'samples': {
        'without_org': without_org[:20],
        'without_person': without_person[:20],
        'duplicate_titles_top20': sorted(
            [(k, len(v), v[:10]) for k,v in by_title_final.items() if k and len(v) > 1],
            key=lambda x: -x[1]
        )[:20],
        'multi_orgs_top20': sorted(
            [(k, len(v), v[:10]) for k,v in by_org_final.items() if len(v) > 1],
            key=lambda x: -x[1]
        )[:20],
    }
}

RESULT_JSON.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding='utf-8')
SUMMARY_JSON.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')

log('[FINAL_COUNTS]')
for k, v in sorted(counts.items()):
    log(f'{k}={v}')
log('[FINAL_SUMMARY]')
log(json.dumps(summary['final_open'], ensure_ascii=False, indent=2))
log(f'RESULT_JSON={RESULT_JSON}')
log(f'SUMMARY_JSON={SUMMARY_JSON}')
