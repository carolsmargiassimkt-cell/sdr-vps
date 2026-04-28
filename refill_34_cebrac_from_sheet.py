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
RESULT_JSON = Path('/root/sdr-vps/runtime/refill_34_cebrac_from_sheet_result.json')

PIPELINE_ID = 2
TARGET_DEALS = {
    2959,2960,2961,2962,2963,2964,2965,2966,2967,2968,2969,2970,2971,2972,
    2973,2974,2975,2976,2977,2978,2979,2980,2981,2983,2984,3000,3001,3002,
    3003,3006,3010,3011,3012,3014
}
NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"

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

def api_get_all_open_p2(token):
    out = []
    start = 0
    while True:
        r = requests.get(
            'https://api.pipedrive.com/v1/deals',
            params={'api_token': token, 'status': 'open', 'start': start, 'limit': 500},
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
    return [d for d in out if int(d.get('pipeline_id') or 0) == PIPELINE_ID]

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
    return r.status_code, data

def api_update_deal_person(token, deal_id, person_id):
    r = requests.put(
        f'https://api.pipedrive.com/v1/deals/{deal_id}',
        params={'api_token': token},
        json={'person_id': person_id},
        timeout=60,
    )
    return r.status_code

token = load_token()

sheet = parse_xlsx(XLSX)
sheet_by_company = {}
sheet_by_title = {}

for r in sheet:
    item = {
        'company': r.get('Organização - Nome') or '',
        'title': r.get('Negócio - Título') or '',
        'person_name': str(r.get('Pessoa - Nome') or '').strip(),
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

deals = api_get_all_open_p2(token)
target_deals = [d for d in deals if int(d.get('id') or 0) in TARGET_DEALS]

results = []
counts = Counter()

print(f'[START] target_deals={len(target_deals)}', flush=True)

for d in target_deals:
    deal_id = int(d.get('id') or 0)
    title = d.get('title') or ''
    org_name = d.get('org_name') or ''
    org_id = extract_id(d.get('org_id'))

    src = sheet_by_company.get(norm_key(org_name)) or sheet_by_title.get(norm_key(title)) or {}

    person_name = src.get('person_name') or org_name or title
    emails = src.get('emails', [])
    phones = src.get('phones', [])

    row = {
        'deal_id': deal_id,
        'title': title,
        'org_name': org_name,
        'status': 'needs_review',
    }

    if emails or phones:
        payload = {
            'name': person_name,
            'org_id': org_id,
        }
        if emails:
            payload['email'] = [{'value': e, 'primary': i == 0} for i, e in enumerate(emails)]
        if phones:
            payload['phone'] = [{'value': p, 'primary': i == 0} for i, p in enumerate(phones)]

        sc1, created = api_create_person(token, payload)
        if 200 <= sc1 < 300 and created.get('id'):
            person_id = int(created.get('id') or 0)
            sc2 = api_update_deal_person(token, deal_id, person_id)
            if 200 <= sc2 < 300:
                row['status'] = 'fixed'
                row['person_id'] = person_id
            else:
                row['status'] = 'error'
                row['reason'] = 'link_failed'
        else:
            row['status'] = 'error'
            row['reason'] = 'create_person_failed'
    else:
        row['reason'] = 'no_contact_data'

    results.append(row)
    counts[row['status']] += 1
    print(row['deal_id'], row['title'], row['status'], row.get('person_id'), row.get('reason'), flush=True)

RESULT_JSON.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding='utf-8')
print('[FINAL_COUNTS]', flush=True)
for k, v in sorted(counts.items()):
    print(f'{k}={v}', flush=True)
print(f'[RESULT_JSON] {RESULT_JSON}', flush=True)
