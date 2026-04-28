import json
import re
import unicodedata
import zipfile
import xml.etree.ElementTree as ET
from collections import defaultdict, Counter
from pathlib import Path
import requests

CFG = Path('/root/sdr-vps/config/system_config.json')
INPUT_XLSX = Path('/root/BASE_LEADS_COM_LINKEDIN.xlsx')
STATE_JSON = Path('/root/sdr-vps/runtime/enrich_missing_persons_from_sheet_state.json')
RESULT_JSON = Path('/root/sdr-vps/runtime/enrich_missing_persons_from_sheet_result.json')

PIPELINE_ID = 2
NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"

def load_token():
    cfg = json.load(open(CFG))
    return cfg.get('pipedrive_token') or cfg.get('pipedrive_api_token') or ''

def strip_accents(v):
    s = str(v or '').strip()
    s = unicodedata.normalize('NFKD', s)
    return ''.join(ch for ch in s if not unicodedata.combining(ch))

def norm_company(v):
    s = strip_accents(v).upper()
    s = re.sub(r'\s*-\s*LEAD\s*$', '', s, flags=re.I)
    s = re.sub(r'\s*-\s*DEAL\s*$', '', s, flags=re.I)
    s = re.sub(r'\s*LEADSTER\s*$', '', s, flags=re.I)
    s = re.sub(r'\s+', ' ', s).strip(' -')
    return s

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

def api_get_all_open_deals(token):
    all_deals = []
    start = 0
    limit = 500
    while True:
        r = requests.get(
            'https://api.pipedrive.com/v1/deals',
            params={
                'api_token': token,
                'start': start,
                'limit': limit,
                'status': 'open',
            },
            timeout=60,
        )
        r.raise_for_status()
        payload = r.json()
        batch = payload.get('data') or []
        all_deals.extend(batch)
        pag = payload.get('additional_data', {}).get('pagination', {})
        if not pag.get('more_items_in_collection', False) or not batch:
            break
        start += limit
    return all_deals

def api_get_org(token, org_id):
    r = requests.get(
        f'https://api.pipedrive.com/v1/organizations/{org_id}',
        params={'api_token': token},
        timeout=60,
    )
    if r.status_code >= 400:
        return {}
    return (r.json().get('data') or {})

def api_search_persons(token, term):
    r = requests.get(
        'https://api.pipedrive.com/v1/persons/search',
        params={
            'api_token': token,
            'term': term,
            'fields': 'name',
            'limit': 10,
        },
        timeout=60,
    )
    if r.status_code >= 400:
        return []
    data = r.json().get('data') or {}
    return data.get('items') or []

def api_create_person(token, payload):
    r = requests.post(
        'https://api.pipedrive.com/v1/persons',
        params={'api_token': token},
        json=payload,
        timeout=60,
    )
    return r.status_code, r.text, (r.json().get('data') or {} if 'application/json' in r.headers.get('content-type','') else {})

def api_update_deal_person(token, deal_id, person_id):
    r = requests.put(
        f'https://api.pipedrive.com/v1/deals/{deal_id}',
        params={'api_token': token},
        json={'person_id': person_id},
        timeout=60,
    )
    return r.status_code, r.text

def build_sheet_index(rows):
    by_company = defaultdict(list)
    by_cnpj = defaultdict(list)
    for row in rows:
        company = norm_company(row.get('Organização - Nome'))
        cnpj = norm_cnpj(row.get('cnpj_formatado'))
        row['_company_norm'] = company
        row['_cnpj_norm'] = cnpj
        row['_person_name'] = str(row.get('Pessoa - Nome') or '').strip()
        row['_emails'] = dedupe([
            norm_email(row.get('API_EMAIL')),
        ])
        row['_phones'] = dedupe([
            norm_phone(row.get('Pessoa - Telefone - Trabalho')),
            norm_phone(row.get('Pessoa - Telefone - Celular')),
            norm_phone(row.get('API_TEL_OFICIAL')),
        ])
        by_company[company].append(row)
        if cnpj:
            by_cnpj[cnpj].append(row)
    return by_company, by_cnpj

def choose_best_sheet_row(rows):
    scored = []
    for r in rows:
        score = (
            int(bool(r.get('_person_name'))),
            len(r.get('_emails') or []),
            len(r.get('_phones') or []),
        )
        scored.append((score, r))
    scored.sort(reverse=True, key=lambda x: x[0])
    return scored[0][1] if scored else None

token = load_token()
state = load_state()
done = set(state.get('done', []))

sheet_rows = parse_xlsx(INPUT_XLSX)
by_company, by_cnpj = build_sheet_index(sheet_rows)

all_deals = api_get_all_open_deals(token)
p2 = [d for d in all_deals if int(d.get('pipeline_id') or 0) == PIPELINE_ID]
missing = [d for d in p2 if not extract_id(d.get('person_id'))]

results = []
counts = Counter()

print(f'[START] missing_person_open_p2={len(missing)}', flush=True)

for d in missing:
    deal_id = int(d.get('id') or 0)
    title = str(d.get('title') or '')
    org_id = extract_id(d.get('org_id'))
    key = str(deal_id)

    if key in done:
        continue

    row_result = {
        'deal_id': deal_id,
        'title': title,
        'org_id': org_id,
        'status': '',
    }

    if not org_id:
        row_result['status'] = 'needs_review'
        row_result['reason'] = 'deal_without_org'
        print(f'[REVIEW] deal={deal_id} deal_without_org', flush=True)
        results.append(row_result)
        done.add(key)
        save_state({'done': sorted(done)})
        counts[row_result['status']] += 1
        continue

    org = api_get_org(token, org_id)
    org_name = norm_company(org.get('name'))
    org_cnpj = ''
    for _, value in org.items():
        c = norm_cnpj(value)
        if c:
            org_cnpj = c
            break

    candidate_rows = []
    if org_cnpj and org_cnpj in by_cnpj:
        candidate_rows = by_cnpj[org_cnpj]
        row_result['match_reason'] = 'cnpj'
    elif org_name and org_name in by_company:
        candidate_rows = by_company[org_name]
        row_result['match_reason'] = 'company'
    else:
        title_base = norm_company(title)
        if title_base in by_company:
            candidate_rows = by_company[title_base]
            row_result['match_reason'] = 'title_base'

    if not candidate_rows:
        row_result['status'] = 'needs_review'
        row_result['reason'] = 'no_sheet_match'
        print(f'[REVIEW] deal={deal_id} no_sheet_match', flush=True)
        results.append(row_result)
        done.add(key)
        save_state({'done': sorted(done)})
        counts[row_result['status']] += 1
        continue

    chosen = choose_best_sheet_row(candidate_rows)
    if not chosen:
        row_result['status'] = 'needs_review'
        row_result['reason'] = 'no_best_row'
        print(f'[REVIEW] deal={deal_id} no_best_row', flush=True)
        results.append(row_result)
        done.add(key)
        save_state({'done': sorted(done)})
        counts[row_result['status']] += 1
        continue

    person_name = chosen.get('_person_name') or ''
    emails = chosen.get('_emails') or []
    phones = chosen.get('_phones') or []

    if not person_name and not emails and not phones:
        row_result['status'] = 'needs_review'
        row_result['reason'] = 'sheet_row_without_contact_data'
        print(f'[REVIEW] deal={deal_id} sheet_row_without_contact_data', flush=True)
        results.append(row_result)
        done.add(key)
        save_state({'done': sorted(done)})
        counts[row_result['status']] += 1
        continue

    # evitar criar pessoa duplicada pelo nome quando houver match claro
    existing_person_id = None
    if person_name:
        found = api_search_persons(token, person_name)
        exact = []
        for item in found:
            p = item.get('item') or {}
            if norm_company(p.get('name')) == norm_company(person_name):
                exact.append(p)
        if len(exact) == 1:
            existing_person_id = int(exact[0].get('id') or 0)

    if existing_person_id:
        status_code, body = api_update_deal_person(token, deal_id, existing_person_id)
        if 200 <= status_code < 300:
            row_result['status'] = 'linked_existing_person'
            row_result['person_id'] = existing_person_id
            print(f'[LINKED_EXISTING] deal={deal_id} person={existing_person_id}', flush=True)
        else:
            row_result['status'] = 'error'
            row_result['http_status'] = status_code
            row_result['body'] = body[:300]
            print(f'[ERROR] deal={deal_id} link_existing status={status_code}', flush=True)
    else:
        payload = {
            'name': person_name or org.get('name') or f'Contato {deal_id}',
            'org_id': org_id,
        }
        if phones:
            payload['phone'] = [{'value': p, 'primary': i == 0} for i, p in enumerate(phones)]
        if emails:
            payload['email'] = [{'value': e, 'primary': i == 0} for i, e in enumerate(emails)]

        status_code, body, created = api_create_person(token, payload)
        if 200 <= status_code < 300 and created.get('id'):
            person_id = int(created.get('id') or 0)
            status_code2, body2 = api_update_deal_person(token, deal_id, person_id)
            if 200 <= status_code2 < 300:
                row_result['status'] = 'created_and_linked_person'
                row_result['person_id'] = person_id
                print(f'[CREATED_AND_LINKED] deal={deal_id} person={person_id}', flush=True)
            else:
                row_result['status'] = 'error'
                row_result['person_id'] = person_id
                row_result['http_status'] = status_code2
                row_result['body'] = body2[:300]
                print(f'[ERROR] deal={deal_id} link_new status={status_code2}', flush=True)
        else:
            row_result['status'] = 'error'
            row_result['http_status'] = status_code
            row_result['body'] = body[:300]
            print(f'[ERROR] deal={deal_id} create_person status={status_code}', flush=True)

    row_result['person_name'] = person_name
    row_result['emails'] = emails
    row_result['phones'] = phones
    results.append(row_result)
    done.add(key)
    save_state({'done': sorted(done)})
    counts[row_result['status']] += 1

RESULT_JSON.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding='utf-8')
print('[FINAL_COUNTS]', flush=True)
for k, v in sorted(counts.items()):
    print(f'{k}={v}', flush=True)
print(f'[RESULT_JSON] {RESULT_JSON}', flush=True)
