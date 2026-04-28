import json
import re
import unicodedata
import requests
import time
from pathlib import Path
from collections import Counter

CFG = Path('/root/sdr-vps/config/system_config.json')
INPUT_JSON = Path('/root/sdr-vps/runtime/reconcile_export_vs_full_pipeline2_api_v2_nomatch.json')
STATE_JSON = Path('/root/sdr-vps/runtime/enrich_342_existing_only_v2_state.json')
RESULT_JSON = Path('/root/sdr-vps/runtime/enrich_342_existing_only_v2_result.json')

PIPELINE_ID = 2
BATCH_LIMIT = 10
BATCH_DELAY_SEC = 5
OPORTUNIDADOS_BASE = "https://app.oportunidados.com.br"

def load_tokens():
    cfg = json.load(open(CFG))
    pipedrive = cfg.get('pipedrive_token') or cfg.get('pipedrive_api_token') or ''
    oportun = ''
    txt = Path('/root/sdr-vps/super_minas_public_links_to_crm.py').read_text(encoding='utf-8', errors='ignore')
    m = re.search(r'OPORTUNIDADOS_TOKEN\s*=\s*"([^"]+)"', txt)
    if m:
        oportun = m.group(1).strip()
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

def api_get_all_open_deals(token):
    all_deals = []
    start = 0
    limit = 500
    while True:
        r = requests.get(
            'https://api.pipedrive.com/v1/deals',
            params={'api_token': token, 'start': start, 'limit': limit, 'status': 'all_not_deleted'},
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
    return r.json().get('data') or {}

def api_search_orgs(token, term):
    r = requests.get(
        'https://api.pipedrive.com/v1/organizations/search',
        params={
            'api_token': token,
            'term': term,
            'fields': 'name',
            'limit': 20,
            'exact_match': False,
        },
        timeout=60,
    )
    if r.status_code >= 400:
        return []
    return (r.json().get('data') or {}).get('items') or []

def api_search_persons(token, term):
    r = requests.get(
        'https://api.pipedrive.com/v1/persons/search',
        params={'api_token': token, 'term': term, 'fields': 'name', 'limit': 10},
        timeout=60,
    )
    if r.status_code >= 400:
        return []
    return (r.json().get('data') or {}).get('items') or []

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

def api_update_org(token, org_id, payload):
    r = requests.put(
        f'https://api.pipedrive.com/v1/organizations/{org_id}',
        params={'api_token': token},
        json=payload,
        timeout=60,
    )
    return r.status_code, r.text

def fetch_oportunidados(token, cnpj):
    r = requests.get(
        f'{OPORTUNIDADOS_BASE}/api/v1/brazilian_companies/{cnpj}/company',
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

    company_name = root.get('company_name') or root.get('razao_social') or root.get('name') or ''
    trade_name = root.get('fantasy_name') or root.get('nome_fantasia') or ''
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
        'company_name': company_name,
        'trade_name': trade_name,
        'person_name': person_name,
        'emails': dedupe(emails),
        'phones': dedupe(phones),
    }

def org_candidates_from_row(token, company, title, ext_names):
    terms = []
    for t in [company, title, *ext_names]:
        t = norm(t)
        if t and t not in terms:
            terms.append(t)

    candidates = {}
    for term in terms:
        items = api_search_orgs(token, term)
        for item in items:
            org = item.get('item') or {}
            oid = int(org.get('id') or 0)
            name = org.get('name') or ''
            if oid and oid not in candidates:
                candidates[oid] = {'id': oid, 'name': name}
    return list(candidates.values())

def choose_org(candidates, company, title, ext_names):
    wanted = [norm_key(company), norm_key(title)] + [norm_key(x) for x in ext_names if x]
    wanted = [x for x in wanted if x]

    exact = []
    for c in candidates:
        ck = norm_key(c.get('name'))
        if ck in wanted:
            exact.append(c)

    if len(exact) == 1:
        return exact[0], 'exact_name'
    if len(candidates) == 1:
        return candidates[0], 'single_candidate'
    return None, 'ambiguous'

def deals_for_org(open_p2, org_id):
    out = []
    for d in open_p2:
        if extract_id(d.get('org_id')) == org_id:
            out.append(d)
    return sorted(out, key=lambda x: int(x.get('id') or 10**12))

pipedrive_token, oportun_token = load_tokens()
rows = json.load(open(INPUT_JSON))
state = load_state()
done = set(state.get('done', []))
open_deals = api_get_all_open_deals(pipedrive_token)
open_p2 = [d for d in open_deals if int(d.get('pipeline_id') or 0) == PIPELINE_ID]

results = []
counts = Counter()
processed = 0

print(f'[START] rows={len(rows)}', flush=True)

for row in rows:
    title = row.get('title') or ''
    company = row.get('company') or ''
    cnpj = norm_cnpj(row.get('cnpj'))
    key = f"{company}|{title}|{cnpj}"

    if key in done:
        continue

    result = {'company': company, 'title': title, 'cnpj': cnpj, 'status': ''}

    ext = {'company_name': '', 'trade_name': '', 'person_name': '', 'emails': [], 'phones': []}
    if cnpj and oportun_token:
        sc, payload = fetch_oportunidados(oportun_token, cnpj)
        result['oportunidados_status'] = sc
        if sc < 400 and payload:
            ext = collect_oportunidados(payload)

    ext_names = [ext.get('company_name'), ext.get('trade_name')]
    candidates = org_candidates_from_row(pipedrive_token, company, title, ext_names)
    chosen_org, org_reason = choose_org(candidates, company, title, ext_names)

    if not chosen_org:
        result['status'] = 'needs_review'
        result['reason'] = 'no_unique_org_match'
        result['candidate_org_ids'] = [c['id'] for c in candidates[:10]]
        results.append(result)
        done.add(key)
        save_state({'done': sorted(done)})
        counts[result['status']] += 1
        continue

    org_id = chosen_org['id']
    org_deals = deals_for_org(open_p2, org_id)

    if len(org_deals) != 1:
        result['status'] = 'needs_review'
        result['reason'] = 'org_without_unique_open_deal'
        result['org_id'] = org_id
        result['candidate_deal_ids'] = [int(d.get('id') or 0) for d in org_deals[:10]]
        results.append(result)
        done.add(key)
        save_state({'done': sorted(done)})
        counts[result['status']] += 1
        continue

    deal = org_deals[0]
    deal_id = int(deal.get('id') or 0)
    person_id = extract_id(deal.get('person_id'))

    emails = dedupe([norm_email(row.get('api_email')), *ext.get('emails', [])])
    phones = dedupe([norm_phone(row.get('api_tel_oficial')), *ext.get('phones', [])])
    person_name = ext.get('person_name') or company

    if ext.get('trade_name') or ext.get('company_name'):
        org_payload = {'name': ext.get('trade_name') or ext.get('company_name')}
        sc, body = api_update_org(pipedrive_token, org_id, org_payload)
        if not (200 <= sc < 300):
            result['org_update_error'] = sc

    if person_id:
        payload_person = {}
        if emails:
            payload_person['email'] = [{'value': e, 'primary': i == 0} for i, e in enumerate(emails)]
        if phones:
            payload_person['phone'] = [{'value': p, 'primary': i == 0} for i, p in enumerate(phones)]
        if payload_person:
            sc, body = api_update_person(pipedrive_token, person_id, payload_person)
            if 200 <= sc < 300:
                result['status'] = 'updated_existing_person'
                result['deal_id'] = deal_id
                result['org_id'] = org_id
                result['person_id'] = person_id
                print(f'[UPDATED_EXISTING] deal={deal_id} org={org_id} company={company}', flush=True)
            else:
                result['status'] = 'error'
                result['reason'] = 'update_existing_person_failed'
                result['http_status'] = sc
        else:
            result['status'] = 'needs_review'
            result['reason'] = 'no_contact_data'
    else:
        if not emails and not phones:
            result['status'] = 'needs_review'
            result['reason'] = 'no_contact_data'
        else:
            existing_person_id = 0
            if person_name:
                found = api_search_persons(pipedrive_token, person_name)
                exact = []
                for item in found:
                    p = item.get('item') or {}
                    if norm_key(p.get('name')) == norm_key(person_name):
                        exact.append(p)
                if len(exact) == 1:
                    existing_person_id = int(exact[0].get('id') or 0)

            if existing_person_id:
                sc, body = api_update_deal_person(pipedrive_token, deal_id, existing_person_id)
                if 200 <= sc < 300:
                    result['status'] = 'linked_existing_person'
                    result['deal_id'] = deal_id
                    result['org_id'] = org_id
                    result['person_id'] = existing_person_id
                    print(f'[LINKED_EXISTING] deal={deal_id} org={org_id} company={company}', flush=True)
                else:
                    result['status'] = 'error'
                    result['reason'] = 'link_existing_person_failed'
                    result['http_status'] = sc
            else:
                payload = {'name': person_name or company, 'org_id': org_id}
                if emails:
                    payload['email'] = [{'value': e, 'primary': i == 0} for i, e in enumerate(emails)]
                if phones:
                    payload['phone'] = [{'value': p, 'primary': i == 0} for i, p in enumerate(phones)]
                sc1, body1, created = api_create_person(pipedrive_token, payload)
                if 200 <= sc1 < 300 and created.get('id'):
                    new_person_id = int(created.get('id') or 0)
                    sc2, body2 = api_update_deal_person(pipedrive_token, deal_id, new_person_id)
                    if 200 <= sc2 < 300:
                        result['status'] = 'created_and_linked_person'
                        result['deal_id'] = deal_id
                        result['org_id'] = org_id
                        result['person_id'] = new_person_id
                        print(f'[CREATED_AND_LINKED] deal={deal_id} org={org_id} company={company}', flush=True)
                    else:
                        result['status'] = 'error'
                        result['reason'] = 'link_new_person_failed'
                        result['http_status'] = sc2
                else:
                    result['status'] = 'error'
                    result['reason'] = 'create_person_failed'
                    result['http_status'] = sc1

    result['emails'] = emails
    result['phones'] = phones
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
