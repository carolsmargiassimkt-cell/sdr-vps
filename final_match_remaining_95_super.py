import json, re, unicodedata, requests, difflib
from collections import defaultdict

CFG = json.load(open('/root/sdr-vps/config/system_config.json'))
TOKEN = CFG['pipedrive_token']

INPUT = '/root/sdr-vps/runtime/remaining_200_for_reconcile.json'
MATCHED_ALREADY = '/root/sdr-vps/runtime/final_reconcile_remaining_200_matched.json'
OUTPUT = '/root/sdr-vps/runtime/final_reconcile_remaining_95_super.json'

def norm(s):
    s = str(s or '')
    s = unicodedata.normalize('NFKD', s)
    s = ''.join(c for c in s if not unicodedata.combining(c))
    s = s.upper()
    s = re.sub(r'[^A-Z0-9 ]+', '', s)
    return re.sub(r'\s+', ' ', s).strip()

def clean_cnpj(c):
    return re.sub(r'\D+', '', str(c or ''))

def fetch_all(endpoint):
    out = []
    start = 0
    while True:
        r = requests.get(
            f'https://api.pipedrive.com/v1/{endpoint}',
            params={'api_token': TOKEN, 'start': start, 'limit': 500}
        )
        data = r.json()
        batch = data.get('data') or []
        out += batch
        if not data.get('additional_data', {}).get('pagination', {}).get('more_items_in_collection'):
            break
        start += 500
    return out

print('[LOAD DEALS]')
deals = fetch_all('deals')
print('[LOAD ORGS]')
orgs = fetch_all('organizations')
print('[LOAD PERSONS]')
persons = fetch_all('persons')

# index org por nome
org_by_name = {}
for o in orgs:
    name = o.get('name')
    if name:
        org_by_name[norm(name)] = o

# index pessoa por email/telefone
person_by_email = {}
person_by_phone = {}

for p in persons:
    for e in p.get('email') or []:
        if e.get('value'):
            person_by_email[e['value'].lower()] = p
    for ph in p.get('phone') or []:
        v = re.sub(r'\D+', '', ph.get('value',''))
        if v:
            person_by_phone[v] = p

# index deals por org
deals_by_org = defaultdict(list)
for d in deals:
    org = d.get('org_id')
    if isinstance(org, dict):
        deals_by_org[org.get('value')].append(d)

# carregar dados
all_rows = json.load(open(INPUT))
matched = json.load(open(MATCHED_ALREADY))
matched_companies = {x['company'] for x in matched}

remaining = [r for r in all_rows if r.get('company') not in matched_companies]

results = []

for r in remaining:
    company = r.get('company')
    title = r.get('title')
    cnpj = clean_cnpj(r.get('cnpj'))

    best = None

    # 1. fuzzy org name
    names = list(org_by_name.keys())
    match = difflib.get_close_matches(norm(company), names, n=1, cutoff=0.85)
    if match:
        org = org_by_name[match[0]]
        deals_list = deals_by_org.get(org['id'])
        if deals_list:
            best = deals_list[0]

    if best:
        results.append({
            'company': company,
            'deal_id': best['id'],
            'match_type': 'fuzzy_org'
        })

print('[DONE]', len(results))
json.dump(results, open(OUTPUT,'w'), indent=2)
