import json, re, unicodedata, requests
from collections import defaultdict

TOKEN = json.load(open('/root/sdr-vps/config/system_config.json'))['pipedrive_token']

INPUT = '/root/sdr-vps/runtime/remaining_200_for_reconcile.json'
OUTPUT = '/root/sdr-vps/runtime/final_reconcile_remaining_200.json'

def norm(s):
    s = str(s or '')
    s = unicodedata.normalize('NFKD', s)
    s = ''.join(c for c in s if not unicodedata.combining(c))
    s = s.upper()
    s = re.sub(r'[^A-Z0-9 ]+', '', s)
    return re.sub(r'\s+', ' ', s).strip()

def fetch_all_deals():
    deals = []
    start = 0
    while True:
        r = requests.get(
            'https://api.pipedrive.com/v1/deals',
            params={
                'api_token': TOKEN,
                'status': 'all_not_deleted',
                'start': start,
                'limit': 500
            }
        )
        data = r.json()
        batch = data.get('data') or []
        deals += batch
        if not data.get('additional_data', {}).get('pagination', {}).get('more_items_in_collection'):
            break
        start += 500
    return deals

print('[LOAD DEALS]')
deals = fetch_all_deals()

org_index = defaultdict(list)

for d in deals:
    org = d.get('org_id')
    if isinstance(org, dict):
        name = org.get('name')
    else:
        name = ''
    if name:
        org_index[norm(name)].append(d)

remaining = json.load(open(INPUT))

results = []

for r in remaining:
    company = r.get('company')
    c_norm = norm(company)

    best = None

    # 1. match exato
    if c_norm in org_index:
        best = org_index[c_norm][0]

    # 2. contains
    if not best:
        for k in org_index:
            if c_norm in k or k in c_norm:
                best = org_index[k][0]
                break

    if best:
        results.append({
            'company': company,
            'deal_id': best['id']
        })

print('[DONE]', len(results))
json.dump(results, open(OUTPUT, 'w'), indent=2)
