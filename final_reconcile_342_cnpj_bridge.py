import json, re, unicodedata, requests, time
from pathlib import Path
from collections import Counter

CFG = Path('/root/sdr-vps/config/system_config.json')
INPUT = Path('/root/sdr-vps/runtime/reconcile_export_vs_full_pipeline2_api_v2_nomatch.json')
OUT = Path('/root/sdr-vps/runtime/final_reconcile_342.json')

def norm(v):
    s = str(v or '')
    s = unicodedata.normalize('NFKD', s)
    s = ''.join(c for c in s if not unicodedata.combining(c))
    return re.sub(r'[^A-Z0-9]+', '', s.upper())

def cnpj(v):
    return re.sub(r'\D+', '', str(v or ''))

cfg = json.load(open(CFG))
token = cfg.get('pipedrive_token')

OP_TOKEN = re.search(
    r'OPORTUNIDADOS_TOKEN\s*=\s*"([^"]+)"',
    Path('/root/sdr-vps/super_minas_public_links_to_crm.py').read_text()
).group(1)

def get_all_deals():
    deals = []
    start = 0
    while True:
        r = requests.get(
            'https://api.pipedrive.com/v1/deals',
            params={'api_token': token, 'start': start, 'limit': 500, 'status': 'all_not_deleted'}
        )
        data = r.json()
        batch = data.get('data') or []
        deals += batch
        if not data.get('additional_data', {}).get('pagination', {}).get('more_items_in_collection'):
            break
        start += 500
    return deals

def get_org(org_id):
    r = requests.get(
        f'https://api.pipedrive.com/v1/organizations/{org_id}',
        params={'api_token': token}
    )
    return r.json().get('data') or {}

def oportunidade(cnpj):
    r = requests.get(
        f'https://app.oportunidados.com.br/api/v1/brazilian_companies/{cnpj}/company',
        headers={'Authorization': f'Bearer {OP_TOKEN}'}
    )
    if r.status_code >= 400:
        return {}
    return r.json()

print('Carregando deals...')
deals = get_all_deals()

index = []
for d in deals:
    org = d.get('org_id')
    if isinstance(org, dict):
        org_id = org.get('value')
    else:
        org_id = org
    if not org_id:
        continue
    org_data = get_org(org_id)
    index.append({
        'deal_id': d['id'],
        'org_id': org_id,
        'name': norm(org_data.get('name'))
    })

rows = json.load(open(INPUT))
res = []
cnt = Counter()

for r in rows:
    c = cnpj(r.get('cnpj'))
    if not c:
        cnt['no_cnpj'] += 1
        continue

    opp = oportunidade(c)
    data = opp.get('data') or {}
    names = [
        data.get('company_name'),
        data.get('fantasy_name'),
        r.get('company'),
        r.get('title')
    ]
    names = [norm(x) for x in names if x]

    matches = []
    for i in index:
        if any(n and n in i['name'] for n in names):
            matches.append(i)

    if len(matches) == 1:
        cnt['matched'] += 1
        res.append({'company': r['company'], 'deal_id': matches[0]['deal_id']})
    else:
        cnt['needs_review'] += 1

    if len(res) % 10 == 0:
        print('processados', len(res))
        time.sleep(2)

json.dump(res, open(OUT, 'w'), indent=2)
print(cnt)
