import json
import requests
import re
import unicodedata
from collections import Counter, defaultdict

cfg = json.load(open('/root/sdr-vps/config/system_config.json'))
API_TOKEN = cfg.get('pipedrive_token') or cfg.get('pipedrive_api_token') or ''

def norm(v):
    s = str(v or '').strip()
    s = unicodedata.normalize('NFKD', s)
    s = ''.join(ch for ch in s if not unicodedata.combining(ch))
    s = s.upper()
    s = re.sub(r'\s*-\s*LEAD\s*$', '', s)
    s = re.sub(r'\s*-\s*DEAL\s*$', '', s)
    s = re.sub(r'\s+', ' ', s).strip(' -')
    return s

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

all_deals = []
start = 0
limit = 500

while True:
    r = requests.get(
        'https://api.pipedrive.com/v1/deals',
        params={
            'api_token': API_TOKEN,
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

p2 = [d for d in all_deals if int(d.get('pipeline_id') or 0) == 2]

with_org = 0
with_person = 0
bad_org = []
bad_person = []
titles = Counter()
org_deal_count = Counter()

for d in p2:
    deal_id = int(d.get('id') or 0)
    title = norm(d.get('title'))
    titles[title] += 1

    org_id = extract_id(d.get('org_id'))
    person_id = extract_id(d.get('person_id'))

    if org_id:
        with_org += 1
        org_deal_count[org_id] += 1
    else:
        bad_org.append({'deal_id': deal_id, 'title': d.get('title')})

    if person_id:
        with_person += 1
    else:
        bad_person.append({'deal_id': deal_id, 'title': d.get('title')})

dup_titles = [(k, v) for k, v in titles.items() if k and v > 1]
dup_orgs = [(k, v) for k, v in org_deal_count.items() if v > 1]

print("TOTAL_DEALS_PIPELINE_2_API =", len(p2))
print("DEALS_COM_ORG =", with_org)
print("DEALS_COM_PESSOA_PRINCIPAL =", with_person)
print("DEALS_SEM_ORG =", len(bad_org))
print("DEALS_SEM_PESSOA_PRINCIPAL =", len(bad_person))
print("TITULOS_DUPLICADOS =", len(dup_titles))
print("ORGS_COM_MULTIPLOS_DEALS =", len(dup_orgs))

print("\\nEXEMPLOS_DEALS_SEM_ORG:")
for x in bad_org[:20]:
    print(x)

print("\\nEXEMPLOS_DEALS_SEM_PESSOA:")
for x in bad_person[:20]:
    print(x)

print("\\nTOP_TITULOS_DUPLICADOS:")
for x in sorted(dup_titles, key=lambda t: -t[1])[:20]:
    print(x)

print("\\nTOP_ORGS_COM_MULTIPLOS_DEALS:")
for x in sorted(dup_orgs, key=lambda t: -t[1])[:20]:
    print(x)
