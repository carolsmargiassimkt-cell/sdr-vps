import json, requests, re, unicodedata
from collections import defaultdict

cfg = json.load(open('/root/sdr-vps/config/system_config.json'))
TOKEN = cfg.get('pipedrive_token') or cfg.get('pipedrive_api_token') or ''

SAFE_DUPES = {
    'PGL DISTRIBUICAO DE ALIMENTOS',
    'FORTUNA COMERCIO DE PECAS E SERVICOS LTDA',
    'FUNDO',
    'AMBEV',
    'ZD ALIMENTOS',
    'TOZZI ALIMENTOS',
    'ZINHO ALIMENTOS',
    'PIF PAF ALIMENTOS'
}

def norm(v):
    s = unicodedata.normalize('NFKD', str(v or ''))
    s = ''.join(ch for ch in s if not unicodedata.combining(ch))
    s = s.upper()
    s = re.sub(r'\s*-\s*DEAL$', '', s)
    return s.strip()

def get_all():
    out, start = [], 0
    while True:
        r = requests.get(
            'https://api.pipedrive.com/v1/deals',
            params={'api_token': TOKEN, 'status': 'open', 'start': start, 'limit': 500},
        )
        data = r.json()
        batch = data.get('data') or []
        out += batch
        if not data.get('additional_data', {}).get('pagination', {}).get('more_items_in_collection'):
            break
        start += 500
    return out

def lose(deal_id):
    return requests.put(
        f'https://api.pipedrive.com/v1/deals/{deal_id}',
        params={'api_token': TOKEN},
        json={'status': 'lost'}
    ).status_code

deals = [d for d in get_all() if int(d.get('pipeline_id') or 0)==2]

groups = defaultdict(list)
for d in deals:
    groups[norm(d.get('title'))].append(d)

for title, items in groups.items():
    if title not in SAFE_DUPES or len(items)<=1:
        continue

    items = sorted(items, key=lambda x: int(x.get('id')))
    master = items[0]['id']

    for dup in items[1:]:
        sc = lose(dup['id'])
        print(title, 'LOSE', dup['id'], sc)
