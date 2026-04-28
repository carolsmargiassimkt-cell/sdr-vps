import json, requests, re, unicodedata
from collections import defaultdict
from pathlib import Path

CFG = Path('/root/sdr-vps/config/system_config.json')
RESULT_JSON = Path('/root/sdr-vps/runtime/fix_last_4_duplicate_titles_result.json')

PIPELINE_ID = 2
SAFE_DUPES = {
    'DROGAMARYS',
    'FARMACIA PRECO BAIXO',
    'FARMACIAS PROSAUDE',
    'DROGARIA MEGA DESCONTAO',
}

cfg = json.load(open(CFG))
TOKEN = cfg.get('pipedrive_token') or cfg.get('pipedrive_api_token') or ''

def norm(v):
    s = str(v or '').strip()
    s = unicodedata.normalize('NFKD', s)
    s = ''.join(ch for ch in s if not unicodedata.combining(ch))
    s = s.upper()
    s = re.sub(r'\s*-\s*DEAL\s*$', '', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s

def get_all_open_p2():
    out = []
    start = 0
    while True:
        r = requests.get(
            'https://api.pipedrive.com/v1/deals',
            params={'api_token': TOKEN, 'status': 'open', 'start': start, 'limit': 500},
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

def lose_real(deal_id):
    r = requests.put(
        f'https://api.pipedrive.com/v1/deals/{deal_id}',
        params={'api_token': TOKEN},
        json={'status': 'lost'},
        timeout=60,
    )
    return r.status_code, r.text

deals = get_all_open_p2()
groups = defaultdict(list)
for d in deals:
    groups[norm(d.get('title'))].append(d)

results = []
for title, items in groups.items():
    if title not in SAFE_DUPES or len(items) <= 1:
        continue

    items = sorted(items, key=lambda x: int(x.get('id') or 10**12))
    master = int(items[0].get('id') or 0)

    for dup in items[1:]:
        dup_id = int(dup.get('id') or 0)
        sc, body = lose_real(dup_id)
        row = {
            'title_base': title,
            'master_deal_id': master,
            'dup_deal_id': dup_id,
            'status_code': sc,
            'ok': 200 <= sc < 300,
        }
        results.append(row)
        print(title, 'MASTER', master, 'LOSE', dup_id, sc, flush=True)

RESULT_JSON.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding='utf-8')
print('RESULT_JSON =', RESULT_JSON)
