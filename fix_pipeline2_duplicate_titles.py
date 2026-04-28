import json
import re
import unicodedata
import requests
from collections import defaultdict, Counter
from pathlib import Path

CFG = Path('/root/sdr-vps/config/system_config.json')
STATE_JSON = Path('/root/sdr-vps/runtime/fix_pipeline2_duplicate_titles_state.json')
LOG_JSON = Path('/root/sdr-vps/runtime/fix_pipeline2_duplicate_titles_result.json')

PIPELINE_ID = 2
LOST_STAGE_ID = 50
SKIP_TITLES = {'NAN', 'NONE', 'NEGOCIO', 'FUNDO'}

def load_token():
    cfg = json.load(open(CFG))
    return cfg.get('pipedrive_token') or cfg.get('pipedrive_api_token') or ''

def norm(v):
    s = str(v or '').strip()
    s = unicodedata.normalize('NFKD', s)
    s = ''.join(ch for ch in s if not unicodedata.combining(ch))
    s = s.upper()
    s = re.sub(r'\s*-\s*LEAD\s*$', '', s)
    s = re.sub(r'\s*-\s*DEAL\s*$', '', s)
    s = re.sub(r'\s+', ' ', s).strip(' -')
    return s

def load_state():
    if not STATE_JSON.exists():
        return {'done': []}
    return json.load(open(STATE_JSON))

def save_state(state):
    STATE_JSON.parent.mkdir(parents=True, exist_ok=True)
    STATE_JSON.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding='utf-8')

def api_get_all_deals(token):
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
                'status': 'all_not_deleted',
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

def api_update_stage(token, deal_id, stage_id):
    r = requests.put(
        f'https://api.pipedrive.com/v1/deals/{deal_id}',
        params={'api_token': token},
        json={'stage_id': stage_id},
        timeout=60,
    )
    return r.status_code, r.text

token = load_token()
state = load_state()
done = set(state.get('done', []))

all_deals = api_get_all_deals(token)
p2 = [d for d in all_deals if int(d.get('pipeline_id') or 0) == PIPELINE_ID]

by_title = defaultdict(list)
for d in p2:
    t = norm(d.get('title'))
    by_title[t].append(d)

groups = {k: v for k, v in by_title.items() if len(v) > 1}

results = []
counts = Counter()

print(f'[START] duplicate_title_groups={len(groups)}', flush=True)

for title, deals in sorted(groups.items(), key=lambda kv: (-len(kv[1]), kv[0])):
    if title in SKIP_TITLES:
        counts['skip_title'] += 1
        results.append({
            'title': title,
            'status': 'skip_title',
            'deal_ids': [int(d.get('id') or 0) for d in deals],
        })
        continue

    key = f'{title}|{len(deals)}'
    if key in done:
        counts['already_done'] += 1
        continue

    deals = sorted(deals, key=lambda d: int(d.get('id') or 10**12))
    master = deals[0]
    dups = deals[1:]

    group_result = {
        'title': title,
        'master_deal_id': int(master.get('id') or 0),
        'duplicate_deal_ids': [int(d.get('id') or 0) for d in dups],
        'status': 'processed',
        'errors': [],
    }

    print(f'[GROUP] title={title} master={group_result["master_deal_id"]} dups={group_result["duplicate_deal_ids"]}', flush=True)

    for d in dups:
        deal_id = int(d.get('id') or 0)
        status_code, body = api_update_stage(token, deal_id, LOST_STAGE_ID)
        if 200 <= status_code < 300:
            print(f'[DUP_LOST_OK] title={title} dup={deal_id}', flush=True)
            counts['dup_lost_ok'] += 1
        else:
            print(f'[DUP_LOST_ERROR] title={title} dup={deal_id} status={status_code}', flush=True)
            counts['dup_lost_error'] += 1
            group_result['errors'].append({
                'deal_id': deal_id,
                'status_code': status_code,
                'body': body[:300],
            })

    done.add(key)
    save_state({'done': sorted(done)})
    results.append(group_result)
    counts['groups_processed'] += 1

LOG_JSON.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding='utf-8')

print('[FINAL_COUNTS]', flush=True)
for k, v in sorted(counts.items()):
    print(f'{k}={v}', flush=True)
print(f'[RESULT_JSON] {LOG_JSON}', flush=True)
