import json
import re
import unicodedata
import requests
from pathlib import Path

CFG = Path('/root/sdr-vps/config/system_config.json')
STATE_JSON = Path('/root/sdr-vps/runtime/fix_pipeline2_missing_orgs_state.json')
RESULT_JSON = Path('/root/sdr-vps/runtime/fix_pipeline2_missing_orgs_result.json')

PIPELINE_ID = 2

def load_token():
    cfg = json.load(open(CFG))
    return cfg.get('pipedrive_token') or cfg.get('pipedrive_api_token') or ''

def norm(v):
    s = str(v or '').strip()
    s = unicodedata.normalize('NFKD', s)
    s = ''.join(ch for ch in s if not unicodedata.combining(ch))
    s = s.upper()
    s = re.sub(r'\s*-\s*DEAL\s*$', '', s, flags=re.I)
    s = re.sub(r'\s*LEADSTER\s*$', '', s, flags=re.I)
    s = re.sub(r'\s+', ' ', s).strip(' -')
    return s

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

def api_search_orgs(token, term):
    r = requests.get(
        'https://api.pipedrive.com/v1/organizations/search',
        params={
            'api_token': token,
            'term': term,
            'limit': 10,
            'fields': 'name',
            'exact_match': False,
        },
        timeout=60,
    )
    if r.status_code >= 400:
        return []
    payload = r.json()
    return payload.get('data', {}).get('items', []) or []

def api_update_deal_org(token, deal_id, org_id):
    r = requests.put(
        f'https://api.pipedrive.com/v1/deals/{deal_id}',
        params={'api_token': token},
        json={'org_id': org_id},
        timeout=60,
    )
    return r.status_code, r.text

token = load_token()
state = load_state()
done = set(state.get('done', []))

all_deals = api_get_all_open_deals(token)
p2 = [d for d in all_deals if int(d.get('pipeline_id') or 0) == PIPELINE_ID]

missing = [d for d in p2 if not (d.get('org_id') and ((isinstance(d.get('org_id'), dict) and (d.get('org_id').get('value') or d.get('org_id').get('id'))) or (not isinstance(d.get('org_id'), dict) and d.get('org_id'))))]

results = []

print(f'[START] missing_org_deals={len(missing)}', flush=True)

for d in missing:
    deal_id = int(d.get('id') or 0)
    title = str(d.get('title') or '')
    key = str(deal_id)
    if key in done:
        continue

    candidates = []
    search_terms = [title, norm(title)]

    seen_org_ids = set()
    for term in search_terms:
        if not term:
            continue
        items = api_search_orgs(token, term)
        for item in items:
            org = item.get('item') or {}
            org_id = int(org.get('id') or 0)
            name = str(org.get('name') or '')
            if org_id and org_id not in seen_org_ids:
                seen_org_ids.add(org_id)
                candidates.append({'org_id': org_id, 'name': name})

    title_norm = norm(title)
    exact = [c for c in candidates if norm(c['name']) == title_norm]
    chosen = None
    reason = ''

    if len(exact) == 1:
        chosen = exact[0]
        reason = 'exact_name'
    elif len(candidates) == 1:
        chosen = candidates[0]
        reason = 'single_candidate'
    else:
        reason = 'ambiguous' if candidates else 'no_candidate'

    row = {
        'deal_id': deal_id,
        'title': title,
        'status': '',
        'reason': reason,
        'candidates': candidates[:10],
    }

    if chosen:
        status_code, body = api_update_deal_org(token, deal_id, chosen['org_id'])
        if 200 <= status_code < 300:
            row['status'] = 'fixed'
            row['org_id'] = chosen['org_id']
            row['org_name'] = chosen['name']
            print(f"[FIXED] deal={deal_id} org={chosen['org_id']} reason={reason}", flush=True)
        else:
            row['status'] = 'error'
            row['http_status'] = status_code
            row['body'] = body[:300]
            print(f"[ERROR] deal={deal_id} status={status_code}", flush=True)
    else:
        row['status'] = 'needs_review'
        print(f"[REVIEW] deal={deal_id} reason={reason}", flush=True)

    results.append(row)
    done.add(key)
    save_state({'done': sorted(done)})

RESULT_JSON.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding='utf-8')
print(f"[RESULT_JSON] {RESULT_JSON}", flush=True)
