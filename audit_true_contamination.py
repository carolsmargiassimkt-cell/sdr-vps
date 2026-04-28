import json, unicodedata, requests
from pathlib import Path

CFG = Path('/root/sdr-vps/config/system_config.json')
OUT_JSON = Path('/root/sdr-vps/runtime/audit_true_contamination.json')
PIPELINE_ID = 2
SUSPECT_TERMS = ['FARMACIA INDIANA', 'CEBRAC', 'EXCLU']

cfg = json.load(open(CFG))
TOKEN = cfg.get('pipedrive_token') or cfg.get('pipedrive_api_token') or ''

def norm(v):
    s = unicodedata.normalize('NFKD', str(v or ''))
    s = ''.join(ch for ch in s if not unicodedata.combining(ch))
    return s.upper().strip()

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

def get_all_open_p2():
    out, start = [], 0
    while True:
        r = requests.get(
            'https://api.pipedrive.com/v1/deals',
            params={'api_token': TOKEN, 'status': 'open', 'start': start, 'limit': 500},
            timeout=10
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

def get_participants(deal_id):
    r = requests.get(
        f'https://api.pipedrive.com/v1/deals/{deal_id}/participants',
        params={'api_token': TOKEN},
        timeout=10
    )
    if r.status_code >= 400:
        return []
    return r.json().get('data') or []

rows = []
deals = get_all_open_p2()
print("TOTAL DEALS =", len(deals))
rows = []
for i, d in enumerate(deals, 1):
    deal_id = int(d.get('id') or 0)
    print(f"Processando {i}/{len(deals)} deal_id={deal_id}")
    deal_id = int(d.get('id') or 0)
    title = d.get('title') or ''
    org_name = d.get('org_name') or ''
    pid = d.get('person_id')
    person_name = pid.get('name','') if isinstance(pid, dict) else ''
    suspicious_main = any(t in norm(person_name) for t in SUSPECT_TERMS)

    parts = get_participants(deal_id)
    suspicious_parts = []
    for p in parts:
        name = ''
        if isinstance(p.get('person_id'), dict):
            name = p['person_id'].get('name','')
        elif isinstance(p.get('person'), dict):
            name = p['person'].get('name','')
        if any(t in norm(name) for t in SUSPECT_TERMS):
            suspicious_parts.append(name)

    if suspicious_main or suspicious_parts or len(parts) >= 20:
        rows.append({
            'deal_id': deal_id,
            'title': title,
            'org_name': org_name,
            'main_person': person_name,
            'suspicious_main': suspicious_main,
            'participant_count': len(parts),
            'suspicious_participants': suspicious_parts[:20],
        })

OUT_JSON.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding='utf-8')
print('COUNT =', len(rows))
print('OUT_JSON =', OUT_JSON)
for r in rows[:50]:
    print(r)
