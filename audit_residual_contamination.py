import json, re, unicodedata, requests
from pathlib import Path

CFG = Path('/root/sdr-vps/config/system_config.json')
OUT_JSON = Path('/root/sdr-vps/runtime/audit_residual_contamination.json')
PIPELINE_ID = 2
SUSPECT_TERMS = ['FARMACIA INDIANA', 'CEBRAC', 'EXCLU']

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

def norm_key(v):
    return re.sub(r'[^A-Z0-9]+', '', norm(v))

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

def get_person(pid):
    r = requests.get(
        f'https://api.pipedrive.com/v1/persons/{pid}',
        params={'api_token': TOKEN},
        timeout=60,
    )
    if r.status_code >= 400:
        return {}
    return r.json().get('data') or {}

def get_participants(deal_id):
    r = requests.get(
        f'https://api.pipedrive.com/v1/deals/{deal_id}/participants',
        params={'api_token': TOKEN},
        timeout=60,
    )
    if r.status_code >= 400:
        return []
    return r.json().get('data') or []

rows = []
deals = get_all_open_p2()

for d in deals:
    deal_id = int(d.get('id') or 0)
    title = d.get('title') or ''
    title_key = norm_key(title)
    org_name = d.get('org_name') or ''
    org_key = norm_key(org_name)
    person_id = extract_id(d.get('person_id'))

    person_name = ''
    suspicious_main_person = False
    if person_id:
        p = get_person(person_id)
        person_name = p.get('name') or ''
        pname = norm(person_name)
        suspicious_main_person = any(term in pname for term in SUSPECT_TERMS)

    parts = get_participants(deal_id)
    suspect_participants = []
    for part in parts:
        pid = extract_id(part.get('person_id') or part.get('person'))
        name = ''
        if isinstance(part.get('person_id'), dict):
            name = part.get('person_id', {}).get('name') or ''
        elif isinstance(part.get('person'), dict):
            name = part.get('person', {}).get('name') or ''
        n = norm(name)
        if any(term in n for term in SUSPECT_TERMS):
            suspect_participants.append({'person_id': pid, 'name': name})

    title_org_mismatch = bool(org_key and title_key and org_key not in title_key and title_key not in org_key)

    if len(parts) >= 10 or suspicious_main_person or suspect_participants or title_org_mismatch:
        rows.append({
            'deal_id': deal_id,
            'title': title,
            'org_name': org_name,
            'person_id': person_id,
            'person_name': person_name,
            'participant_count': len(parts),
            'title_org_mismatch': title_org_mismatch,
            'suspicious_main_person': suspicious_main_person,
            'suspect_participants': suspect_participants,
        })

OUT_JSON.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding='utf-8')
print('COUNT =', len(rows))
print('OUT_JSON =', OUT_JSON)
for r in rows[:50]:
    print(r)
