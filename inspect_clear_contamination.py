import json, requests
from pathlib import Path

CFG = Path('/root/sdr-vps/config/system_config.json')
OUT = Path('/root/sdr-vps/runtime/inspect_clear_contamination.json')
IDS = Path('/root/sdr-vps/runtime/clear_contamination_ids.txt')

cfg = json.load(open(CFG))
TOKEN = cfg.get('pipedrive_token') or cfg.get('pipedrive_api_token') or ''

deal_ids = [int(x.strip()) for x in open(IDS, encoding='utf-8') if x.strip()]

def get(path, **params):
    r = requests.get(
        f'https://api.pipedrive.com/v1/{path}',
        params={'api_token': TOKEN, **params},
        timeout=20
    )
    r.raise_for_status()
    return r.json().get('data')

rows = []

for deal_id in deal_ids:
    d = get(f'deals/{deal_id}') or {}
    parts = get(f'deals/{deal_id}/participants') or []
    rows.append({
        'deal_id': deal_id,
        'title': d.get('title'),
        'org_id': (d.get('org_id') or {}).get('value') if isinstance(d.get('org_id'), dict) else d.get('org_id'),
        'org_name': d.get('org_name'),
        'person_id': (d.get('person_id') or {}).get('value') if isinstance(d.get('person_id'), dict) else d.get('person_id'),
        'person_name': (d.get('person_id') or {}).get('name') if isinstance(d.get('person_id'), dict) else '',
        'participants': [
            {
                'id': p.get('id'),
                'person_id': (p.get('person_id') or {}).get('value') if isinstance(p.get('person_id'), dict) else p.get('person_id'),
                'name': (p.get('person_id') or {}).get('name') if isinstance(p.get('person_id'), dict) else ''
            }
            for p in parts
        ]
    })

OUT.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding='utf-8')
print("OUT =", OUT)
print("COUNT =", len(rows))
for r in rows:
    print(json.dumps(r, ensure_ascii=False))
