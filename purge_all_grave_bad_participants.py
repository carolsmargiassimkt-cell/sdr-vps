import json, requests, time
from pathlib import Path

CFG = Path('/root/sdr-vps/config/system_config.json')
INP = Path('/root/sdr-vps/runtime/find_global_bad_participants.json')
OUT = Path('/root/sdr-vps/runtime/purge_all_grave_bad_participants_result.json')

cfg = json.load(open(CFG))
TOKEN = cfg.get('pipedrive_token') or cfg.get('pipedrive_api_token') or ''

rows = json.load(open(INP, encoding='utf-8'))

seen = set()
results = []

for row in rows:
    deal_id = row['deal_id']
    deal_participant_id = row['deal_participant_id']
    key = (deal_id, deal_participant_id)
    if key in seen:
        continue
    seen.add(key)

    r = requests.delete(
        f'https://api.pipedrive.com/v1/deals/{deal_id}/participants/{deal_participant_id}',
        params={'api_token': TOKEN},
        timeout=20
    )

    item = {
        'deal_id': deal_id,
        'deal_participant_id': deal_participant_id,
        'person_id': row.get('person_id'),
        'person_name': row.get('person_name'),
        'person_org_name': row.get('person_org_name'),
        'status_code': r.status_code,
    }
    results.append(item)
    print(item)
    time.sleep(0.12)

OUT.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding='utf-8')
print('TOTAL_ATTEMPTS =', len(results))
print('OUT =', OUT)
