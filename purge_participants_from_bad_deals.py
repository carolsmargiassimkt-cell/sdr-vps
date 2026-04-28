import json, requests, time
from pathlib import Path

CFG = Path('/root/sdr-vps/config/system_config.json')
BACKUP = Path('/root/sdr-vps/runtime/bad_deal_participants_backup.json')

cfg = json.load(open(CFG))
TOKEN = cfg.get('pipedrive_token') or cfg.get('pipedrive_api_token') or ''

rows = json.load(open(BACKUP, encoding='utf-8'))

for row in rows:
    deal_id = row['deal_id']
    print(f"\nDEAL {deal_id}")
    for x in row['participants']:
        participant_id = x.get('id')
        person = x.get('person_id') or {}
        name = person.get('name') if isinstance(person, dict) else ''
        r = requests.delete(
            f'https://api.pipedrive.com/v1/participants/{participant_id}',
            params={'api_token': TOKEN},
            timeout=20
        )
        print(f"deal={deal_id} participant_id={participant_id} status={r.status_code} name={name}")
        time.sleep(0.25)
