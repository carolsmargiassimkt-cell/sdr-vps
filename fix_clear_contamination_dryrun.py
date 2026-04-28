import json, requests
from pathlib import Path

CFG = Path('/root/sdr-vps/config/system_config.json')
INSP = Path('/root/sdr-vps/runtime/inspect_clear_contamination.json')

cfg = json.load(open(CFG))
TOKEN = cfg.get('pipedrive_token') or cfg.get('pipedrive_api_token') or ''

rows = json.load(open(INSP, encoding='utf-8'))

for row in rows:
    deal_id = row['deal_id']
    bad_parts = [p for p in row.get('participants', []) if 'CEBRAC' in (p.get('name') or '').upper()]
    if row.get('person_name') and 'CEBRAC' in row['person_name'].upper():
        print(f"[ATENCAO] deal {deal_id}: person principal contaminada -> {row['person_name']}")
    for p in bad_parts:
        print(f"[DRY-RUN] remover participante deal={deal_id} participant_id={p['id']} nome={p['name']}")
