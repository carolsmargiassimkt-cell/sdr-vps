import csv, json, requests
from pathlib import Path

CFG = Path('/root/sdr-vps/config/system_config.json')
CSV_IN = Path('/root/sdr-vps/runtime/rescue_lost_fixed_32.csv')
RESULT_JSON = Path('/root/sdr-vps/runtime/revert_32_rescued_to_lost_result.json')
LOST_STAGE_ID = 50

cfg = json.load(open(CFG))
TOKEN = cfg.get('pipedrive_token') or cfg.get('pipedrive_api_token') or ''

rows = list(csv.DictReader(open(CSV_IN, encoding='utf-8')))
results = []

for r in rows:
    deal_id = int(r['deal_id'])
    resp = requests.put(
        f'https://api.pipedrive.com/v1/deals/{deal_id}',
        params={'api_token': TOKEN},
        json={'stage_id': LOST_STAGE_ID},
        timeout=60,
    )
    results.append({
        'deal_id': deal_id,
        'title': r.get('title'),
        'status_code': resp.status_code,
        'ok': 200 <= resp.status_code < 300,
    })
    print(deal_id, resp.status_code, flush=True)

RESULT_JSON.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding='utf-8')
print('RESULT_JSON =', RESULT_JSON)
