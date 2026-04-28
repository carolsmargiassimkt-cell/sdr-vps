import json, requests

cfg = json.load(open('/root/sdr-vps/config/system_config.json'))
TOKEN = cfg.get('pipedrive_token') or cfg.get('pipedrive_api_token') or ''

deal_ids = [993, 994, 1008, 1010, 1062, 1064, 1077, 2807]

for deal_id in deal_ids:
    r = requests.put(
        f'https://api.pipedrive.com/v1/deals/{deal_id}',
        params={'api_token': TOKEN},
        json={
            'status': 'lost'
        },
        timeout=60,
    )
    try:
        body = r.json()
    except:
        body = r.text
    print(deal_id, r.status_code, body)
