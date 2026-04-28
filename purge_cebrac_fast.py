import json, requests, re, unicodedata
from pathlib import Path

CFG = Path('/root/sdr-vps/config/system_config.json')

TARGET_CNPJ = '07065870000198'
TARGET_NAME = 'CEBRAC'

cfg = json.load(open(CFG))
TOKEN = cfg.get('pipedrive_token') or cfg.get('pipedrive_api_token') or ''

def norm(v):
    s = unicodedata.normalize('NFKD', str(v or ''))
    s = ''.join(ch for ch in s if not unicodedata.combining(ch))
    return s.upper()

def get_deals():
    out=[]
    start=0
    while True:
        r=requests.get(
            'https://api.pipedrive.com/v1/deals',
            params={'api_token':TOKEN,'status':'open','start':start,'limit':500}
        )
        data=r.json()
        batch=data.get('data') or []
        out+=batch
        if not data.get('additional_data',{}).get('pagination',{}).get('more_items_in_collection'):
            break
        start+=500
    return [d for d in out if int(d.get('pipeline_id') or 0)==2]

def get_person(pid):
    r=requests.get(
        f'https://api.pipedrive.com/v1/persons/{pid}',
        params={'api_token':TOKEN}
    )
    return (r.json().get('data') or {}) if r.status_code<400 else {}

def fix_deal_person(deal_id,new_pid):
    return requests.put(
        f'https://api.pipedrive.com/v1/deals/{deal_id}',
        params={'api_token':TOKEN},
        json={'person_id':new_pid}
    ).status_code

deals = get_deals()
print("TOTAL DEALS:", len(deals))

fixed=0
review=0

for d in deals:
    deal_id = d['id']
    pid = (d.get('person_id') or {}).get('value') if isinstance(d.get('person_id'),dict) else d.get('person_id')

    if not pid:
        continue

    p = get_person(pid)
    pname = norm(p.get('name'))

    # detecta contaminação
    if TARGET_NAME in pname:
        print("CONTAMINATED:", deal_id, pname)

        # remove pessoa (zera)
        sc = fix_deal_person(deal_id, None)

        if sc < 300:
            fixed+=1
            print("REMOVED PERSON", deal_id)
        else:
            review+=1

print("\nRESULT:")
print("fixed =", fixed)
print("review =", review)
