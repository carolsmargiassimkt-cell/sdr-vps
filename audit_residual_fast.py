import json, re, unicodedata, requests
from pathlib import Path

CFG = Path('/root/sdr-vps/config/system_config.json')
OUT_JSON = Path('/root/sdr-vps/runtime/audit_residual_fast.json')
PIPELINE_ID = 2
SUSPECT_TERMS = ['FARMACIA INDIANA', 'CEBRAC']

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

rows=[]
deals=get_deals()

for d in deals:
    deal_id=d['id']
    title=d.get('title','')
    org=d.get('org_name','')

    pid = d.get('person_id')
    pname=''

    if isinstance(pid, dict):
        pname = pid.get('name','')
    else:
        pname = ''

    suspicious = any(t in norm(pname) for t in SUSPECT_TERMS)

    mismatch = False
    if org and title:
        if norm(org) not in norm(title) and norm(title) not in norm(org):
            mismatch = True

    if suspicious or mismatch:
        rows.append({
            'deal_id':deal_id,
            'title':title,
            'org':org,
            'person':pname,
            'suspicious_person':suspicious,
            'org_mismatch':mismatch
        })

print("COUNT =", len(rows))
OUT_JSON.write_text(json.dumps(rows, indent=2, ensure_ascii=False))
print("OUT =", OUT_JSON)

for r in rows[:50]:
    print(r)
