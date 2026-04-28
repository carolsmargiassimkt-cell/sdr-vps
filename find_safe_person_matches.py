import json, requests, re
from pathlib import Path

CFG = Path('/root/sdr-vps/config/system_config.json')
cfg = json.load(open(CFG))
TOKEN = cfg.get('pipedrive_token') or cfg.get('pipedrive_api_token') or ''

cases = [
    {
        "deal_id": 2961,
        "expected_org": "ATACADAO DAS TINTAS",
        "email": "atacadaodastintas.loja@gmail.com",
        "phone": "71999215454",
        "name": "NELSON REIS DULTRA FILHO",
    },
    {
        "deal_id": 3001,
        "expected_org": "MANDIOCA PREDILECTA",
        "email": "waltiljunior@gmail.com",
        "phone": "62994535322",
        "name": "WALTIL NUNES DE OLIVEIRA JUNIOR",
    },
    {
        "deal_id": 3002,
        "expected_org": "QUALISEG",
        "email": "qualiseg@yahoo.com.br",
        "phone": "8225256893",
        "name": "WELINGTON WAGNER MEDEIROS",
    },
]

def digits(v):
    return re.sub(r'\D+', '', str(v or ''))

def search_person(term):
    r = requests.get(
        'https://api.pipedrive.com/v1/persons/search',
        params={
            'api_token': TOKEN,
            'term': term,
            'fields': 'name,email,phone',
            'exact_match': False,
            'limit': 20,
        },
        timeout=20
    )
    r.raise_for_status()
    return r.json().get('data', {}).get('items', []) or []

def norm_multi(values, is_phone=False):
    out = []
    for x in values or []:
        if isinstance(x, dict):
            v = x.get('value')
        else:
            v = x
        if not v:
            continue
        v = digits(v) if is_phone else str(v).strip()
        if v:
            out.append(v)
    return out

def simplify(items):
    out = []
    for it in items:
        item = it.get('item') or {}
        person_id = item.get('id')
        name = item.get('name')
        org = item.get('organization') or {}
        org_name = org.get('name') if isinstance(org, dict) else ''
        emails = norm_multi(item.get('emails') or [], is_phone=False)
        phones = norm_multi(item.get('phones') or [], is_phone=True)
        out.append({
            'person_id': person_id,
            'name': name,
            'org_name': org_name,
            'emails': emails,
            'phones': phones,
        })
    return out

for c in cases:
    print("\n" + "="*80)
    print(f"DEAL {c['deal_id']} | ORG esperada: {c['expected_org']}")
    print(f"EMAIL: {c['email']}")
    print(f"PHONE: {c['phone']}")
    print(f"NOME: {c['name']}")

    by_email = simplify(search_person(c['email'])) if c['email'] else []
    by_phone = simplify(search_person(c['phone'])) if c['phone'] else []
    by_name = simplify(search_person(c['name'])) if c['name'] else []

    print("\n-- MATCH POR EMAIL --")
    print(json.dumps(by_email[:10], ensure_ascii=False, indent=2))

    print("\n-- MATCH POR PHONE --")
    print(json.dumps(by_phone[:10], ensure_ascii=False, indent=2))

    print("\n-- MATCH POR NOME --")
    print(json.dumps(by_name[:10], ensure_ascii=False, indent=2))
