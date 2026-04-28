import json, requests, re, time
from pathlib import Path

CFG = Path('/root/sdr-vps/config/system_config.json')
cfg = json.load(open(CFG))
PD_TOKEN = cfg.get('pipedrive_token') or cfg.get('pipedrive_api_token') or ''

OPP_TOKEN = "6a15950c356566f697425b11567f391e5993e544d3030bc836e41fd07a2599dd"
CNPJ_KEY = 'aa3d1d254e5f80a2e5c791cdd390ef58ccfb68f0'

DEAL_IDS = [2978, 2981, 2983, 2984]

def digits(v):
    return re.sub(r'\D+', '', str(v or ''))

def pd_get(path, **params):
    r = requests.get(
        f'https://api.pipedrive.com/v1/{path}',
        params={'api_token': PD_TOKEN, **params},
        timeout=20
    )
    r.raise_for_status()
    return r.json().get('data')

def pd_post(path, payload):
    r = requests.post(
        f'https://api.pipedrive.com/v1/{path}',
        params={'api_token': PD_TOKEN},
        json=payload,
        timeout=20
    )
    r.raise_for_status()
    return r.json().get('data')

def pd_put(path, payload):
    r = requests.put(
        f'https://api.pipedrive.com/v1/{path}',
        params={'api_token': PD_TOKEN},
        json=payload,
        timeout=20
    )
    r.raise_for_status()
    return r.json().get('data')

def fetch_opp(cnpj):
    r = requests.get(
        f'https://app.oportunidados.com.br/api/v1/brazilian_companies/{cnpj}/company',
        headers={'Authorization': f'Bearer {OPP_TOKEN}'},
        timeout=20
    )
    if r.status_code != 200:
        return None
    return r.json()

report = []

for deal_id in DEAL_IDS:
    deal = pd_get(f'deals/{deal_id}') or {}
    org = deal.get('org_id') or {}
    org_id = org.get('value') if isinstance(org, dict) else org

    item = {
        'deal_id': deal_id,
        'title': deal.get('title'),
        'org_id': org_id,
        'org_name': deal.get('org_name'),
        'old_person_id': None,
        'cnpj': '',
        'status': 'skip',
        'reason': '',
        'created_person_id': None,
        'created_person_name': None,
    }

    pid = deal.get('person_id') or {}
    old_person_id = pid.get('value') if isinstance(pid, dict) else pid
    item['old_person_id'] = old_person_id
    if old_person_id:
        item['reason'] = 'deal ja tem person_id'
        report.append(item)
        continue

    if not org_id:
        item['reason'] = 'deal sem org_id'
        report.append(item)
        continue

    org_data = pd_get(f'organizations/{org_id}') or {}
    cnpj = digits(org_data.get(CNPJ_KEY))
    item['cnpj'] = cnpj

    if not cnpj:
        item['reason'] = 'org sem cnpj'
        report.append(item)
        continue

    opp = fetch_opp(cnpj)
    if not opp:
        item['reason'] = 'oportunidados sem resposta'
        report.append(item)
        continue

    company = opp.get('company') or {}
    socios = opp.get('socios') or []

    email = (company.get('email') or '').strip()
    phone = digits(f"{company.get('ddd_1') or ''}{company.get('telefone_1') or ''}")

    # prioridade: socio; se nao houver, nome fantasia/razao com nome neutro
    if socios and socios[0].get('nome_socio'):
        person_name = socios[0]['nome_socio'].strip()
    elif company.get('nome_fantasia'):
        person_name = f"Contato - {company.get('nome_fantasia').strip()}"
    else:
        person_name = f"Contato - {item['org_name']}"

    payload = {
        'name': person_name,
        'org_id': org_id,
    }
    if email:
        payload['email'] = [{'value': email, 'primary': True}]
    if phone:
        payload['phone'] = [{'value': phone, 'primary': True}]

    created = pd_post('persons', payload)
    person_id = created.get('id')

    pd_put(f'deals/{deal_id}', {'person_id': person_id})

    # opcional: adiciona como participant unico
    try:
        pd_post(f'deals/{deal_id}/participants', {'person_id': person_id})
    except Exception:
        pass

    item['status'] = 'filled'
    item['created_person_id'] = person_id
    item['created_person_name'] = person_name
    report.append(item)
    print(item)
    time.sleep(0.4)

out = Path('/root/sdr-vps/runtime/fill_missing_persons_from_oportunidados_safe_result.json')
out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
print("OUT =", out)
