import csv, json, re, time, requests
from pathlib import Path

CFG = Path('/root/sdr-vps/config/system_config.json')
cfg = json.load(open(CFG))
PD_TOKEN = cfg.get('pipedrive_token') or cfg.get('pipedrive_api_token') or ''

OPP_TOKEN = "6a15950c356566f697425b11567f391e5993e544d3030bc836e41fd07a2599dd"
CNPJ_KEY = 'aa3d1d254e5f80a2e5c791cdd390ef58ccfb68f0'
PIPELINE_ID = 2

CRM_CSV = Path('/root/sdr-vps/runtime/base_leads_com_linkedin.csv')
OUT = Path('/root/sdr-vps/runtime/enrich_missing_after_cleanup_from_linkedin_csv_result.json')

TARGET_DEALS = [2961, 2978, 2979, 2980, 2981, 2982, 2983, 2984, 2985, 3001, 3002]

def digits(v):
    return re.sub(r'\D+', '', str(v or ''))

def norm(v):
    return re.sub(r'\s+', ' ', str(v or '').strip().upper())

def pd_get(path, **params):
    r = requests.get(
        f'https://api.pipedrive.com/v1/{path}',
        params={'api_token': PD_TOKEN, **params},
        timeout=30
    )
    r.raise_for_status()
    return r.json().get('data')

def pd_post(path, payload):
    r = requests.post(
        f'https://api.pipedrive.com/v1/{path}',
        params={'api_token': PD_TOKEN},
        json=payload,
        timeout=30
    )
    r.raise_for_status()
    return r.json().get('data')

def pd_put(path, payload):
    r = requests.put(
        f'https://api.pipedrive.com/v1/{path}',
        params={'api_token': PD_TOKEN},
        json=payload,
        timeout=30
    )
    r.raise_for_status()
    return r.json().get('data')

def fetch_opp(cnpj):
    r = requests.get(
        f'https://app.oportunidados.com.br/api/v1/brazilian_companies/{cnpj}/company',
        headers={'Authorization': f'Bearer {OPP_TOKEN}'},
        timeout=30
    )
    if r.status_code != 200:
        return None
    return r.json()

def load_crm_by_cnpj():
    idx = {}
    with open(CRM_CSV, encoding='utf-8-sig', newline='') as f:
        rd = csv.DictReader(f)
        for row in rd:
            cnpj = digits(row.get('cnpj_formatado') or '')
            if not cnpj:
                continue
            idx.setdefault(cnpj, []).append(row)
    return idx

def get_person_contact(person_id):
    if not person_id:
        return {}, False
    p = pd_get(f'persons/{person_id}') or {}
    emails = [x.get('value') for x in (p.get('email') or []) if isinstance(x, dict) and x.get('value')]
    phones = [digits(x.get('value')) for x in (p.get('phone') or []) if isinstance(x, dict) and x.get('value')]
    return p, bool(emails or phones)

crm_by_cnpj = load_crm_by_cnpj()
report = []

for deal_id in TARGET_DEALS:
    deal = pd_get(f'deals/{deal_id}') or {}
    title = deal.get('title') or ''
    org_name = deal.get('org_name') or ''
    org = deal.get('org_id') or {}
    org_id = org.get('value') if isinstance(org, dict) else org

    pid = deal.get('person_id') or {}
    person_id = pid.get('value') if isinstance(pid, dict) else pid
    person_name = pid.get('name') if isinstance(pid, dict) else ''

    item = {
        'deal_id': deal_id,
        'title': title,
        'org_name': org_name,
        'org_id': org_id,
        'old_person_id': person_id,
        'old_person_name': person_name,
        'cnpj': '',
        'status': 'skip',
        'reason': '',
        'new_person_id': None,
        'new_person_name': None,
    }

    person_data, has_contact = get_person_contact(person_id) if person_id else ({}, False)

    # só tenta enriquecer quando falta pessoa ou falta contato
    if person_id and has_contact:
        item['reason'] = 'ja_tem_contato'
        report.append(item)
        continue

    if not org_id:
        item['reason'] = 'sem_org_id'
        report.append(item)
        continue

    org_data = pd_get(f'organizations/{org_id}') or {}
    cnpj = digits(org_data.get(CNPJ_KEY))
    item['cnpj'] = cnpj

    if not cnpj:
        item['reason'] = 'sem_cnpj_org'
        report.append(item)
        continue

    crm_rows = crm_by_cnpj.get(cnpj, [])
    if len(crm_rows) != 1:
        item['reason'] = f'crm_matchs_por_cnpj={len(crm_rows)}'
        report.append(item)
        continue

    crm = crm_rows[0]
    opp = fetch_opp(cnpj)
    company = (opp or {}).get('company') or {}
    socios = (opp or {}).get('socios') or []

    crm_org = norm(crm.get('Organização - Nome') or '')
    crm_api_razao = norm(crm.get('API_RAZAO') or '')
    org_n = norm(org_name)
    opp_rs = norm(company.get('razao_social') or '')
    opp_nf = norm(company.get('nome_fantasia') or '')

    # trava de coerência
    coherent = False
    for cand in [crm_org, crm_api_razao, opp_rs, opp_nf]:
        if cand and (org_n in cand or cand in org_n):
            coherent = True
            break
    if not coherent:
        item['reason'] = 'nome_empresa_inconsistente'
        report.append(item)
        continue

    # prioridade de contato: planilha > oportunidade
    email = (crm.get('API_EMAIL') or '').strip() or (company.get('email') or '').strip()
    phone = digits(crm.get('API_TEL_OFICIAL') or '')
    if not phone:
        phone = digits(f"{company.get('ddd_1') or ''}{company.get('telefone_1') or ''}")

    # prioridade de nome: pessoa da planilha > socio > neutro
    person_name_new = (crm.get('Pessoa - Nome') or '').strip()
    if not person_name_new:
        if socios and socios[0].get('nome_socio'):
            person_name_new = socios[0]['nome_socio'].strip()
        elif crm.get('Organização - Nome'):
            person_name_new = f"Contato - {crm.get('Organização - Nome').strip()}"
        elif company.get('nome_fantasia'):
            person_name_new = f"Contato - {company.get('nome_fantasia').strip()}"
        else:
            person_name_new = f"Contato - {org_name}"

    payload = {'name': person_name_new, 'org_id': org_id}
    if email:
        payload['email'] = [{'value': email, 'primary': True}]
    if phone:
        payload['phone'] = [{'value': phone, 'primary': True}]

    created = pd_post('persons', payload)
    new_person_id = created.get('id')

    pd_put(f'deals/{deal_id}', {'person_id': new_person_id})

    # deixa como unico participant se ainda não houver participant
    try:
        parts = pd_get(f'deals/{deal_id}/participants') or []
        if len(parts) == 0:
            pd_post(f'deals/{deal_id}/participants', {'person_id': new_person_id})
    except Exception:
        pass

    item['status'] = 'filled'
    item['new_person_id'] = new_person_id
    item['new_person_name'] = person_name_new
    report.append(item)
    print(item)
    time.sleep(0.25)

OUT.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
print('OUT =', OUT)
