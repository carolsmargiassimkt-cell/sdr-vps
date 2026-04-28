import json, re, time, requests
from pathlib import Path
import re


def _looks_like_company_name(nome: str) -> bool:
    if not nome:
        return True
    n = re.sub(r"\s+", " ", str(nome)).strip().upper()

    termos_empresa = [
        " LTDA", "LTDA ", " S/A", "S/A ", " S. A.", " SA ", " S A ",
        "EIRELI", "HOLDING", "INDUSTRIA", "INDÚSTRIA", "COMERCIO", "COMÉRCIO",
        "DISTRIBUIDORA", "CONDOMINIO", "CONDOMÍNIO", "COOPERATIVA",
        "ASSOCIACAO", "ASSOCIAÇÃO", "FUNDO", "SHOPPING", "EMPRESARIAL",
        "ADMINISTRADORA", "PARTICIPACOES", "PARTICIPAÇÕES", "LOGISTICA",
        "LOGÍSTICA", "TRANSPORTES", "AGROINDUSTRIAL", "SPE", "CORP", "CORPORATION",
        "INC", "LLC", "B.V.", " BV", " GMBH", " SROC", " PLC"
    ]

    if any(t in f" {n} " for t in termos_empresa):
        return True

    # muitos numeros costuma indicar razao social/cadastro
    if len(re.findall(r"\d", n)) >= 3:
        return True

    # tudo muito "corporativo"
    palavras = n.split()
    if len(palavras) >= 4:
        caixa_alta = sum(1 for p in palavras if p.isupper() and len(p) > 2)
        if caixa_alta >= max(3, len(palavras) - 1):
            return True

    return False


def _looks_like_placeholder_name(nome: str) -> bool:
    if not nome:
        return True
    n = re.sub(r"\s+", " ", str(nome)).strip().upper()
    placeholders = {
        "RESPONSAVEL", "RESPONSÁVEL", "CONTATO", "SEM NOME", "N/A", "NA",
        "A DEFINIR", "A-DEFINIR", "NAO INFORMADO", "NÃO INFORMADO"
    }
    return n in placeholders


def _is_valid_person_name(nome: str) -> bool:
    if not nome:
        return False

    n = re.sub(r"\s+", " ", str(nome)).strip()
    if len(n) < 8:
        return False

    partes = [p for p in n.split() if p]
    if len(partes) < 2:
        return False

    if _looks_like_placeholder_name(n):
        return False

    if _looks_like_company_name(n):
        return False

    return True


CFG = Path('/root/sdr-vps/config/system_config.json')
cfg = json.load(open(CFG))
PD_TOKEN = cfg.get('pipedrive_token') or cfg.get('pipedrive_api_token') or ''

OPP_TOKEN = "6a15950c356566f697425b11567f391e5993e544d3030bc836e41fd07a2599dd"
CNPJ_KEY = 'aa3d1d254e5f80a2e5c791cdd390ef58ccfb68f0'
PIPELINE_ID = 2
OUT = Path('/root/sdr-vps/runtime/enrich_from_crm_cnpj_only_safe_result.json')

BAD_TERMS = ['CEBRAC', 'RESPONSAVEL', 'RESPONSÁVEL', 'FARMACIA INDIANA']
BAD_EMAILS = {'andrea@sherwin.com.br'}
BAD_PHONES = {'1121375000'}

def digits(v):
    return re.sub(r'\D+', '', str(v or ''))

def norm(v):
    return re.sub(r'\s+', ' ', str(v or '').strip().upper())

def is_bad_name(name):
    n = norm(name)
    return any(t in n for t in BAD_TERMS)

def is_bad_email(email):
    return str(email or '').strip().lower() in BAD_EMAILS

def is_bad_phone(phone):
    return digits(phone) in BAD_PHONES

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

def get_all_open_p2():
    out, start = [], 0
    while True:
        r = requests.get(
            'https://api.pipedrive.com/v1/deals',
            params={'api_token': PD_TOKEN, 'status': 'open', 'start': start, 'limit': 500},
            timeout=30
        )
        r.raise_for_status()
        payload = r.json()
        batch = payload.get('data') or []
        out.extend(batch)
        pag = payload.get('additional_data', {}).get('pagination', {})
        if not pag.get('more_items_in_collection', False) or not batch:
            break
        start += 500
    return [d for d in out if int(d.get('pipeline_id') or 0) == PIPELINE_ID]

def person_has_good_contact(person_id):
    if not person_id:
        return False
    p = pd_get(f'persons/{person_id}') or {}
    name = p.get('name') or ''
    emails = [x.get('value') for x in (p.get('email') or []) if isinstance(x, dict) and x.get('value')]
    phones = [x.get('value') for x in (p.get('phone') or []) if isinstance(x, dict) and x.get('value')]

    if is_bad_name(name):
        return False
    if any(is_bad_email(e) for e in emails):
        return False
    if any(is_bad_phone(ph) for ph in phones):
        return False

    return bool(emails or phones)

deals = get_all_open_p2()
report = []

for deal in deals:
    deal_id = int(deal.get('id') or 0)
    title = deal.get('title') or ''
    org_name = deal.get('org_name') or ''

    pid = deal.get('person_id') or {}
    old_person_id = pid.get('value') if isinstance(pid, dict) else pid
    old_person_name = pid.get('name') if isinstance(pid, dict) else ''

    item = {
        'deal_id': deal_id,
        'title': title,
        'org_name': org_name,
        'old_person_id': old_person_id,
        'old_person_name': old_person_name,
        'cnpj': '',
        'status': 'skip',
        'reason': '',
        'new_person_id': None,
        'new_person_name': None,
    }

    if old_person_id and person_has_good_contact(old_person_id):
        item['reason'] = 'ja_tem_contato_bom'
        report.append(item)
        continue

    org = deal.get('org_id') or {}
    org_id = org.get('value') if isinstance(org, dict) else org
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

    opp = fetch_opp(cnpj)
    if not opp:
        item['reason'] = 'oportunidados_sem_resposta'
        report.append(item)
        continue

    company = opp.get('company') or {}
    socios = opp.get('socios') or []

    opp_rs = norm(company.get('razao_social') or '')
    opp_nf = norm(company.get('nome_fantasia') or '')
    org_n = norm(org_name)

    # trava mínima: org do CRM tem que lembrar a empresa da API
    coherent = False
    for cand in [opp_rs, opp_nf]:
        if cand and (org_n in cand or cand in org_n):
            coherent = True
            break
    if not coherent:
        item['reason'] = 'empresa_api_inconsistente'
        report.append(item)
        continue

    email = (company.get('email') or '').strip()
    phone = digits(f"{company.get('ddd_1') or ''}{company.get('telefone_1') or ''}")

    if is_bad_email(email):
        email = ''
    if is_bad_phone(phone):
        phone = ''

    person_name = ''
    if not _is_valid_person_name(person_name):
        print({'status': 'skipped_invalid_person', 'invalid_person_name': person_name})
        continue
    if socios and socios[0].get('nome_socio'):
        socio = socios[0]['nome_socio'].strip()
        if not is_bad_name(socio):
            person_name = socio

    if not person_name:
        if company.get('nome_fantasia'):
            person_name = f"Contato - {company.get('nome_fantasia').strip()}"
        else:
            person_name = f"Contato - {org_name}"

    payload = {'name': person_name, 'org_id': org_id}
    if email:
        payload['email'] = [{'value': email, 'primary': True}]
    if phone:
        payload['phone'] = [{'value': phone, 'primary': True}]

    created = pd_post('persons', payload)
    new_person_id = created.get('id')

    pd_put(f'deals/{deal_id}', {'person_id': new_person_id})

    try:
        parts = pd_get(f'deals/{deal_id}/participants') or []
        if len(parts) == 0:
            pd_post(f'deals/{deal_id}/participants', {'person_id': new_person_id})
    except Exception:
        pass

    item['status'] = 'filled'
    item['new_person_id'] = new_person_id
    item['new_person_name'] = person_name
    report.append(item)
    print(item)
    time.sleep(0.25)

OUT.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
print('OUT =', OUT)
