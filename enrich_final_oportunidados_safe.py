import json, re, time, requests, random
from pathlib import Path

CFG = Path('/root/sdr-vps/config/system_config.json')
cfg = json.load(open(CFG, encoding='utf-8'))
PD_TOKEN = cfg.get('pipedrive_token') or cfg.get('pipedrive_api_token') or ''
if not PD_TOKEN:
    raise SystemExit('ERRO: token do Pipedrive não encontrado em system_config.json')

# Mantidos do fluxo que você já vinha usando
OPP_TOKEN = "6a15950c356566f697425b11567f391e5993e544d3030bc836e41fd07a2599dd"
CNPJ_KEY = 'aa3d1d254e5f80a2e5c791cdd390ef58ccfb68f0'
PIPELINE_ID = 2

OUT = Path('/root/sdr-vps/runtime/enrich_final_oportunidados_safe_result.json')

BAD_TERMS = [
    'CEBRAC', 'RESPONSAVEL', 'RESPONSÁVEL',
    'FARMACIA INDIANA', 'FARMÁCIA INDIANA',
    'CONTATO PITÚ', 'CONTATO PITU'
]
BAD_EMAILS = {
    'andrea@sherwin.com.br',
}
BAD_PHONES = {
    '1121375000',
}

session = requests.Session()

def digits(v):
    return re.sub(r'\D+', '', str(v or ''))

def norm(v):
    return re.sub(r'\s+', ' ', str(v or '').strip().upper())

def title_case(s):
    s = re.sub(r'\s+', ' ', str(s or '').strip())
    return s


def definitely_company_name(name):
    n = norm(name)
    if not n:
        return False
    hard_terms = [
        'LTDA', 'S/A', ' SA', 'EIRELI', 'HOLDING', 'INDUSTRIA', 'INDÚSTRIA',
        'COMERCIO', 'COMÉRCIO', 'DISTRIBUIDORA', 'CONDOMINIO', 'CONDOMÍNIO',
        'ASSOCIACAO', 'ASSOCIAÇÃO', 'FUNDO', 'SHOPPING', 'EMPRESARIAL',
        'CIA', 'COMPANHIA', 'COOPERATIVA', 'PARTICIPACOES', 'PARTICIPAÇÕES',
        'INC.', ' INC', 'B.V.', ' BV', 'LLC', 'CORP'
    ]
    return any(t in f' {n} ' for t in hard_terms)

def is_bad_name(name):
    n = norm(name)
    if not n:
        return True
    if any(t in n for t in BAD_TERMS):
        return True
    # empresa como pessoa
    company_terms = [
        ' LTDA', ' S/A', ' SA', ' EIRELI', ' HOLDING', ' INDUSTRIA',
        ' INDÚSTRIA', ' COMERCIO', ' COMÉRCIO', ' DISTRIBUIDORA',
        ' CONDOMINIO', ' CONDOMÍNIO', ' ASSOCIACAO', ' ASSOCIAÇÃO',
        ' FUNDO', ' SHOPPING', ' EMPRESARIAL', ' CIA', ' COMPANHIA',
        ' COOPERATIVA', ' PARTICIPACOES', ' PARTICIPAÇÕES'
    ]
    if any(t in f' {n} ' for t in company_terms):
        return True
    # nome curto / placeholder
    if n in {'CONTATO', 'RESPONSAVEL', 'RESPONSÁVEL', 'DIRETOR', 'GERENTE'}:
        return True
    if len(n.split()) < 2:
        return True
    return False

def is_bad_email(email):
    e = str(email or '').strip().lower()
    if not e or e == 'n/a':
        return True
    return e in BAD_EMAILS

def is_bad_phone(phone):
    p = digits(phone)
    if not p:
        return True
    return p in BAD_PHONES

def _pd_request(method, path, **kwargs):
    url = f'https://api.pipedrive.com/v1/{path}'
    params = kwargs.pop('params', {})
    params = {'api_token': PD_TOKEN, **params}

    for attempt in range(8):
        r = session.request(method, url, params=params, timeout=30, **kwargs)

        if r.status_code != 429:
            r.raise_for_status()
            return r.json().get('data')

        wait = min(60, (2 ** attempt)) + random.uniform(0.2, 1.0)
        print({'status': 'rate_limited', 'path': path, 'attempt': attempt + 1, 'wait_s': round(wait, 2)})
        time.sleep(wait)

    r.raise_for_status()

def pd_get(path, **params):
    return _pd_request('GET', path, params=params)

def pd_post(path, payload):
    return _pd_request('POST', path, json=payload)

def pd_put(path, payload):
    return _pd_request('PUT', path, json=payload)

def fetch_opp(cnpj):
    r = session.get(
        f'https://app.oportunidados.com.br/api/v1/brazilian_companies/{cnpj}/company',
        headers={'Authorization': f'Bearer {OPP_TOKEN}'},
        timeout=30
    )
    if r.status_code != 200:
        return None
    try:
        return r.json()
    except Exception:
        return None

def get_all_p2():
    out, start = [], 0
    while True:
        payload = _pd_request('GET', 'deals', params={
            'status': 'open',
            'start': start,
            'limit': 100
        })

        batch = payload or []
        if not batch:
            break

        out.extend(batch)

        # precisa pegar pagination completo
        raw = session.get(
            'https://api.pipedrive.com/v1/deals',
            params={'api_token': PD_TOKEN, 'status': 'open', 'start': start, 'limit': 1},
            timeout=30
        ).json()

        pag = (raw.get('additional_data') or {}).get('pagination') or {}
        if not pag.get('more_items_in_collection', False):
            break

        start += 100
        time.sleep(0.3)

    return [d for d in out if int(d.get('pipeline_id') or 0) == PIPELINE_ID]

def person_emails(person):
    vals = []
    for x in (person.get('email') or []):
        if isinstance(x, dict) and x.get('value'):
            vals.append(str(x['value']).strip().lower())
    return vals

def person_phones(person):
    vals = []
    for x in (person.get('phone') or []):
        if isinstance(x, dict) and x.get('value'):
            vals.append(digits(x['value']))
    return vals

def person_has_good_contact(person_id):
    if not person_id:
        return False
    p = pd_get(f'persons/{person_id}') or {}
    name = p.get('name') or ''
    emails = person_emails(p)
    phones = person_phones(p)

    if is_bad_name(name):
        return False
    if any(is_bad_email(e) for e in emails):
        return False
    if any(is_bad_phone(ph) for ph in phones):
        return False

    return bool([e for e in emails if not is_bad_email(e)] or [ph for ph in phones if not is_bad_phone(ph)])

def current_person_is_contaminated(person_id):
    if not person_id:
        return False
    p = pd_get(f'persons/{person_id}') or {}
    name = p.get('name') or ''
    emails = person_emails(p)
    phones = person_phones(p)

    if is_bad_name(name):
        return True
    if any(is_bad_email(e) for e in emails):
        return True
    if any(is_bad_phone(ph) for ph in phones):
        return True
    return False

def get_org_persons(org_id):
    try:
        rows = pd_get(f'organizations/{org_id}/persons') or []
        return rows if isinstance(rows, list) else []
    except Exception:
        return []

def find_reusable_person(org_id, target_name, email, phone):
    rows = get_org_persons(org_id)
    target_name_n = norm(target_name)
    email_n = str(email or '').strip().lower()
    phone_n = digits(phone)

    for p in rows:
        pid = p.get('id')
        if not pid:
            continue
        full = pd_get(f'persons/{pid}') or {}
        full_name = full.get('name') or ''
        name_n = norm(full_name)
        emails = person_emails(full)
        phones = person_phones(full)

        # nunca reaproveita contato contaminado / empresarial
        if is_bad_name(full_name) or definitely_company_name(full_name):
            continue

        if name_n and name_n == target_name_n:
            return full

        if email_n and email_n in emails and not is_bad_email(email_n):
            return full

        if phone_n and phone_n in phones and not is_bad_phone(phone_n):
            return full
    return None

def pick_person_name(company, socios, org_name):
    # prioriza sócio limpo
    for socio in socios or []:
        nome = (socio.get('nome_socio') or socio.get('nome') or '').strip()
        if nome and not is_bad_name(nome):
            return title_case(nome)

    # sem sócio limpo => NÃO inventa contato empresarial
    return ''

def coherent_company(org_name, opp_company):
    opp_rs = norm(opp_company.get('razao_social') or '')
    opp_nf = norm(opp_company.get('nome_fantasia') or '')
    org_n = norm(org_name)

    for cand in [opp_rs, opp_nf]:
        if cand and (org_n in cand or cand in org_n):
            return True
    return False

deals = get_all_p2()
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
        'org_renamed': False,
        'deal_renamed': False,
    }

    # mantém contato já bom
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

    if not coherent_company(org_name, company):
        item['reason'] = 'empresa_api_inconsistente'
        report.append(item)
        continue

    # limpa vínculo contaminado antes
    if old_person_id and current_person_is_contaminated(old_person_id):
        try:
            pd_put(f'deals/{deal_id}', {'person_id': None})
            time.sleep(0.12)
        except Exception:
            pass

    razao = title_case(company.get('razao_social') or '')
    fantasia = title_case(company.get('nome_fantasia') or '')
    email = (company.get('email') or '').strip().lower()
    phone = digits(f"{company.get('ddd_1') or ''}{company.get('telefone_1') or ''}")

    if is_bad_email(email):
        email = ''
    if is_bad_phone(phone):
        phone = ''

    person_name = pick_person_name(company, socios, org_name)

    # BLOQUEIO FINAL: se não tem pessoa física REAL → não faz nada
    if not person_name:
        item['reason'] = 'sem_pessoa_fisica_na_api'
        report.append(item)
        continue

    if is_bad_name(person_name) or definitely_company_name(person_name):
        item['reason'] = 'nome_empresarial_bloqueado'
        report.append(item)
        continue

    # garantia extra: precisa ter pelo menos nome e sobrenome
    if len([x for x in person_name.split() if x]) < 2:
        item['reason'] = 'nome_invalido_bloqueado'
        report.append(item)
        continue

    # dedupe por org
    reusable = find_reusable_person(org_id, person_name, email, phone)
    if reusable:
        new_person_id = reusable.get('id')
        pd_put(f'deals/{deal_id}', {'person_id': new_person_id})
        item['status'] = 'reused'
        item['new_person_id'] = new_person_id
        item['new_person_name'] = reusable.get('name')
    else:
        payload = {'name': person_name, 'org_id': org_id}
        if email:
            payload['email'] = [{'value': email, 'primary': True}]
        if phone:
            payload['phone'] = [{'value': phone, 'primary': True}]

        created = pd_post('persons', payload)
        new_person_id = created.get('id')

        pd_put(f'deals/{deal_id}', {'person_id': new_person_id})
        item['status'] = 'filled'
        item['new_person_id'] = new_person_id
        item['new_person_name'] = person_name

        try:
            parts = pd_get(f'deals/{deal_id}/participants') or []
            if len(parts) == 0:
                pd_post(f'deals/{deal_id}/participants', {'person_id': new_person_id})
        except Exception:
            pass

    # padroniza org e deal só com razão social coerente
    if razao and norm(org_data.get('name') or '') != norm(razao):
        try:
            pd_put(f'organizations/{org_id}', {'name': razao})
            item['org_renamed'] = True
        except Exception:
            pass

    desired_title = f'{razao} - Deal' if razao else title
    if desired_title and norm(title) != norm(desired_title):
        try:
            pd_put(f'deals/{deal_id}', {'title': desired_title})
            item['deal_renamed'] = True
        except Exception:
            pass

    report.append(item)
    print(item)
    time.sleep(0.25)

OUT.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
print('OUT =', OUT)


# ===== PATCH FINAL HARDENING =====
def definitely_company_name(name):
    n = norm(name)
    if not n:
        return False

    hard_terms = [
        'LTDA', 'S/A', ' SA', 'EIRELI', 'HOLDING', 'INDUSTRIA', 'INDÚSTRIA',
        'COMERCIO', 'COMÉRCIO', 'DISTRIBUIDORA', 'CONDOMINIO', 'CONDOMÍNIO',
        'ASSOCIACAO', 'ASSOCIAÇÃO', 'FUNDO', 'SHOPPING', 'EMPRESARIAL',
        'CIA', 'COMPANHIA', 'COOPERATIVA', 'PARTICIPACOES', 'PARTICIPAÇÕES',
        'INC.', ' INC', 'LLC', 'CORP', 'INTERNATIONAL', 'GROUP', 'GRUPO',
        'S.A.', 'S. A.', 'B.V.', ' B.V', 'BV', 'R.L.', 'R L', 'S.A R.L.', 'S A R L'
    ]
    if any(t in f' {n} ' for t in hard_terms):
        return True

    if re.search(r'\b(?:[A-Z]\.){2,}', n):
        return True
    if re.search(r'\bS\.?\s*A\.?\s*R\.?\s*L\.?\b', n):
        return True

    return False


def find_reusable_person(org_id, target_name, email, phone):
    rows = get_org_persons(org_id)
    target_name_n = norm(target_name)
    email_n = str(email or '').strip().lower()
    phone_n = digits(phone)

    for p in rows:
        pid = p.get('id')
        if not pid:
            continue

        full = pd_get(f'persons/{pid}') or {}
        full_name = full.get('name') or ''
        name_n = norm(full_name)
        emails = person_emails(full)
        phones = person_phones(full)

        if is_bad_name(full_name) or definitely_company_name(full_name):
            continue

        if len([x for x in full_name.strip().split() if x]) < 2:
            continue

        if name_n and name_n == target_name_n:
            return full

        if email_n and email_n in emails and not is_bad_email(email_n):
            return full

        if phone_n and phone_n in phones and not is_bad_phone(phone_n):
            return full

    return None


def pick_person_name(company, socios, org_name):
    for socio in socios or []:
        nome = (socio.get('nome_socio') or socio.get('nome') or '').strip()
        if nome and not is_bad_name(nome) and not definitely_company_name(nome):
            if len([x for x in nome.split() if x]) >= 2:
                return title_case(nome)

    return ''
# ===== END PATCH FINAL HARDENING =====
