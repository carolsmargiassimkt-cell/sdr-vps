import json
import re
import requests
from pathlib import Path

CFG = Path('/root/sdr-vps/config/system_config.json')
TARGETS_JSON = Path('/root/sdr-vps/runtime/oportunidades_targets_with_cnpj.json')
STATE_JSON = Path('/root/sdr-vps/runtime/enrich_8_with_oportunidados_state.json')
RESULT_JSON = Path('/root/sdr-vps/runtime/enrich_8_with_oportunidados_result.json')

OPORTUNIDADOS_TOKEN = "6a15950c356566f697425b11567f391e5993e544d3030bc836e41fd07a2599dd"
OPORTUNIDADOS_BASE = "https://app.oportunidados.com.br"
PIPELINE_ID = 2

def load_pipedrive_token():
    cfg = json.load(open(CFG))
    return cfg.get('pipedrive_token') or cfg.get('pipedrive_api_token') or ''

def norm_phone(v):
    d = re.sub(r'\D+', '', str(v or ''))
    if d.startswith('55') and len(d) > 11:
        d = d[2:]
    if len(d) not in {10, 11}:
        return ''
    return d

def norm_email(v):
    s = str(v or '').strip().lower()
    return s if '@' in s else ''

def dedupe(vals):
    out, seen = [], set()
    for v in vals:
        if v and v not in seen:
            seen.add(v)
            out.append(v)
    return out

def load_state():
    if not STATE_JSON.exists():
        return {'done': []}
    return json.load(open(STATE_JSON))

def save_state(state):
    STATE_JSON.parent.mkdir(parents=True, exist_ok=True)
    STATE_JSON.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding='utf-8')

def api_get_deal(token, deal_id):
    r = requests.get(
        f'https://api.pipedrive.com/v1/deals/{deal_id}',
        params={'api_token': token},
        timeout=60,
    )
    if r.status_code >= 400:
        return {}
    return (r.json().get('data') or {})

def api_get_org(token, org_id):
    r = requests.get(
        f'https://api.pipedrive.com/v1/organizations/{org_id}',
        params={'api_token': token},
        timeout=60,
    )
    if r.status_code >= 400:
        return {}
    return (r.json().get('data') or {})

def api_get_person(token, person_id):
    r = requests.get(
        f'https://api.pipedrive.com/v1/persons/{person_id}',
        params={'api_token': token},
        timeout=60,
    )
    if r.status_code >= 400:
        return {}
    return (r.json().get('data') or {})

def api_create_person(token, payload):
    r = requests.post(
        'https://api.pipedrive.com/v1/persons',
        params={'api_token': token},
        json=payload,
        timeout=60,
    )
    data = {}
    try:
        data = r.json().get('data') or {}
    except Exception:
        pass
    return r.status_code, r.text, data

def api_update_person(token, person_id, payload):
    r = requests.put(
        f'https://api.pipedrive.com/v1/persons/{person_id}',
        params={'api_token': token},
        json=payload,
        timeout=60,
    )
    return r.status_code, r.text

def api_update_deal_person(token, deal_id, person_id):
    r = requests.put(
        f'https://api.pipedrive.com/v1/deals/{deal_id}',
        params={'api_token': token},
        json={'person_id': person_id},
        timeout=60,
    )
    return r.status_code, r.text

def fetch_oportunidados(cnpj):
    r = requests.get(
        f'{OPORTUNIDADOS_BASE}/api/v1/brazilian_companies/{cnpj}/company',
        headers={'Authorization': f'Bearer {OPORTUNIDADOS_TOKEN}'},
        timeout=60,
    )
    payload = {}
    try:
        payload = r.json()
    except Exception:
        pass
    return r.status_code, payload

def collect_from_oportunidados(payload):
    phones = []
    emails = []
    person_name = ''

    # tenta vários formatos possíveis
    data = payload.get('data') if isinstance(payload, dict) else None
    root = data if isinstance(data, dict) else payload if isinstance(payload, dict) else {}

    # empresa / razão
    company_name = (
        root.get('company_name')
        or root.get('razao_social')
        or root.get('name')
        or ''
    )

    # contatos possíveis
    for key in ['email', 'company_email']:
        e = norm_email(root.get(key))
        if e:
            emails.append(e)

    for key in ['phone', 'company_phone', 'phone_number', 'telephone']:
        p = norm_phone(root.get(key))
        if p:
            phones.append(p)

    # listas
    for item in root.get('emails', []) if isinstance(root.get('emails'), list) else []:
        e = norm_email(item.get('email') if isinstance(item, dict) else item)
        if e:
            emails.append(e)

    for item in root.get('phones', []) if isinstance(root.get('phones'), list) else []:
        p = norm_phone(item.get('number') if isinstance(item, dict) else item)
        if p:
            phones.append(p)

    # decisores / contatos
    contact_lists = []
    for key in ['contacts', 'people', 'decision_makers', 'owners', 'partners']:
        val = root.get(key)
        if isinstance(val, list):
            contact_lists.extend(val)

    chosen_contact = None
    for c in contact_lists:
        if not isinstance(c, dict):
            continue
        email = norm_email(c.get('email'))
        phone = norm_phone(c.get('phone') or c.get('mobile') or c.get('telephone'))
        name = str(c.get('name') or c.get('full_name') or '').strip()
        if email or phone or name:
            chosen_contact = c
            break

    if chosen_contact:
        person_name = str(chosen_contact.get('name') or chosen_contact.get('full_name') or '').strip()
        e = norm_email(chosen_contact.get('email'))
        p = norm_phone(chosen_contact.get('phone') or chosen_contact.get('mobile') or chosen_contact.get('telephone'))
        if e:
            emails.append(e)
        if p:
            phones.append(p)

    return {
        'company_name': company_name,
        'person_name': person_name,
        'emails': dedupe(emails),
        'phones': dedupe(phones),
    }

pipedrive_token = load_pipedrive_token()
targets = json.load(open(TARGETS_JSON))
state = load_state()
done = set(state.get('done', []))
results = []

print(f'[START] targets={len(targets)}', flush=True)

for t in targets:
    deal_id = int(t.get('deal_id') or 0)
    cnpj = str(t.get('cnpj') or '').strip()
    key = str(deal_id)

    if key in done:
        continue

    row = {
        'deal_id': deal_id,
        'company': t.get('company'),
        'cnpj': cnpj,
        'status': '',
    }

    if not deal_id or not cnpj:
        row['status'] = 'needs_review'
        row['reason'] = 'missing_deal_or_cnpj'
        results.append(row)
        done.add(key)
        save_state({'done': sorted(done)})
        continue

    deal = api_get_deal(pipedrive_token, deal_id)
    if not deal:
        row['status'] = 'error'
        row['reason'] = 'deal_not_found'
        results.append(row)
        done.add(key)
        save_state({'done': sorted(done)})
        continue

    org_id = deal.get('org_id')
    if isinstance(org_id, dict):
        org_id = int(org_id.get('value') or org_id.get('id') or 0)
    else:
        org_id = int(org_id or 0)

    if not org_id:
        row['status'] = 'needs_review'
        row['reason'] = 'deal_without_org'
        results.append(row)
        done.add(key)
        save_state({'done': sorted(done)})
        continue

    status_code, payload = fetch_oportunidados(cnpj)
    row['oportunidados_status'] = status_code

    if status_code >= 400 or not payload:
        row['status'] = 'needs_review'
        row['reason'] = 'oportunidados_error'
        results.append(row)
        done.add(key)
        save_state({'done': sorted(done)})
        print(f'[REVIEW] deal={deal_id} oportunidades_error={status_code}', flush=True)
        continue

    extracted = collect_from_oportunidados(payload)
    person_name = extracted['person_name'] or t.get('company') or f'Contato {deal_id}'
    emails = extracted['emails']
    phones = extracted['phones']

    if not emails and not phones:
        row['status'] = 'needs_review'
        row['reason'] = 'oportunidados_without_contact'
        row['person_name'] = person_name
        results.append(row)
        done.add(key)
        save_state({'done': sorted(done)})
        print(f'[REVIEW] deal={deal_id} oportunidades_without_contact', flush=True)
        continue

    current_person_id = deal.get('person_id')
    if isinstance(current_person_id, dict):
        current_person_id = int(current_person_id.get('value') or current_person_id.get('id') or 0)
    else:
        current_person_id = int(current_person_id or 0)

    if current_person_id:
        person_payload = {}
        if phones:
            person_payload['phone'] = [{'value': p, 'primary': i == 0} for i, p in enumerate(phones)]
        if emails:
            person_payload['email'] = [{'value': e, 'primary': i == 0} for i, e in enumerate(emails)]

        status_code2, body2 = api_update_person(pipedrive_token, current_person_id, person_payload)
        if 200 <= status_code2 < 300:
            row['status'] = 'updated_existing_person'
            row['person_id'] = current_person_id
            row['person_name'] = person_name
            row['emails'] = emails
            row['phones'] = phones
            print(f'[UPDATED_EXISTING] deal={deal_id} person={current_person_id}', flush=True)
        else:
            row['status'] = 'error'
            row['reason'] = 'update_existing_person_failed'
            row['http_status'] = status_code2
            results.append(row)
            done.add(key)
            save_state({'done': sorted(done)})
            print(f'[ERROR] deal={deal_id} update_existing_failed={status_code2}', flush=True)
            continue
    else:
        create_payload = {
            'name': person_name,
            'org_id': org_id,
        }
        if phones:
            create_payload['phone'] = [{'value': p, 'primary': i == 0} for i, p in enumerate(phones)]
        if emails:
            create_payload['email'] = [{'value': e, 'primary': i == 0} for i, e in enumerate(emails)]

        status_code3, body3, created = api_create_person(pipedrive_token, create_payload)
        if 200 <= status_code3 < 300 and created.get('id'):
            new_person_id = int(created.get('id') or 0)
            status_code4, body4 = api_update_deal_person(pipedrive_token, deal_id, new_person_id)
            if 200 <= status_code4 < 300:
                row['status'] = 'created_and_linked_person'
                row['person_id'] = new_person_id
                row['person_name'] = person_name
                row['emails'] = emails
                row['phones'] = phones
                print(f'[CREATED_AND_LINKED] deal={deal_id} person={new_person_id}', flush=True)
            else:
                row['status'] = 'error'
                row['reason'] = 'link_new_person_failed'
                row['http_status'] = status_code4
                results.append(row)
                done.add(key)
                save_state({'done': sorted(done)})
                print(f'[ERROR] deal={deal_id} link_new_failed={status_code4}', flush=True)
                continue
        else:
            row['status'] = 'error'
            row['reason'] = 'create_person_failed'
            row['http_status'] = status_code3
            results.append(row)
            done.add(key)
            save_state({'done': sorted(done)})
            print(f'[ERROR] deal={deal_id} create_person_failed={status_code3}', flush=True)
            continue

    results.append(row)
    done.add(key)
    save_state({'done': sorted(done)})

RESULT_JSON.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding='utf-8')
print(f'[RESULT_JSON] {RESULT_JSON}', flush=True)
