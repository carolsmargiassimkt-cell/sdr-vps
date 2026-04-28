import json, requests, re, time
from pathlib import Path

CFG = Path('/root/sdr-vps/config/system_config.json')
OUT = Path('/root/sdr-vps/runtime/find_global_bad_participants.json')
ERR = Path('/root/sdr-vps/runtime/find_global_bad_participants_errors.json')

cfg = json.load(open(CFG))
TOKEN = cfg.get('pipedrive_token') or cfg.get('pipedrive_api_token') or ''

PIPELINE_ID = 2

BAD_EMAILS = {'andrea@sherwin.com.br'}
BAD_PHONES = {'1121375000'}
BAD_ORG_TERMS = ['FARMACIA INDIANA']
BAD_NAME_TERMS = ['RESPONSAVEL', 'RESPONSÁVEL']

def norm(s):
    return str(s or '').upper().strip()

def digits(s):
    return re.sub(r'\D+', '', str(s or ''))

def get_json(url, params=None, tries=4, sleep_sec=1.5):
    last_err = None
    for attempt in range(1, tries + 1):
        try:
            r = requests.get(url, params=params, timeout=30)
            if r.status_code in (429, 500, 502, 503, 504):
                last_err = f"status={r.status_code}"
                print(f"retry {attempt}/{tries} url={url} {last_err}")
                time.sleep(sleep_sec * attempt)
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = str(e)
            print(f"retry {attempt}/{tries} url={url} err={e}")
            time.sleep(sleep_sec * attempt)
    raise RuntimeError(last_err)

def get_all_open_p2():
    out, start = [], 0
    while True:
        payload = get_json(
            'https://api.pipedrive.com/v1/deals',
            params={'api_token': TOKEN, 'status': 'open', 'start': start, 'limit': 500},
        )
        batch = payload.get('data') or []
        out.extend(batch)
        pag = payload.get('additional_data', {}).get('pagination', {})
        print("pagina deals:", start, "qtd:", len(batch), "total acumulado:", len(out))
        if not pag.get('more_items_in_collection', False) or not batch:
            break
        start += 500
    return [d for d in out if int(d.get('pipeline_id') or 0) == PIPELINE_ID]

person_cache = {}
errors = []

def get_person(person_id):
    if not person_id:
        return {}
    if person_id in person_cache:
        return person_cache[person_id]
    try:
        payload = get_json(
            f'https://api.pipedrive.com/v1/persons/{person_id}',
            params={'api_token': TOKEN},
        )
        d = payload.get('data') or {}
    except Exception as e:
        errors.append({'type': 'person', 'person_id': person_id, 'error': str(e)})
        d = {}
    person_cache[person_id] = d
    return d

deals = get_all_open_p2()
print("TOTAL DEALS P2 =", len(deals))

rows = []

for i, deal in enumerate(deals, 1):
    deal_id = int(deal.get('id') or 0)
    title = deal.get('title') or ''
    org_name = deal.get('org_name') or ''

    print(f"Processando deal {i}/{len(deals)} id={deal_id} title={title}")

    try:
        payload = get_json(
            f'https://api.pipedrive.com/v1/deals/{deal_id}/participants',
            params={'api_token': TOKEN},
        )
        parts = payload.get('data') or []
    except Exception as e:
        errors.append({'type': 'participants', 'deal_id': deal_id, 'title': title, 'error': str(e)})
        print(f"ERRO deal={deal_id} participants: {e}")
        continue

    for p in parts:
        dpid = p.get('id')
        pid = p.get('person_id') or {}
        if isinstance(pid, dict):
            person_id = pid.get('value')
            person_name = pid.get('name') or ''
        else:
            person_id = pid
            person_name = ''

        person = get_person(person_id)
        p_org = person.get('org_name') or ''
        emails = {str(x.get('value') or '').strip().lower() for x in (person.get('email') or []) if isinstance(x, dict)}
        phones = {digits(x.get('value')) for x in (person.get('phone') or []) if isinstance(x, dict) and x.get('value')}

        bad = False
        reasons = []

        if emails & BAD_EMAILS:
            bad = True
            reasons.append('bad_email')
        if phones & BAD_PHONES:
            bad = True
            reasons.append('bad_phone')
        if any(term in norm(p_org) for term in BAD_ORG_TERMS):
            bad = True
            reasons.append('bad_org')
        if any(term in norm(person_name) for term in BAD_NAME_TERMS) and ((emails & BAD_EMAILS) or (phones & BAD_PHONES)):
            bad = True
            reasons.append('bad_name_plus_contact')

        if bad:
            row = {
                'deal_id': deal_id,
                'deal_title': title,
                'deal_org_name': org_name,
                'deal_participant_id': dpid,
                'person_id': person_id,
                'person_name': person_name,
                'person_org_name': p_org,
                'emails': sorted(emails),
                'phones': sorted(phones),
                'reasons': reasons,
            }
            rows.append(row)
            print("BAD:", row)

OUT.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding='utf-8')
ERR.write_text(json.dumps(errors, ensure_ascii=False, indent=2), encoding='utf-8')

print('TOTAL_BAD_LINKS =', len(rows))
print('TOTAL_ERRORS =', len(errors))
print('OUT =', OUT)
print('ERR =', ERR)
