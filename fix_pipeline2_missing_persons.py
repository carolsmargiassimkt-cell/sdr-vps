import json
import re
import unicodedata
import requests
from pathlib import Path

CFG = Path('/root/sdr-vps/config/system_config.json')
STATE_JSON = Path('/root/sdr-vps/runtime/fix_pipeline2_missing_persons_state.json')
RESULT_JSON = Path('/root/sdr-vps/runtime/fix_pipeline2_missing_persons_result.json')

PIPELINE_ID = 2

def load_token():
    cfg = json.load(open(CFG))
    return cfg.get('pipedrive_token') or cfg.get('pipedrive_api_token') or ''

def extract_id(v):
    if isinstance(v, dict):
        try:
            return int(v.get('value') or v.get('id') or 0)
        except:
            return 0
    try:
        return int(v or 0)
    except:
        return 0

def normalize_email(v):
    s = str(v or '').strip().lower()
    return s if '@' in s else ''

def normalize_phone(v):
    d = re.sub(r'\D+', '', str(v or ''))
    if d.startswith('55') and len(d) > 11:
        d = d[2:]
    if len(d) not in {10, 11}:
        return ''
    return d

def load_state():
    if not STATE_JSON.exists():
        return {'done': []}
    return json.load(open(STATE_JSON))

def save_state(state):
    STATE_JSON.parent.mkdir(parents=True, exist_ok=True)
    STATE_JSON.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding='utf-8')

def api_get_all_open_deals(token):
    all_deals = []
    start = 0
    limit = 500
    while True:
        r = requests.get(
            'https://api.pipedrive.com/v1/deals',
            params={
                'api_token': token,
                'start': start,
                'limit': limit,
                'status': 'open',
            },
            timeout=60,
        )
        r.raise_for_status()
        payload = r.json()
        batch = payload.get('data') or []
        all_deals.extend(batch)
        pag = payload.get('additional_data', {}).get('pagination', {})
        if not pag.get('more_items_in_collection', False) or not batch:
            break
        start += limit
    return all_deals

def api_get_deal_participants(token, deal_id):
    r = requests.get(
        f'https://api.pipedrive.com/v1/deals/{deal_id}/participants',
        params={'api_token': token},
        timeout=60,
    )
    if r.status_code >= 400:
        return []
    payload = r.json()
    return payload.get('data') or []

def api_get_person(token, person_id):
    r = requests.get(
        f'https://api.pipedrive.com/v1/persons/{person_id}',
        params={'api_token': token},
        timeout=60,
    )
    if r.status_code >= 400:
        return {}
    payload = r.json()
    return payload.get('data') or {}

def person_quality(person):
    emails = []
    phones = []

    raw_email = person.get('email') or []
    if isinstance(raw_email, list):
        for item in raw_email:
            if isinstance(item, dict):
                e = normalize_email(item.get('value'))
            else:
                e = normalize_email(item)
            if e:
                emails.append(e)

    raw_phone = person.get('phone') or []
    if isinstance(raw_phone, list):
        for item in raw_phone:
            if isinstance(item, dict):
                p = normalize_phone(item.get('value'))
            else:
                p = normalize_phone(item)
            if p:
                phones.append(p)

    return {
        'has_email': bool(emails),
        'has_phone': bool(phones),
        'email_count': len(emails),
        'phone_count': len(phones),
    }

def choose_best_participant(token, participants):
    enriched = []
    for p in participants:
        person_id = extract_id(p.get('person_id') or p.get('person'))
        if not person_id:
            continue
        person = api_get_person(token, person_id)
        q = person_quality(person)
        enriched.append({
            'person_id': person_id,
            'name': person.get('name'),
            'quality': q,
        })

    if not enriched:
        return None, []

    enriched.sort(
        key=lambda x: (
            x['quality']['has_email'],
            x['quality']['has_phone'],
            x['quality']['email_count'],
            x['quality']['phone_count'],
            -int(x['person_id']),
        ),
        reverse=True
    )
    return enriched[0], enriched

def api_update_deal_person(token, deal_id, person_id):
    r = requests.put(
        f'https://api.pipedrive.com/v1/deals/{deal_id}',
        params={'api_token': token},
        json={'person_id': person_id},
        timeout=60,
    )
    return r.status_code, r.text

token = load_token()
state = load_state()
done = set(state.get('done', []))

all_deals = api_get_all_open_deals(token)
p2 = [d for d in all_deals if int(d.get('pipeline_id') or 0) == PIPELINE_ID]

missing = [d for d in p2 if not extract_id(d.get('person_id'))]

results = []

print(f'[START] missing_person_deals={len(missing)}', flush=True)

for d in missing:
    deal_id = int(d.get('id') or 0)
    title = str(d.get('title') or '')
    key = str(deal_id)

    if key in done:
        continue

    participants = api_get_deal_participants(token, deal_id)

    row = {
        'deal_id': deal_id,
        'title': title,
        'participants_count': len(participants),
        'status': '',
    }

    if not participants:
        row['status'] = 'needs_review'
        row['reason'] = 'no_participants'
        print(f'[REVIEW] deal={deal_id} no_participants', flush=True)

    elif len(participants) == 1:
        person_id = extract_id(participants[0].get('person_id') or participants[0].get('person'))
        if person_id:
            status_code, body = api_update_deal_person(token, deal_id, person_id)
            if 200 <= status_code < 300:
                row['status'] = 'fixed_single_participant'
                row['person_id'] = person_id
                print(f'[FIXED_SINGLE] deal={deal_id} person={person_id}', flush=True)
            else:
                row['status'] = 'error'
                row['person_id'] = person_id
                row['http_status'] = status_code
                row['body'] = body[:300]
                print(f'[ERROR] deal={deal_id} person={person_id} status={status_code}', flush=True)
        else:
            row['status'] = 'needs_review'
            row['reason'] = 'single_participant_without_person_id'
            print(f'[REVIEW] deal={deal_id} single_participant_without_person_id', flush=True)

    else:
        best, enriched = choose_best_participant(token, participants)
        row['candidate_person_ids'] = [x['person_id'] for x in enriched[:10]]

        if best and (best['quality']['has_email'] or best['quality']['has_phone']):
            person_id = best['person_id']
            status_code, body = api_update_deal_person(token, deal_id, person_id)
            if 200 <= status_code < 300:
                row['status'] = 'fixed_best_participant'
                row['person_id'] = person_id
                row['reason'] = 'best_quality_participant'
                print(f'[FIXED_BEST] deal={deal_id} person={person_id}', flush=True)
            else:
                row['status'] = 'error'
                row['person_id'] = person_id
                row['http_status'] = status_code
                row['body'] = body[:300]
                print(f'[ERROR] deal={deal_id} person={person_id} status={status_code}', flush=True)
        else:
            row['status'] = 'needs_review'
            row['reason'] = 'multiple_participants_no_good_candidate'
            print(f'[REVIEW] deal={deal_id} multiple_participants_no_good_candidate', flush=True)

    results.append(row)
    done.add(key)
    save_state({'done': sorted(done)})

RESULT_JSON.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding='utf-8')
print(f'[RESULT_JSON] {RESULT_JSON}', flush=True)
