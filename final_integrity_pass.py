import json
import re
import unicodedata
import requests
import time
import zipfile
import xml.etree.ElementTree as ET
from collections import defaultdict, Counter
from pathlib import Path

CFG = Path('/root/sdr-vps/config/system_config.json')
STATE_JSON = Path('/root/sdr-vps/runtime/final_integrity_pass_state.json')
RESULT_JSON = Path('/root/sdr-vps/runtime/final_integrity_pass_result.json')
SUMMARY_JSON = Path('/root/sdr-vps/runtime/final_integrity_pass_summary.json')
XLSX = Path('/root/BASE_LEADS_COM_LINKEDIN.xlsx')

PIPELINE_ID = 2
ENTRY_STAGE_ID = 8
LOST_STAGE_ID = 50
BATCH_LIMIT = 10
BATCH_DELAY_SEC = 5
APPLY = True

NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
JUNK_TITLES = {'NAN', 'NONE', 'NEGOCIO', 'FUNDO'}

def log(msg):
    print(msg, flush=True)

def load_token():
    cfg = json.load(open(CFG))
    return cfg.get('pipedrive_token') or cfg.get('pipedrive_api_token') or ''

def strip_accents(v):
    s = str(v or '').strip()
    s = unicodedata.normalize('NFKD', s)
    return ''.join(ch for ch in s if not unicodedata.combining(ch))

def norm(v):
    s = strip_accents(v).upper()
    s = re.sub(r'\s*-\s*LEAD\s*$', '', s, flags=re.I)
    s = re.sub(r'\s*-\s*DEAL\s*$', '', s, flags=re.I)
    s = re.sub(r'\s*-\s*SORTEIO/VB\s*$', '', s, flags=re.I)
    s = re.sub(r'\s*LEADSTER\s*$', '', s, flags=re.I)
    s = re.sub(r'\s+', ' ', s).strip(' -')
    return s

def norm_key(v):
    return re.sub(r'[^A-Z0-9]+', '', norm(v))

def norm_cnpj(v):
    d = re.sub(r'\D+', '', str(v or ''))
    return d if len(d) == 14 else ''

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

def load_state():
    if not STATE_JSON.exists():
        return {'done': []}
    return json.load(open(STATE_JSON))

def save_state(state):
    STATE_JSON.parent.mkdir(parents=True, exist_ok=True)
    STATE_JSON.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding='utf-8')

def api_get_all(endpoint, token, params_extra=None):
    out = []
    start = 0
    while True:
        params = {'api_token': token, 'start': start, 'limit': 500}
        if params_extra:
            params.update(params_extra)
        r = requests.get(f'https://api.pipedrive.com/v1/{endpoint}', params=params, timeout=60)
        r.raise_for_status()
        payload = r.json()
        batch = payload.get('data') or []
        out.extend(batch)
        pag = payload.get('additional_data', {}).get('pagination', {})
        if not pag.get('more_items_in_collection', False) or not batch:
            break
        start += 500
    return out

def api_get_org(token, org_id):
    r = requests.get(f'https://api.pipedrive.com/v1/organizations/{org_id}', params={'api_token': token}, timeout=60)
    if r.status_code >= 400:
        return {}
    return r.json().get('data') or {}

def api_get_person(token, person_id):
    r = requests.get(f'https://api.pipedrive.com/v1/persons/{person_id}', params={'api_token': token}, timeout=60)
    if r.status_code >= 400:
        return {}
    return r.json().get('data') or {}

def api_get_deal_participants(token, deal_id):
    r = requests.get(f'https://api.pipedrive.com/v1/deals/{deal_id}/participants', params={'api_token': token}, timeout=60)
    if r.status_code >= 400:
        return []
    return r.json().get('data') or []

def api_search_orgs(token, term):
    r = requests.get(
        'https://api.pipedrive.com/v1/organizations/search',
        params={'api_token': token, 'term': term, 'fields': 'name', 'limit': 20, 'exact_match': False},
        timeout=60
    )
    if r.status_code >= 400:
        return []
    return (r.json().get('data') or {}).get('items') or []

def api_update_deal(token, deal_id, payload):
    if not APPLY:
        return 200, 'dry-run'
    r = requests.put(
        f'https://api.pipedrive.com/v1/deals/{deal_id}',
        params={'api_token': token},
        json=payload,
        timeout=60
    )
    return r.status_code, r.text

def parse_xlsx(path):
    with zipfile.ZipFile(path) as z:
        root = ET.fromstring(z.read('xl/worksheets/sheet1.xml'))
    rows_raw = []
    for row in root.iter(NS + 'row'):
        vals = []
        for c in row:
            t = c.find('.//' + NS + 't')
            v = c.find('.//' + NS + 'v')
            vals.append(t.text if t is not None else (v.text if v is not None else ''))
        if vals:
            rows_raw.append(vals)
    header = rows_raw[0]
    out = []
    for vals in rows_raw[1:]:
        if len(vals) < len(header):
            vals += [''] * (len(header) - len(vals))
        out.append(dict(zip(header, vals)))
    return out

def load_sheet_signals():
    rows = parse_xlsx(XLSX)
    keys = set()
    for r in rows:
        keys.add(norm_key(r.get('Organização - Nome')))
        keys.add(norm_key(r.get('Negócio - Título')))
        c = norm_cnpj(r.get('cnpj_formatado'))
        if c:
            keys.add(c)
    return keys

def load_matched_deal_ids():
    files = [
        '/root/sdr-vps/runtime/final_reconcile_342.json',
        '/root/sdr-vps/runtime/final_reconcile_remaining_200_matched.json',
        '/root/sdr-vps/runtime/final_reconcile_remaining_95_super_matched.json',
    ]
    ids = set()
    for f in files:
        p = Path(f)
        if not p.exists():
            continue
        try:
            data = json.load(open(p))
            for x in data:
                did = int(x.get('deal_id') or 0)
                if did:
                    ids.add(did)
        except:
            pass
    return ids

def participant_candidates(token, deal_id, deal_org_id):
    participants = api_get_deal_participants(token, deal_id)
    candidates = []
    for p in participants:
        pid = extract_id(p.get('person_id') or p.get('person'))
        if not pid:
            continue
        person = api_get_person(token, pid)
        p_org = extract_id(person.get('org_id'))
        emails = person.get('email') or []
        phones = person.get('phone') or []
        has_email = any(isinstance(e, dict) and e.get('value') for e in emails)
        has_phone = any(isinstance(ph, dict) and ph.get('value') for ph in phones)
        candidates.append({
            'person_id': pid,
            'person_org_id': p_org,
            'has_email': has_email,
            'has_phone': has_phone,
            'score': (
                int(p_org == deal_org_id and deal_org_id != 0),
                int(has_email),
                int(has_phone),
                -pid
            )
        })
    candidates.sort(key=lambda x: x['score'], reverse=True)
    return candidates

token = load_token()
state = load_state()
done = set(state.get('done', []))

log('[LOAD] deals/persons...')
all_deals = api_get_all('deals', token, {'status': 'all_not_deleted'})
all_persons = api_get_all('persons', token)
open_deals = [d for d in all_deals if int(d.get('pipeline_id') or 0) == PIPELINE_ID and int(d.get('stage_id') or 0) != LOST_STAGE_ID]
lost_deals = [d for d in all_deals if int(d.get('pipeline_id') or 0) == PIPELINE_ID and int(d.get('stage_id') or 0) == LOST_STAGE_ID]

sheet_signals = load_sheet_signals()
matched_deal_ids = load_matched_deal_ids()

person_org_cache = {int(p.get('id') or 0): extract_id(p.get('org_id')) for p in all_persons}

results = []
counts = Counter()
processed = 0

# indexes
open_by_title_org = defaultdict(list)
open_by_org = defaultdict(list)
for d in open_deals:
    title_base = norm(d.get('title'))
    org_id = extract_id(d.get('org_id'))
    open_by_title_org[(title_base, org_id)].append(d)
    if org_id:
        open_by_org[org_id].append(d)

# 1) missing org
missing_org = [d for d in open_deals if not extract_id(d.get('org_id'))]
log(f'[PHASE1] missing_org={len(missing_org)}')
for d in missing_org:
    deal_id = int(d.get('id') or 0)
    key = f'missing_org|{deal_id}'
    if key in done:
        continue

    title = d.get('title') or ''
    candidates = []
    for term in [title, norm(title)]:
        items = api_search_orgs(token, term)
        for item in items:
            org = item.get('item') or {}
            oid = int(org.get('id') or 0)
            name = org.get('name') or ''
            if oid:
                candidates.append((oid, name))
    uniq = {}
    for oid, name in candidates:
        uniq[oid] = name
    exact = [oid for oid, name in uniq.items() if norm_key(name) == norm_key(title)]

    row = {'phase': 'missing_org', 'deal_id': deal_id, 'title': title}
    if len(exact) == 1:
        sc, body = api_update_deal(token, deal_id, {'org_id': exact[0]})
        row['status'] = 'fixed' if 200 <= sc < 300 else 'error'
        row['org_id'] = exact[0]
        log(f"[MISSING_ORG_{row['status'].upper()}] deal={deal_id} org={exact[0]}")
    else:
        row['status'] = 'needs_review'
        row['reason'] = 'no_unique_exact_org'
    results.append(row); done.add(key); save_state({'done': sorted(done)}); counts[f"{row['phase']}:{row['status']}"] += 1
    processed += 1
    if processed >= BATCH_LIMIT:
        log(f'[PAUSA] {BATCH_DELAY_SEC}s'); time.sleep(BATCH_DELAY_SEC); processed = 0

# refresh open_deals after missing org fixes
all_deals = api_get_all('deals', token, {'status': 'all_not_deleted'})
open_deals = [d for d in all_deals if int(d.get('pipeline_id') or 0) == PIPELINE_ID and int(d.get('stage_id') or 0) != LOST_STAGE_ID]
lost_deals = [d for d in all_deals if int(d.get('pipeline_id') or 0) == PIPELINE_ID and int(d.get('stage_id') or 0) == LOST_STAGE_ID]

# 2) missing person + person/org mismatch
targets = []
for d in open_deals:
    deal_id = int(d.get('id') or 0)
    org_id = extract_id(d.get('org_id'))
    person_id = extract_id(d.get('person_id'))
    if not person_id:
        targets.append(('missing_person', d))
    else:
        p_org = person_org_cache.get(person_id, 0)
        if p_org and org_id and p_org != org_id:
            targets.append(('person_mismatch', d))

log(f'[PHASE2] person_targets={len(targets)}')
for phase, d in targets:
    deal_id = int(d.get('id') or 0)
    key = f'{phase}|{deal_id}'
    if key in done:
        continue

    org_id = extract_id(d.get('org_id'))
    person_id = extract_id(d.get('person_id'))
    title = d.get('title') or ''
    cands = participant_candidates(token, deal_id, org_id)

    row = {'phase': phase, 'deal_id': deal_id, 'title': title, 'org_id': org_id, 'current_person_id': person_id}
    chosen = None

    if len(cands) == 1:
        chosen = cands[0]
    else:
        matching_org = [c for c in cands if c['person_org_id'] == org_id and org_id != 0]
        if len(matching_org) == 1:
            chosen = matching_org[0]

    if chosen:
        sc, body = api_update_deal(token, deal_id, {'person_id': chosen['person_id']})
        row['status'] = 'fixed' if 200 <= sc < 300 else 'error'
        row['new_person_id'] = chosen['person_id']
        log(f"[{phase.upper()}_{row['status'].upper()}] deal={deal_id} person={chosen['person_id']}")
    else:
        row['status'] = 'needs_review'
        row['reason'] = 'no_unique_participant_candidate'
    results.append(row); done.add(key); save_state({'done': sorted(done)}); counts[f"{row['phase']}:{row['status']}"] += 1
    processed += 1
    if processed >= BATCH_LIMIT:
        log(f'[PAUSA] {BATCH_DELAY_SEC}s'); time.sleep(BATCH_DELAY_SEC); processed = 0

# refresh after person fixes
all_deals = api_get_all('deals', token, {'status': 'all_not_deleted'})
open_deals = [d for d in all_deals if int(d.get('pipeline_id') or 0) == PIPELINE_ID and int(d.get('stage_id') or 0) != LOST_STAGE_ID]
lost_deals = [d for d in all_deals if int(d.get('pipeline_id') or 0) == PIPELINE_ID and int(d.get('stage_id') or 0) == LOST_STAGE_ID]

# 3) duplicates open -> lost (safe only)
open_groups = defaultdict(list)
for d in open_deals:
    title_base = norm(d.get('title'))
    org_id = extract_id(d.get('org_id'))
    if title_base in JUNK_TITLES:
        continue
    open_groups[(title_base, org_id)].append(d)

dup_groups = [(k, v) for k, v in open_groups.items() if len(v) > 1]
log(f'[PHASE3] duplicate_groups={len(dup_groups)}')
for (title_base, org_id), deals in dup_groups:
    deals = sorted(deals, key=lambda x: int(x.get('id') or 10**12))
    master = deals[0]
    for d in deals[1:]:
        deal_id = int(d.get('id') or 0)
        key = f'dup_to_lost|{deal_id}'
        if key in done:
            continue
        row = {'phase': 'dup_to_lost', 'deal_id': deal_id, 'title': d.get('title'), 'master_deal_id': int(master.get('id') or 0)}
        sc, body = api_update_deal(token, deal_id, {'stage_id': LOST_STAGE_ID})
        row['status'] = 'fixed' if 200 <= sc < 300 else 'error'
        log(f"[DUP_TO_LOST_{row['status'].upper()}] dup={deal_id} master={row['master_deal_id']}")
        results.append(row); done.add(key); save_state({'done': sorted(done)}); counts[f"{row['phase']}:{row['status']}"] += 1
        processed += 1
        if processed >= BATCH_LIMIT:
            log(f'[PAUSA] {BATCH_DELAY_SEC}s'); time.sleep(BATCH_DELAY_SEC); processed = 0

# refresh after dup fixes
all_deals = api_get_all('deals', token, {'status': 'all_not_deleted'})
open_deals = [d for d in all_deals if int(d.get('pipeline_id') or 0) == PIPELINE_ID and int(d.get('stage_id') or 0) != LOST_STAGE_ID]
lost_deals = [d for d in all_deals if int(d.get('pipeline_id') or 0) == PIPELINE_ID and int(d.get('stage_id') or 0) == LOST_STAGE_ID]
open_by_title_org = defaultdict(list)
for d in open_deals:
    open_by_title_org[(norm(d.get('title')), extract_id(d.get('org_id')))].append(d)

# 4) rescue lost -> entry (safe only)
log(f'[PHASE4] lost_total={len(lost_deals)}')
for d in lost_deals:
    deal_id = int(d.get('id') or 0)
    key = f'rescue_lost|{deal_id}'
    if key in done:
        continue

    title = d.get('title') or ''
    title_base = norm(title)
    if title_base in JUNK_TITLES:
        continue

    org_id = extract_id(d.get('org_id'))
    person_id = extract_id(d.get('person_id'))
    open_same = open_by_title_org.get((title_base, org_id), [])

    # strong signals for rescue:
    # - no open master same title+org
    # - appears in today's matched deal ids OR exact signal in export sheet
    # - has org and person
    should_rescue = (
        len(open_same) == 0 and
        org_id != 0 and
        person_id != 0 and
        (
            deal_id in matched_deal_ids or
            norm_key(title) in sheet_signals or
            norm_key(title_base) in sheet_signals
        )
    )

    row = {'phase': 'rescue_lost', 'deal_id': deal_id, 'title': title, 'org_id': org_id, 'person_id': person_id}
    if should_rescue:
        sc, body = api_update_deal(token, deal_id, {'stage_id': ENTRY_STAGE_ID})
        row['status'] = 'fixed' if 200 <= sc < 300 else 'error'
        log(f"[RESCUE_LOST_{row['status'].upper()}] deal={deal_id}")
    else:
        row['status'] = 'skip'
    results.append(row); done.add(key); save_state({'done': sorted(done)}); counts[f"{row['phase']}:{row['status']}"] += 1
    processed += 1
    if processed >= BATCH_LIMIT:
        log(f'[PAUSA] {BATCH_DELAY_SEC}s'); time.sleep(BATCH_DELAY_SEC); processed = 0

# final audit
all_deals = api_get_all('deals', token, {'status': 'all_not_deleted'})
open_deals = [d for d in all_deals if int(d.get('pipeline_id') or 0) == PIPELINE_ID and int(d.get('stage_id') or 0) != LOST_STAGE_ID]
lost_deals = [d for d in all_deals if int(d.get('pipeline_id') or 0) == PIPELINE_ID and int(d.get('stage_id') or 0) == LOST_STAGE_ID]

by_title = defaultdict(list)
by_org = defaultdict(list)
mismatch = []
without_org = []
without_person = []

for d in open_deals:
    deal_id = int(d.get('id') or 0)
    title_base = norm(d.get('title'))
    org_id = extract_id(d.get('org_id'))
    person_id = extract_id(d.get('person_id'))
    by_title[title_base].append(deal_id)
    if org_id:
        by_org[org_id].append(deal_id)
    else:
        without_org.append({'deal_id': deal_id, 'title': d.get('title')})
    if not person_id:
        without_person.append({'deal_id': deal_id, 'title': d.get('title')})
    else:
        p_org = person_org_cache.get(person_id, 0)
        if p_org and org_id and p_org != org_id:
            mismatch.append({'deal_id': deal_id, 'title': d.get('title'), 'org_id': org_id, 'person_id': person_id, 'person_org_id': p_org})

summary = {
    'apply_counts': dict(counts),
    'final_open': {
        'total': len(open_deals),
        'without_org': len(without_org),
        'without_person': len(without_person),
        'duplicate_title_bases': len({k:v for k,v in by_title.items() if k and len(v) > 1}),
        'orgs_with_multiple_open_deals': len({k:v for k,v in by_org.items() if len(v) > 1}),
        'person_org_mismatch_count': len(mismatch),
    },
    'final_lost_total': len(lost_deals),
    'samples': {
        'without_org': without_org[:20],
        'without_person': without_person[:20],
        'mismatch': mismatch[:20],
    }
}

RESULT_JSON.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding='utf-8')
SUMMARY_JSON.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')

log('[FINAL_COUNTS]')
for k, v in sorted(counts.items()):
    log(f'{k}={v}')
log('[FINAL_SUMMARY]')
log(json.dumps(summary['final_open'], ensure_ascii=False, indent=2))
log(f'RESULT_JSON={RESULT_JSON}')
log(f'SUMMARY_JSON={SUMMARY_JSON}')
