import json
import re
import unicodedata
import requests
from collections import defaultdict
from pathlib import Path

CFG = Path('/root/sdr-vps/config/system_config.json')
RESULT_JSON = Path('/root/sdr-vps/runtime/final_cleanup_last_safe_result.json')

PIPELINE_ID = 2
LOST_STAGE_ID = 50
JUNK_ORPHAN_TITLES = {
    'NONE', 'NEGOCIO', 'MUEL TESTE',
    'YOHANA', 'FRANCYNE', 'ODAIR', 'CRISTIANO DE OLIVEIRA BERNARDINI',
    'ENIVALDO ANGELO PEREIRA', 'BRENDA', 'MARITA BATISTA',
    'THIAGO CONCEICAO BRAGA LEADSTER'
}
DUP_TITLES_TO_FIX = {'FUNDO', 'NAN'}

cfg = json.load(open(CFG))
TOKEN = cfg.get('pipedrive_token') or cfg.get('pipedrive_api_token') or ''

def norm(v):
    s = str(v or '').strip()
    s = unicodedata.normalize('NFKD', s)
    s = ''.join(ch for ch in s if not unicodedata.combining(ch))
    s = s.upper()
    s = re.sub(r'\s*-\s*LEAD\s*$', '', s, flags=re.I)
    s = re.sub(r'\s*-\s*DEAL\s*$', '', s, flags=re.I)
    s = re.sub(r'\s+', ' ', s).strip(' -')
    return s

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

def get_all_open_p2():
    out = []
    start = 0
    while True:
        r = requests.get(
            'https://api.pipedrive.com/v1/deals',
            params={'api_token': TOKEN, 'status': 'open', 'start': start, 'limit': 500},
            timeout=60,
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

def update_stage(deal_id, stage_id):
    r = requests.put(
        f'https://api.pipedrive.com/v1/deals/{deal_id}',
        params={'api_token': TOKEN},
        json={'stage_id': stage_id},
        timeout=60,
    )
    return r.status_code, r.text

deals = get_all_open_p2()
results = []

# 1) sem org -> perdido (somente os 11 "lixo"/órfãos)
for d in deals:
    deal_id = int(d.get('id') or 0)
    org_id = extract_id(d.get('org_id'))
    title = d.get('title') or ''
    title_base = norm(title)

    if not org_id and title_base in JUNK_ORPHAN_TITLES:
        sc, body = update_stage(deal_id, LOST_STAGE_ID)
        results.append({
            'phase': 'orphan_to_lost',
            'deal_id': deal_id,
            'title': title,
            'status_code': sc,
            'ok': 200 <= sc < 300
        })
        print('orphan_to_lost', deal_id, sc, flush=True)

# refresh
deals = get_all_open_p2()

# 2) duplicados finais (FUNDO / NAN) -> manter mais antigo
by_title = defaultdict(list)
for d in deals:
    by_title[norm(d.get('title'))].append(d)

for title_base in DUP_TITLES_TO_FIX:
    group = sorted(by_title.get(title_base, []), key=lambda x: int(x.get('id') or 10**12))
    if len(group) <= 1:
        continue
    master = int(group[0].get('id') or 0)
    for dup in group[1:]:
        dup_id = int(dup.get('id') or 0)
        sc, body = update_stage(dup_id, LOST_STAGE_ID)
        results.append({
            'phase': 'dup_to_lost',
            'title_base': title_base,
            'master_deal_id': master,
            'deal_id': dup_id,
            'status_code': sc,
            'ok': 200 <= sc < 300
        })
        print('dup_to_lost', title_base, dup_id, sc, flush=True)

RESULT_JSON.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding='utf-8')
print('RESULT_JSON =', RESULT_JSON)
