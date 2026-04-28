import json, requests, re, time
from pathlib import Path

CFG = Path('/root/sdr-vps/config/system_config.json')
INSP = Path('/root/sdr-vps/runtime/inspect_clear_contamination.json')

cfg = json.load(open(CFG))
PD_TOKEN = cfg.get('pipedrive_token') or cfg.get('pipedrive_api_token') or ''

# pega token da oportunidados automaticamente
txt = open('/root/sdr-vps/fill_39_missing_persons_from_sheet_cnpj.py').read()
m = re.search(r'OPORTUNIDADOS_TOKEN\s*=\s*"([^"]+)"', txt)
OPORTUN_TOKEN = m.group(1) if m else ''

def digits(v):
    return re.sub(r'\D+', '', str(v or ''))

def pd_get(path):
    r = requests.get(
        f'https://api.pipedrive.com/v1/{path}',
        params={'api_token': PD_TOKEN},
        timeout=20
    )
    r.raise_for_status()
    return r.json().get('data')

def fetch_oportunidados(cnpj):
    if not cnpj:
        return None
    r = requests.get(
        f'https://app.oportunidados.com.br/api/v1/brazilian_companies/{cnpj}/company',
        headers={'Authorization': f'Bearer {OPORTUN_TOKEN}'},
        timeout=20
    )
    if r.status_code != 200:
        return None
    return r.json()

rows = json.load(open(INSP, encoding='utf-8'))

for row in rows:
    deal_id = row['deal_id']
    org_id = row['org_id']

    org = pd_get(f'organizations/{org_id}') or {}

    # tenta pegar cnpj do org (ajustar se necessário)
    cnpj = digits(
        org.get('cnpj') or
        org.get('cnpj_formatado') or
        org.get('value') or ''
    )

    print(f"\nDEAL {deal_id} | ORG {org_id} | CNPJ={cnpj}")

    if not cnpj:
        print("❌ sem cnpj → skip")
        continue

    data = fetch_oportunidados(cnpj)

    if not data:
        print("❌ sem resposta API → skip")
        continue

    # tenta extrair nome principal
    nome = (
        data.get('razao_social') or
        data.get('nome_fantasia') or
        ''
    )

    print("✔️ MATCH OPORTUNIDADOS:", nome)

    # aqui você decide se quer atualizar depois
    time.sleep(0.3)
