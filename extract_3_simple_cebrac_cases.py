import requests, json, re

TOKEN = "6a15950c356566f697425b11567f391e5993e544d3030bc836e41fd07a2599dd"

cases = [
    {"deal_id": 2961, "org_name": "Atacadao Das Tintas", "cnpj": "65373155000119"},
    {"deal_id": 3001, "org_name": "Mandioca Predilecta", "cnpj": "66021991000105"},
    {"deal_id": 3002, "org_name": "Qualiseg", "cnpj": "65586841000178"},
]

out = []

for item in cases:
    cnpj = item["cnpj"]
    r = requests.get(
        f'https://app.oportunidados.com.br/api/v1/brazilian_companies/{cnpj}/company',
        headers={'Authorization': f'Bearer {TOKEN}'},
        timeout=20
    )
    data = r.json() if r.status_code == 200 else {}
    company = data.get("company") or {}
    socios = data.get("socios") or []

    out.append({
        "deal_id": item["deal_id"],
        "org_name": item["org_name"],
        "cnpj": cnpj,
        "razao_social": company.get("razao_social"),
        "nome_fantasia": company.get("nome_fantasia"),
        "email": company.get("email"),
        "telefone_1": f"{company.get('ddd_1') or ''}{company.get('telefone_1') or ''}",
        "telefone_2": f"{company.get('ddd_2') or ''}{company.get('telefone_2') or ''}",
        "socios": socios[:5],
    })

print(json.dumps(out, ensure_ascii=False, indent=2))
