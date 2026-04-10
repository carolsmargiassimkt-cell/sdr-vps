import requests
import os

TOKEN = os.getenv("PIPEDRIVE_TOKEN")

EMPRESAS_SUPER = [
"ambev","supermercados sao joao","super koch","cograin","refrigerantes coroa",
"toyng","adeel","ciclo industria","alimentos triangulo","vinicola aurora",
"bbc group","brasfrut","grupo roma","nacon","alca foods","flex embalagens",
"laticinios cambuiense","softys","suinco","gulozitos","tramontina","brf",
"italac","bimbo","coca-cola","arcor","baly","camil","nestle","danone",
"pif paf","verde campo","bem brasil","itambe","lactalis","jbs"
]

def buscar_deals():
    url = f"https://api.pipedrive.com/v1/deals?status=open&limit=100&api_token={TOKEN}"
    return requests.get(url).json().get("data") or []

def get_person(pid):
    url = f"https://api.pipedrive.com/v1/persons/{pid}?api_token={TOKEN}"
    return requests.get(url).json().get("data") or {}

def limpar_label(pid):
    url = f"https://api.pipedrive.com/v1/persons/{pid}?api_token={TOKEN}"
    requests.put(url, json={"label": None})

print("\n[LIMPEZA REAL POR DEAL]")

deals = buscar_deals()
removidos = 0

for d in deals:

    titulo = (d.get("title") or "").lower()

    pid = d.get("person_id", {}).get("value") if isinstance(d.get("person_id"), dict) else d.get("person_id")

    if not pid:
        continue

    p = get_person(pid)

    label = str(p.get("label") or "")

    # só mexe se tem WPP_CAD1
    if "WPP_CAD1" not in label:
        continue

    # se NÃO for SUPER MINAS → remove
    if not any(emp in titulo for emp in EMPRESAS_SUPER):
        limpar_label(pid)
        print(f"[REMOVIDO] {p.get('name')} - {titulo}")
        removidos += 1

print(f"[TOTAL REMOVIDOS] {removidos}")
