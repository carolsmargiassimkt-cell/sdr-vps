import os, json, requests
from datetime import datetime

TOKEN=os.getenv("PIPEDRIVE_API_TOKEN") or os.getenv("PIPEDRIVE_TOKEN")
if not TOKEN:
    raise SystemExit("ERRO: PIPEDRIVE_API_TOKEN não encontrado no .env")

BASE="https://api.pipedrive.com/v1"
DEPRECATED={
    "sdr_email_clicked",
    "sdr_clicked_step",
    "sdr_contamination_status",
    "sdr_last_event_at",
    "WhatsApp Status",
    "tentativas_contato",
    "Origem Oficial",
    "Etapa Cadencia",
}

def req(method, path, **kw):
    kw.setdefault("params", {})["api_token"]=TOKEN
    r=requests.request(method, BASE+path, timeout=30, **kw)
    print(method, path, r.status_code)
    if r.status_code >= 400:
        print(r.text[:500])
    return r

r=req("GET","/dealFields")
data=r.json().get("data") or []

found=[f for f in data if f.get("name") in DEPRECATED or f.get("key") in DEPRECATED]

backup=f"data/deprecated_deal_fields_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
os.makedirs("data", exist_ok=True)
open(backup,"w",encoding="utf-8").write(json.dumps(found,indent=2,ensure_ascii=False))

print("\nENCONTRADOS:")
for f in found:
    print(f"id={f.get('id')} name={f.get('name')} key={f.get('key')}")

print("\nBACKUP:", backup)

if os.getenv("CONFIRM_DELETE_SDR_FIELDS")!="YES":
    print("\nDRY_RUN: nada apagado.")
    print("Para apagar de verdade rode:")
    print("CONFIRM_DELETE_SDR_FIELDS=YES python3 scripts/delete_deprecated_pipedrive_fields.py")
    raise SystemExit

for f in found:
    fid=f.get("id")
    name=f.get("name")
    if not fid:
        continue
    rr=req("DELETE", f"/dealFields/{fid}")
    print("DELETE", fid, name, rr.text[:300])
