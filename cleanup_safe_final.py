#!/usr/bin/env python3
import os, re, json, time, requests

TOKEN = os.getenv("PIPEDRIVE_TOKEN") or ""
if not TOKEN:
    print("ERRO: export PIPEDRIVE_TOKEN='SEU_TOKEN'")
    exit(1)

BASE = "https://api.pipedrive.com/v1"
PIPELINE_ID = 2
APPLY = os.getenv("APPLY", "0") == "1"

def req(method, path, **kwargs):
    params = kwargs.pop("params", {})
    params["api_token"] = TOKEN
    r = requests.request(method, BASE + path, params=params, timeout=30, **kwargs)
    r.raise_for_status()
    return r.json()

def norm(s):
    return (s or "").strip()

def is_empresa(nome):
    n = (nome or "").upper()
    termos_fortes = ["LTDA","S.A"," SA ","EIRELI","HOLDING","INDUSTRIA","COMERCIO","DISTRIBUIDORA","CONDOMINIO","EMPRESARIAL","SHOPPING","ASSOCIACAO","ASSOCIAÇÃO","FUNDO","PROMOCAO","PROMOÇÃO","RADIO","EIRELI","CIA","COMPANHIA"]
    if any(t in (" " + n + " ") for t in termos_fortes):
        return True
    return False

def is_placeholder(nome):
    n = nome.upper()
    return any(x in n for x in ["CONTATO","RESPONSAVEL","A DEFINIR","SEM NOME"])

def get_deals():
    start=0; out=[]
    while True:
        r = req("GET","/deals", params={"pipeline_id":PIPELINE_ID,"status":"open","start":start,"limit":100})
        data = r.get("data") or []
        if not data: break
        out += data
        more = r.get("additional_data",{}).get("pagination",{})
        if not more.get("more_items_in_collection"): break
        start = more.get("next_start", start+100)
    return out

def get_person(pid):
    return req("GET", f"/persons/{pid}")["data"]

def remove_person(deal_id):
    req("PUT", f"/deals/{deal_id}", json={"person_id": None})

deals = get_deals()
print({"deals": len(deals), "mode": "APPLY" if APPLY else "DRY_RUN"})

for d in deals:
    pid = d.get("person_id",{}).get("value") if isinstance(d.get("person_id"),dict) else None
    if not pid:
        continue

    try:
        p = get_person(pid)
    except:
        continue

    nome = norm(p.get("name"))
    email = (p.get("email") or [{}])[0].get("value","")
    phone = (p.get("phone") or [{}])[0].get("value","")
    org_obj = d.get("org_id") or {}
    org = norm(org_obj.get("name")) if isinstance(org_obj, dict) else ""

    remove = False
    motivos = []

    # REGRAS SEGURAS
    nome_parts = [x for x in nome.strip().split() if x] if nome else []
    if is_empresa(nome):
        remove = True
        motivos.append("empresa_como_person")

    if is_placeholder(nome):
        remove = True
        motivos.append("placeholder")

    if nome and org and nome.lower() == org.lower():
        remove = True
        motivos.append("nome_igual_empresa")

    if not email and not phone:
        remove = True
        motivos.append("sem_contato")

    if remove:
        out = {
            "deal_id": d["id"],
            "person": nome,
            "org": org,
            "motivos": motivos
        }

        if APPLY:
            try:
                remove_person(d["id"])
                out["status"] = "REMOVIDO"
            except Exception as e:
                out["status"] = "ERRO"
                out["erro"] = str(e)
        else:
            out["status"] = "REMOVERIA"

        print(out)

    time.sleep(0.1)
