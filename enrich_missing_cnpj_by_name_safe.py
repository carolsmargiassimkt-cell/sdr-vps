import os, json, re, time, requests, difflib, csv
from pathlib import Path

OPORT_TOKEN=os.getenv("OPORT_TOKEN")
OPORT_BASE="https://app.oportunidados.com.br/api/v1/brazilian_companies"
IN="/root/sdr-vps/runtime/funil2_orgs_sem_cnpj.json"
OUT="/root/sdr-vps/runtime/funil2_missing_cnpj_suggestions.csv"

if not OPORT_TOKEN:
    raise SystemExit("OPORT_TOKEN vazio")

STOP=set("""
brasil brasileira brasileiro do da de das dos e em para com sem s a o os as
ltda sa s/a me eireli grupo comercio comércio industria indústria servicos serviços
participacoes participações holding empresa cia companhia center shopping supermercado supermercados
""".split())

def norm(s):
    s=str(s or "").lower()
    tr=str.maketrans("áàãâäéèêëíìîïóòõôöúùûüç","aaaaaeeeeiiiiooooouuuuc")
    s=s.translate(tr)
    s=re.sub(r"[^a-z0-9]+"," ",s)
    return " ".join(w for w in s.split() if w not in STOP and len(w)>1)

def score(a,b):
    a,b=norm(a),norm(b)
    if not a or not b: return 0
    if a==b: return 100
    if a in b or b in a: return 94
    return int(difflib.SequenceMatcher(None,a,b).ratio()*100)

def search_company(name):
    # tenta endpoints comuns por query
    candidates=[
        f"{OPORT_BASE}/search",
        f"{OPORT_BASE}",
    ]
    for url in candidates:
        try:
            r=requests.get(url,params={"q":name,"name":name,"term":name,"api_token":OPORT_TOKEN},timeout=40)
            if r.status_code not in (200,404):
                return {"status":r.status_code,"items":[],"raw":r.text[:300]}
            try:
                data=r.json()
            except Exception:
                continue
            items=[]
            if isinstance(data,list):
                items=data
            elif isinstance(data,dict):
                for k in ("data","companies","results","items"):
                    if isinstance(data.get(k),list):
                        items=data[k]
                        break
                if not items and data.get("company"):
                    items=[data.get("company")]
            if items:
                return {"status":r.status_code,"items":items,"raw":""}
        except Exception as e:
            return {"status":"exception","items":[],"raw":str(e)}
    return {"status":"not_found","items":[],"raw":""}

def extract_name_cnpj(item):
    if not isinstance(item,dict): return "",""
    name=""
    cnpj=""
    for k in ("razao_social","legal_name","name","nome","company_name","nome_fantasia"):
        if item.get(k):
            name=str(item.get(k))
            break
    for k in ("cnpj","document","tax_id","cnpj_formatado"):
        if item.get(k):
            cnpj=re.sub(r"\D+","",str(item.get(k)))
            break
    return name,cnpj

orgs=json.load(open(IN,encoding="utf-8"))
rows=[]

for i,o in enumerate(orgs,1):
    org_id=o["org_id"]
    org_name=o["name"]
    res=search_company(org_name)

    best=None
    for item in res["items"]:
        api_name,cnpj=extract_name_cnpj(item)
        if len(cnpj)!=14: 
            continue
        sc=score(org_name,api_name)
        cand={"org_id":org_id,"org_name":org_name,"api_name":api_name,"cnpj":cnpj,"score":sc,"status":res["status"]}
        if not best or cand["score"]>best["score"]:
            best=cand

    if best:
        best["match_status"]="safe_match" if best["score"]>=90 else ("review_low_score" if best["score"]>=80 else "reject_low_score")
        rows.append(best)
        print(best)
    else:
        rows.append({"org_id":org_id,"org_name":org_name,"api_name":"","cnpj":"","score":"","status":res["status"],"match_status":"no_result"})
        print("NO_RESULT",org_id,org_name,res["status"],res.get("raw","")[:120])

    if i%20==0:
        print("PROGRESS",i,"/",len(orgs))
        time.sleep(2)
    else:
        time.sleep(0.3)

fields=["org_id","org_name","api_name","cnpj","score","status","match_status"]
with open(OUT,"w",newline="",encoding="utf-8") as f:
    w=csv.DictWriter(f,fieldnames=fields)
    w.writeheader()
    w.writerows(rows)

from collections import Counter
c=Counter(r["match_status"] for r in rows)
print("[COUNTS]")
for k,v in c.most_common():
    print(k,v)
print("[OUT]",OUT)
