#!/usr/bin/env python3
import json, os, re, time, requests
from collections import Counter

BASE="https://api.pipedrive.com/v1"
PIPEDRIVE=os.getenv("PIPEDRIVE_API_TOKEN","").strip()
OPORT=os.getenv("OPORT_TOKEN","").strip()
OPORT_URL="https://app.oportunidados.com.br/api/v1/brazilian_companies"
INP="/root/sdr-vps/runtime/recreate_missing_persons_pipeline2_safe_result.json"
OUT="/root/sdr-vps/runtime/retry_429_missing_persons_result.json"

if not PIPEDRIVE: raise SystemExit("sem PIPEDRIVE_API_TOKEN")
if not OPORT: raise SystemExit("sem OPORT_TOKEN")

def digits(x): return re.sub(r"\D+","",str(x or ""))

def pd(method,path,**kw):
    params=kw.pop("params",{}) or {}
    params["api_token"]=PIPEDRIVE
    r=requests.request(method,f"{BASE}/{path}",params=params,timeout=60,**kw)
    r.raise_for_status()
    return r.json().get("data") if r.content else None

def extract(body):
    phones=[]; emails=[]
    def walk(x):
        if isinstance(x,dict):
            for k,v in x.items():
                lk=k.lower()
                if "telefone" in lk or "phone" in lk or "celular" in lk:
                    if isinstance(v,list):
                        for y in v:
                            d=digits(y.get("value") if isinstance(y,dict) else y)
                            if d: phones.append(d)
                    else:
                        d=digits(v)
                        if d: phones.append(d)
                elif "email" in lk:
                    if isinstance(v,list):
                        for y in v:
                            e=str(y.get("value") if isinstance(y,dict) else y).strip()
                            if e: emails.append(e)
                    else:
                        e=str(v or "").strip()
                        if e: emails.append(e)
                else:
                    walk(v)
        elif isinstance(x,list):
            for y in x: walk(y)
    walk(body)
    return (phones[0] if phones else ""), (emails[0] if emails else "")

def name_from(body, org_name):
    for s in body.get("socios") or []:
        if isinstance(s,dict):
            for k in ("nome","name","nome_socio"):
                v=str(s.get(k) or "").strip()
                if v: return v
    return f"Contato {org_name or 'Lead'}"

data=json.load(open(INP,encoding="utf-8"))
targets=[x for x in data if x.get("result")=="no_contact_data" and str(x.get("oportunidados_status"))=="429"]
print("TARGETS_429",len(targets))

res=[]; c=Counter()
for x in targets:
    deal_id=int(x.get("deal_id") or 0)
    org_id=int(x.get("org_id") or 0)
    cnpj=digits(x.get("cnpj"))
    org_name=x.get("org_name") or x.get("title") or ""
    row=dict(x)

    if not deal_id or not org_id or not cnpj:
        row["retry_result"]="skip_missing_data"; c[row["retry_result"]]+=1; res.append(row); print(row); continue

    deal=pd("GET",f"deals/{deal_id}") or {}
    p=deal.get("person_id")
    pid=p.get("value") if isinstance(p,dict) else p
    if pid:
        row["retry_result"]="skip_now_has_person"; c[row["retry_result"]]+=1; res.append(row); print(row); continue

    r=requests.get(f"{OPORT_URL}/{cnpj}/company",headers={"Authorization":f"Bearer {OPORT}"},timeout=60)
    row["retry_status"]=r.status_code
    try: body=r.json() if r.content else {}
    except Exception: body={}

    if r.status_code==429:
        row["retry_result"]="still_429"; c[row["retry_result"]]+=1; res.append(row); print(row); time.sleep(8); continue
    if r.status_code!=200:
        row["retry_result"]=f"api_{r.status_code}"; c[row["retry_result"]]+=1; res.append(row); print(row); time.sleep(3); continue

    phone,email=extract(body)
    row["new_phone"]=phone; row["new_email"]=email
    if not phone and not email:
        row["retry_result"]="no_contact_data"; c[row["retry_result"]]+=1; res.append(row); print(row); time.sleep(3); continue

    payload={"name":name_from(body,org_name),"org_id":org_id}
    if phone: payload["phone"]=[{"value":phone,"primary":True}]
    if email: payload["email"]=[{"value":email,"primary":True}]
    person=pd("POST","persons",json=payload) or {}
    new_id=int(person.get("id") or 0)
    if new_id:
        pd("PUT",f"deals/{deal_id}",json={"person_id":new_id})
        row["new_person_id"]=new_id
        row["retry_result"]="created_person_and_attached_to_deal"
    else:
        row["retry_result"]="person_create_failed"
    c[row["retry_result"]]+=1
    res.append(row)
    print(row)
    time.sleep(5)

json.dump(res,open(OUT,"w",encoding="utf-8"),ensure_ascii=False,indent=2)
print("[FINAL_COUNTS]")
for k,v in c.most_common(): print(f"{k}={v}")
print("[OUTPUT_JSON]",OUT)
