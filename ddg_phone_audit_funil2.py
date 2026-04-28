#!/usr/bin/env python3
import os, re, csv, time, requests
from bs4 import BeautifulSoup

API=os.getenv("PIPEDRIVE_API_TOKEN","").strip()
BASE="https://api.pipedrive.com/v1"
OUT="/root/sdr-vps/runtime/ddg_phone_audit_funil2.csv"

if not API:
    raise SystemExit("sem PIPEDRIVE_API_TOKEN")

def get(path, params=None):
    p=dict(params or {})
    p["api_token"]=API
    r=requests.get(f"{BASE}/{path}",params=p,timeout=60)
    r.raise_for_status()
    return r.json()

def paged(path, params=None):
    out=[]; start=0
    while True:
        body=get(path,{**(params or {}),"start":start,"limit":500})
        rows=body.get("data") or []
        out.extend(rows)
        pag=((body.get("additional_data") or {}).get("pagination") or {})
        if not pag.get("more_items_in_collection"): break
        start=int(pag.get("next_start") or 0)
    return out

def clean_phone(s):
    d=re.sub(r"\D+","",s or "")
    if len(d) >= 10 and len(d) <= 13:
        return d
    return ""

def ddg_search(query):
    url="https://duckduckgo.com/html/"
    r=requests.get(url,params={"q":query},headers={"User-Agent":"Mozilla/5.0"},timeout=30)
    r.raise_for_status()
    return r.text

def extract_candidates(html):
    text=BeautifulSoup(html,"html.parser").get_text(" ",strip=True)
    phones=set()
    for m in re.findall(r"(?:\+?55\s*)?(?:\(?\d{2}\)?\s*)?\d{4,5}[-.\s]?\d{4}", text):
        p=clean_phone(m)
        if p:
            phones.add(p)
    return sorted(phones)[:5], text[:500]

deals=paged("deals",{"status":"open","pipeline_id":2})
rows=[]

for d in deals:
    person=d.get("person_id")
    pid=person.get("value") if isinstance(person,dict) else person
    if pid:
        continue

    org=d.get("org_id") or {}
    org_name=org.get("name") if isinstance(org,dict) else ""
    title=d.get("title") or ""
    q=f'"{org_name or title}" telefone Google meu negócio'

    try:
        html=ddg_search(q)
        phones,snippet=extract_candidates(html)
        row={
            "deal_id":d.get("id"),
            "title":title,
            "org_name":org_name,
            "query":q,
            "phones":";".join(phones),
            "snippet":snippet,
            "status":"candidate_found" if phones else "no_phone_found"
        }
    except Exception as e:
        row={
            "deal_id":d.get("id"),
            "title":title,
            "org_name":org_name,
            "query":q,
            "phones":"",
            "snippet":"",
            "status":f"error:{e}"
        }

    print(row)
    rows.append(row)
    time.sleep(4)

with open(OUT,"w",newline="",encoding="utf-8") as f:
    w=csv.DictWriter(f,fieldnames=["deal_id","title","org_name","query","phones","snippet","status"])
    w.writeheader()
    w.writerows(rows)

print("OUTPUT",OUT,len(rows))
