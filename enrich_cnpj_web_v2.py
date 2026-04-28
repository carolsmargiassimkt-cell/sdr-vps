import json, re, time, requests, urllib.parse

IN="/root/sdr-vps/runtime/funil2_orgs_sem_cnpj.json"
OUT="/root/sdr-vps/runtime/web_cnpj_suggestions_v2.json"

headers={"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

def find_cnpj(text):
    matches=re.findall(r'\d{2}\.?\d{3}\.?\d{3}/?\d{4}-?\d{2}', text or "")
    out=[]
    for m in matches:
        d=re.sub(r"\D","",m)
        if len(d)==14 and d not in out:
            out.append(d)
    return out

def clean_name(name):
    name=re.sub(r'\b(lead|deal)\b','',name,flags=re.I)
    name=re.sub(r'[-|].*$','',name).strip()
    return name

def search_urls(q):
    return [
        ("bing", "https://www.bing.com/search", {"q": q}),
        ("ddg", "https://duckduckgo.com/html/", {"q": q}),
    ]

data=json.load(open(IN,encoding="utf-8"))
results=[]

for i,item in enumerate(data,1):
    name=clean_name(item["name"])
    queries=[
        f'{name} CNPJ',
        f'{name} "CNPJ"',
        f'{name} cnpj telefone',
        f'{name} site:cnpj.biz',
    ]

    found=[]
    source=""
    for q in queries:
        for src,url,params in search_urls(q):
            try:
                r=requests.get(url,params=params,headers=headers,timeout=25)
                cnpjs=find_cnpj(r.text)
                if cnpjs:
                    found=cnpjs
                    source=f"{src}:{q}"
                    break
            except Exception as e:
                pass
        if found:
            break
        time.sleep(0.8)

    if found:
        print("FOUND:",name,found[0],source)
        results.append({"org_id":item["org_id"],"name":item["name"],"cnpj":found[0],"source":source})
    else:
        print("NONE:",name)

    if i%20==0:
        print("PROGRESS",i,"/",len(data),"found",len(results))
        time.sleep(2)

json.dump(results,open(OUT,"w",encoding="utf-8"),ensure_ascii=False,indent=2)
print("TOTAL FOUND:",len(results))
print("OUT:",OUT)
