import json, re, time, requests

IN="/root/sdr-vps/runtime/funil2_orgs_sem_cnpj.json"
OUT="/root/sdr-vps/runtime/google_cnpj_suggestions.json"

headers={"User-Agent":"Mozilla/5.0"}

def find_cnpj(text):
    matches=re.findall(r'\d{2}\.?\d{3}\.?\d{3}/?\d{4}-?\d{2}', text)
    cleaned=[re.sub(r"\D","",m) for m in matches if len(re.sub(r"\D","",m))==14]
    return list(set(cleaned))

data=json.load(open(IN,encoding="utf-8"))
results=[]

for i,item in enumerate(data):
    name=item["name"]
    query=f'"{name}" CNPJ'

    try:
        r=requests.get("https://www.google.com/search",
            params={"q":query},
            headers=headers,
            timeout=20
        )

        cnpjs=find_cnpj(r.text)

        if cnpjs:
            print("FOUND:",name,cnpjs[0])
            results.append({
                "org_id":item["org_id"],
                "name":name,
                "cnpj":cnpjs[0]
            })
        else:
            print("NONE:",name)

    except Exception as e:
        print("ERROR:",name,str(e))

    time.sleep(3)

json.dump(results,open(OUT,"w",encoding="utf-8"),ensure_ascii=False,indent=2)
print("TOTAL FOUND:",len(results))
print("OUT:",OUT)
