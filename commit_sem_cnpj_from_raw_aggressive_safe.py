import csv, json, os, re, difflib, requests, time
from pathlib import Path

API="https://api.pipedrive.com/v1"
TOKEN=os.getenv("PIPEDRIVE_API_TOKEN")
RAW="/root/sdr-vps/runtime/manual_enrich_raw.txt"
SEM="/root/sdr-vps/runtime/funil2_orgs_sem_cnpj.json"
CNPJ_FIELD="aa3d1d254e5f80a2e5c791cdd390ef58ccfb68f0"
DRY_RUN=os.getenv("DRY_RUN","1")!="0"

def digits(v): return re.sub(r"\D+","",str(v or ""))

def cnpj_valid(c):
    c=digits(c)
    if len(c)!=14 or c==c[0]*14: return False
    nums=[int(x) for x in c]
    w1=[5,4,3,2,9,8,7,6,5,4,3,2]
    d1=11-(sum(nums[i]*w1[i] for i in range(12))%11)
    d1=0 if d1>=10 else d1
    w2=[6,5,4,3,2,9,8,7,6,5,4,3,2]
    d2=11-(sum(nums[i]*w2[i] for i in range(13))%11)
    d2=0 if d2>=10 else d2
    return nums[12]==d1 and nums[13]==d2

STOP=set("brasil brasileira brasileiro do da de das dos e para com ltda sa s/a grupo comercio comércio industria indústria servicos serviços holding empresa cia companhia".split())

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
    if a in b or b in a: return 92
    return int(difflib.SequenceMatcher(None,a,b).ratio()*100)

def req(m,p,**k):
    params=k.pop("params",{}) or {}
    params["api_token"]=TOKEN
    r=requests.request(m,f"{API}/{p}",params=params,timeout=60,**k)
    r.raise_for_status()
    return r.json()

# candidatos vindos do arquivo bruto
cands=[]
for line in Path(RAW).read_text(encoding="utf-8",errors="ignore").splitlines():
    line=line.strip()
    if not line or line.lower().startswith("empresa"):
        continue
    parts=[x.strip() for x in line.split(",")]
    if len(parts)<3:
        continue
    empresa,cnpj,telefone=parts[0],digits(parts[1]),digits(parts[2])
    if cnpj_valid(cnpj):
        cands.append({"empresa":empresa,"cnpj":cnpj,"telefone":telefone})

print("CANDIDATOS_VALIDOS_ARQUIVO:",len(cands))

sem=json.load(open(SEM,encoding="utf-8"))

filled=0
skipped=0
review=0

for org in sem:
    org_id=int(org["org_id"])
    name=org["name"]

    ranked=sorted(
        [(score(name,c["empresa"]),c) for c in cands],
        key=lambda x:x[0],
        reverse=True
    )

    if not ranked or ranked[0][0] < 72:
        print("NO_MATCH:",org_id,name)
        review+=1
        continue

    top_score,top=ranked[0]
    second=ranked[1][0] if len(ranked)>1 else 0

    # se empate/ambíguo, não escreve
    if second >= top_score - 2:
        print("AMBIGUO:",org_id,name,"top",top_score,top["empresa"],"second",second,ranked[1][1]["empresa"])
        review+=1
        continue

    data=req("GET",f"organizations/{org_id}").get("data") or {}
    atual=digits(data.get(CNPJ_FIELD))

    if atual:
        print("SKIP_HAS_CNPJ:",org_id,name,atual)
        skipped+=1
        continue

    if DRY_RUN:
        print("WOULD_FILL:",org_id,name,"=>",top["empresa"],top["cnpj"],"score",top_score)
        filled+=1
    else:
        try:
            req("PUT",f"organizations/{org_id}",json={CNPJ_FIELD:top["cnpj"]})
            print("CNPJ_FILLED:",org_id,name,"=>",top["empresa"],top["cnpj"],"score",top_score)
            filled+=1
        except Exception as e:
            print("ERROR_FILL:",org_id,name,top["cnpj"],str(e))
            skipped+=1

    time.sleep(0.25)

print("[FINAL]")
print("filled_or_would_fill=",filled)
print("skipped=",skipped)
print("review=",review)
print("dry_run=",DRY_RUN)
