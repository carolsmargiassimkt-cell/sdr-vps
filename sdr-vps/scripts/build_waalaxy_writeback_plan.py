import csv, json, re
from pathlib import Path

WAALAXY_DIR = Path(r"C:\Users\Asus\Downloads")
CRM_EXPORT = Path(r"C:\Users\Asus\Downloads\deals-25681240-26.csv")
OUT = Path("waalaxy_writeback_plan.json")

def norm(s):
    return re.sub(r"\s+", " ", str(s or "").strip())

def key(s):
    return re.sub(r"[^a-z0-9]", "", norm(s).lower())

def read_csv(path):
    for enc in ["utf-8-sig", "latin1", "cp1252"]:
        try:
            txt = path.read_text(encoding=enc)
            break
        except Exception:
            continue
    else:
        raise RuntimeError(f"Não consegui ler {path}")

    first = txt.splitlines()[0] if txt.splitlines() else ""
    delim = ";"
    if first.count(",") > first.count(";"):
        delim = ","
    if first.count("\t") > max(first.count(","), first.count(";")):
        delim = "\t"

    rows = list(csv.DictReader(txt.splitlines(), delimiter=delim))
    return rows

def pick(row, names):
    low = {key(k): v for k, v in row.items()}
    for n in names:
        if key(n) in low and norm(low[key(n)]):
            return norm(low[key(n)])
    return ""

def waalaxy_score(lead):
    txt = " ".join(str(v or "") for v in lead.values()).lower()
    score = 0
    if any(t in txt for t in ["supermerc", "varejo", "shopping", "atacado", "loja", "farmacia", "farmácia", "aliment", "bebida"]):
        score += 30
    if any(t in txt for t in ["marketing", "trade", "comercial", "vendas", "crm", "growth", "diretor", "gerente", "head", "ceo"]):
        score += 25
    if pick(lead, ["email", "e-mail", "professional email"]):
        score += 20
    if pick(lead, ["phone", "telefone", "whatsapp", "mobile"]):
        score += 10
    if pick(lead, ["linkedin", "linkedin url", "profile url", "profile"]):
        score += 10
    if any(t in " ".join(str(v or "").lower() for v in lead.values()) for t in ["convite aceito", "accepted", "aceitou", "reply", "replied", "respondeu"]):
        score += 25
    return min(score, 100)

crm_rows = read_csv(CRM_EXPORT)

crm_by_org = {}
crm_by_person = {}
crm_by_deal_title = {}

for r in crm_rows:
    deal_id = pick(r, ["ID", "Deal - ID", "Negócio - ID", "Deal ID"])
    deal_title = pick(r, ["Title", "Título", "Negócio", "Deal - Title", "Nome do negócio"])
    org = pick(r, ["Organization", "Organização", "Org", "Organização - Nome", "Organization name"])
    person = pick(r, ["Person", "Pessoa", "Pessoa - Nome", "Contact person", "Person name"])

    org_id = pick(r, ["Organiza??o - ID", "Organization ID", "Org ID"])

    if org:
        crm_by_org.setdefault(key(org), []).append({"deal_id": deal_id, "deal_title": deal_title, "org": org, "org_id": org_id, "person": person, "raw": r})
    if person:
        crm_by_person.setdefault(key(person), []).append({"deal_id": deal_id, "deal_title": deal_title, "org": org, "org_id": org_id, "person": person, "raw": r})
    if deal_title:
        crm_by_deal_title.setdefault(key(deal_title), []).append({"deal_id": deal_id, "deal_title": deal_title, "org": org, "org_id": org_id, "person": person, "raw": r})

plan = []
seen = set()

for f in WAALAXY_DIR.glob("*.csv"):
    if f.name.lower().startswith("deals-"):
        continue

    try:
        rows = read_csv(f)
    except Exception as e:
        print("[CSV_SKIP]", f.name, e)
        continue

    for row in rows:
        name = pick(row, ["Nome", "Name", "Full name", "Full Name", "First Name", "Lead", "Pessoa"])
        company = pick(row, ["Empresa", "Company", "Current company", "Company name", "Organization", "Organização"])
        title = pick(row, ["Cargo", "Title", "Job title", "Position", "Headline"])
        linkedin = pick(row, ["LinkedIn", "Linkedin", "Profile URL", "LinkedIn URL", "Url", "Profile"])
        email = pick(row, ["Email", "E-mail", "Professional email"])
        phone = pick(row, ["Phone", "Telefone", "WhatsApp", "Mobile"])

        if not name or not company:
            continue

        if key(company) in [key("Waalaxy International"), key("Kawaak")]:
            continue

        dedup = (key(name), key(company), key(email or linkedin))
        if dedup in seen:
            continue
        seen.add(dedup)

        score = waalaxy_score(row)
        interaction = any(t in " ".join(str(v or "").lower() for v in row.values()) for t in ["convite aceito", "accepted", "aceitou", "reply", "replied", "respondeu"])
        tipo = "WARM_IMPORT" if interaction else "ICP_IMPORT"

        org_matches = crm_by_org.get(key(company), [])
        person_matches = crm_by_person.get(key(name), [])
        deal_name_matches = crm_by_deal_title.get(key(name), [])

        action = "CREATE_NEW"
        confidence = "medium"

        if deal_name_matches and not org_matches:
            action = "WAALAXY_DEAL_ORFAO_EXISTE"
            confidence = "high"

        if org_matches:
            action = "ADD_PERSON_TO_EXISTING_ORG"
            confidence = "high"

        if person_matches and org_matches:
            action = "UPDATE_EXISTING_PERSON_AND_ORG"
            confidence = "high"

        # pessoa igual mas empresa n?o bate = N?O atualizar autom?tico
        if person_matches and not org_matches and not deal_name_matches:
            action = "REVIEW_PERSON_EXISTS_DIFFERENT_ORG"
            confidence = "review"

        plan.append({
            "action": action,
            "confidence": confidence,
            "tipo": tipo,
            "waalaxy_score": score,
            "name": name,
            "company": company,
            "title": title,
            "email": email,
            "phone": phone,
            "linkedin": linkedin,
            "source_file": f.name,
            "crm_org_matches": org_matches[:3],
            "crm_person_matches": person_matches[:3],
            "crm_deal_name_matches": deal_name_matches[:3],
        })

OUT.write_text(json.dumps(plan, ensure_ascii=False, indent=2), encoding="utf-8")

print("CRM_ROWS:", len(crm_rows))
print("PLANO_TOTAL:", len(plan))
print("CREATE_NEW:", sum(1 for x in plan if x["action"] == "CREATE_NEW"))
print("WAALAXY_DEAL_ORFAO_EXISTE:", sum(1 for x in plan if x["action"] == "WAALAXY_DEAL_ORFAO_EXISTE"))
print("ADD_PERSON_TO_EXISTING_ORG:", sum(1 for x in plan if x["action"] == "ADD_PERSON_TO_EXISTING_ORG"))
print("UPDATE_EXISTING_PERSON_AND_ORG:", sum(1 for x in plan if x["action"] == "UPDATE_EXISTING_PERSON_AND_ORG"))
print("REVIEW:", sum(1 for x in plan if "REVIEW" in x["action"]))
print("RELATORIO:", OUT.resolve())