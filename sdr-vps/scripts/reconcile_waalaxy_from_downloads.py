import os, csv, re, json, time, argparse
from pathlib import Path
import requests

TOKEN = os.getenv("PIPEDRIVE_API_TOKEN")
BASE = "https://api.pipedrive.com/v1"

DOWNLOADS = Path(r"C:\Users\Asus\Downloads")

INTERACTION_TERMS = [
    "convite aceito", "accepted", "accept", "aceitou",
    "respondeu", "reply", "replied", "respondido",
    "mensagem respondida", "message replied"
]

def norm(s):
    return re.sub(r"\s+", " ", str(s or "").strip())

def key(s):
    return re.sub(r"[^a-z0-9]", "", norm(s).lower())

def api(method, path, **kwargs):
    if not TOKEN:
        raise SystemExit("ERRO: defina PIPEDRIVE_API_TOKEN no ambiente.")
    params = kwargs.pop("params", {}) or {}
    params["api_token"] = TOKEN
    r = requests.request(method, BASE + path, params=params, timeout=40, **kwargs)
    if r.status_code == 429:
        wait = int(r.headers.get("Retry-After", "5"))
        print(f"[RATE_LIMIT] aguardando {wait}s")
        time.sleep(wait)
        return api(method, path, **kwargs, params=params)
    try:
        data = r.json()
    except Exception:
        data = {"raw": r.text}
    if r.status_code >= 400:
        print("[API_ERRO]", method, path, r.status_code, data)
    return data

def pick(row, names):
    low = {key(k): v for k, v in row.items()}
    for n in names:
        if key(n) in low and norm(low[key(n)]):
            return norm(low[key(n)])
    return ""

def row_text(row):
    return " ".join(norm(v).lower() for v in row.values())

def has_interaction(row):
    txt = row_text(row)
    return any(t in txt for t in INTERACTION_TERMS)

def read_csvs():
    files = list(DOWNLOADS.glob("*.csv"))
    leads = []
    for f in files:
        try:
            with open(f, "r", encoding="utf-8-sig", newline="") as fh:
                sample = fh.read(4096)
                fh.seek(0)
                try:
                    dialect = csv.Sniffer().sniff(sample, delimiters=",;\\t|")
                    reader = csv.DictReader(fh, dialect=dialect)
                except Exception:
                    fh.seek(0)
                    first = fh.readline()
                    delim = ";"
                    if first.count(",") > first.count(";"):
                        delim = ","
                    if first.count("\\t") > max(first.count(","), first.count(";")):
                        delim = "\\t"
                    fh.seek(0)
                    reader = csv.DictReader(fh, delimiter=delim)
                for row in reader:
                    name = pick(row, ["Nome", "Name", "Full name", "Full Name", "First Name", "Lead", "Pessoa"])
                    company = pick(row, ["Empresa", "Company", "Current company", "Company name", "Organization", "Organização"])
                    title = pick(row, ["Cargo", "Title", "Job title", "Position", "Headline"])
                    linkedin = pick(row, ["LinkedIn", "Linkedin", "Profile URL", "LinkedIn URL", "Url", "Profile"])
                    email = pick(row, ["Email", "E-mail", "Professional email"])
                    phone = pick(row, ["Phone", "Telefone", "WhatsApp", "Mobile"])

                    if not name:
                        continue

                    if not has_interaction(row):
                        continue

                    leads.append({
                        "source_file": f.name,
                        "name": name,
                        "company": company,
                        "title": title,
                        "linkedin": linkedin,
                        "email": email,
                        "phone": phone,
                        "raw": row
                    })
        except Exception as e:
            print("[CSV_SKIP]", f.name, e)
    return leads

def search_org(company):
    if not company:
        return None
    data = api("GET", "/organizations/search", params={"term": company, "exact_match": 0, "limit": 10})
    items = ((data.get("data") or {}).get("items") or [])
    for it in items:
        org = it.get("item") or {}
        if key(org.get("name")) == key(company):
            return org
    return (items[0].get("item") if items else None)

def search_person(name, email=""):
    term = email or name
    data = api("GET", "/persons/search", params={"term": term, "exact_match": 0, "limit": 10})
    items = ((data.get("data") or {}).get("items") or [])
    if email:
        for it in items:
            p = it.get("item") or {}
            emails = p.get("emails") or []
            if any(key(e) == key(email) for e in emails):
                return p
    for it in items:
        p = it.get("item") or {}
        if key(p.get("name")) == key(name):
            return p
    return (items[0].get("item") if items else None)

def search_open_deal_by_person_name(name):
    data = api("GET", "/deals/search", params={"term": name, "status": "open", "limit": 10})
    items = ((data.get("data") or {}).get("items") or [])
    for it in items:
        d = it.get("item") or {}
        if key(d.get("title")) == key(name):
            return d
    return (items[0].get("item") if items else None)

def create_org(company, dry):
    if dry:
        return {"id": "DRY_ORG", "name": company}
    data = api("POST", "/organizations", json={"name": company})
    return data.get("data")

def create_person(lead, org_id, dry):
    payload = {"name": lead["name"]}
    if org_id:
        payload["org_id"] = org_id
    if lead["email"]:
        payload["email"] = [{"value": lead["email"], "primary": True, "label": "work"}]
    if lead["phone"]:
        payload["phone"] = [{"value": lead["phone"], "primary": True, "label": "work"}]
    if dry:
        return {"id": "DRY_PERSON", "name": lead["name"]}
    data = api("POST", "/persons", json=payload)
    return data.get("data")

def update_person_org(person_id, org_id, dry):
    if dry:
        return
    api("PUT", f"/persons/{person_id}", json={"org_id": org_id})

def create_deal(lead, person_id, org_id, dry):
    title = f"{lead['company']} - {lead['name']}" if lead["company"] else lead["name"]
    payload = {
        "title": title,
        "person_id": person_id,
        "org_id": org_id,
        "status": "open"
    }
    if dry:
        return {"id": "DRY_DEAL", "title": title}
    data = api("POST", "/deals", json=payload)
    return data.get("data")

def update_deal(deal_id, lead, person_id, org_id, dry):
    title = f"{lead['company']} - {lead['name']}" if lead["company"] else lead["name"]
    payload = {"title": title}
    if person_id:
        payload["person_id"] = person_id
    if org_id:
        payload["org_id"] = org_id
    if dry:
        return
    api("PUT", f"/deals/{deal_id}", json=payload)

def add_note(deal_id, lead, dry):
    content = (
        f"<b>Conciliação Waalaxy/SalesNav</b><br>"
        f"Pessoa: {lead['name']}<br>"
        f"Empresa: {lead['company']}<br>"
        f"Cargo: {lead['title']}<br>"
        f"LinkedIn: {lead['linkedin']}<br>"
        f"Arquivo: {lead['source_file']}<br>"
        f"Critério: lead com interação/convite aceito/resposta no CSV."
    )
    if dry:
        return
    api("POST", "/notes", json={"deal_id": deal_id, "content": content})

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    dry = not args.apply

    leads = read_csvs()
    seen = set()
    unique = []
    for l in leads:
        k = (key(l["name"]), key(l["company"]))
        if k not in seen:
            seen.add(k)
            unique.append(l)

    print("MODO:", "APPLY" if args.apply else "DRY_RUN")
    print("LEADS_COM_INTERACAO:", len(unique))

    report = []

    for lead in unique:
        print("\n---")
        print("PESSOA:", lead["name"])
        print("EMPRESA:", lead["company"] or "SEM_EMPRESA")
        print("CARGO:", lead["title"])
        print("ARQUIVO:", lead["source_file"])

        if not lead["company"]:
            print("[SKIP] sem empresa no CSV, não vou criar bagunça.")
            report.append({**lead, "action": "skip_sem_empresa"})
            continue

        org = search_org(lead["company"])
        if org:
            org_id = org.get("id")
            print("[ORG_OK]", org_id, org.get("name"))
        else:
            org = create_org(lead["company"], dry)
            org_id = org.get("id")
            print("[ORG_CREATE]", org_id, lead["company"])

        person = search_person(lead["name"], lead["email"])
        if person:
            person_id = person.get("id")
            print("[PERSON_OK]", person_id, person.get("name"))
            update_person_org(person_id, org_id, dry)
            print("[PERSON_ORG_SET]", person_id, "->", org_id)
        else:
            person = create_person(lead, org_id, dry)
            person_id = person.get("id")
            print("[PERSON_CREATE]", person_id, lead["name"])

        deal = search_open_deal_by_person_name(lead["name"])
        if deal:
            deal_id = deal.get("id")
            print("[DEAL_FOUND_RENAME]", deal_id, deal.get("title"))
            update_deal(deal_id, lead, person_id, org_id, dry)
        else:
            deal = create_deal(lead, person_id, org_id, dry)
            deal_id = deal.get("id")
            print("[DEAL_CREATE]", deal_id, deal.get("title"))

        add_note(deal_id, lead, dry)
        print("[NOTE_OK]", deal_id)

        report.append({
            "name": lead["name"],
            "company": lead["company"],
            "title": lead["title"],
            "source_file": lead["source_file"],
            "org_id": org_id,
            "person_id": person_id,
            "deal_id": deal_id,
            "action": "ok_dry" if dry else "applied"
        })

    out = Path("waalaxy_reconcile_report.json")
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print("\nRELATORIO:", out.resolve())

if __name__ == "__main__":
    main()
