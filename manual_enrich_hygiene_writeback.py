import re, json, csv, os
from pathlib import Path
from collections import defaultdict

RAW = Path("/root/sdr-vps/runtime/manual_enrich_raw.txt")
OUT_CLEAN = Path("/root/sdr-vps/runtime/manual_enrich_clean.csv")
OUT_REVIEW = Path("/root/sdr-vps/runtime/manual_enrich_review.csv")

text = RAW.read_text(encoding="utf-8", errors="ignore")

def digits(v):
    return re.sub(r"\D+", "", str(v or ""))

def valid_cnpj(c):
    c = digits(c)
    if len(c) != 14: return False
    if c in {"00000000000000", "11111111111111", "12345678000190"}: return False
    if re.search(r"123456|234567|345678|456789|567890", c): return False
    return True

def valid_phone(p):
    d = digits(p)
    return 10 <= len(d) <= 11

rows = []
for line in text.splitlines():
    line = line.strip()
    if not line or line.lower().startswith("empresa"):
        continue
    parts = [x.strip() for x in line.split(",")]
    if len(parts) < 3:
        continue
    empresa, cnpj, phone = parts[0], parts[1], parts[2]
    obs = ",".join(parts[3:]).strip() if len(parts) > 3 else ""
    rows.append({
        "empresa": empresa,
        "cnpj": digits(cnpj),
        "telefone": digits(phone),
        "obs": obs,
        "raw": line
    })

by_cnpj = defaultdict(list)
for r in rows:
    by_cnpj[r["cnpj"]].append(r)

clean = []
review = []

for cnpj, items in by_cnpj.items():
    if not valid_cnpj(cnpj):
        for r in items:
            r["motivo"] = "cnpj_invalido_ou_suspeito"
            review.append(r)
        continue

    names = {re.sub(r"\W+", "", x["empresa"].lower())[:18] for x in items}
    phones = [x["telefone"] for x in items if valid_phone(x["telefone"])]

    if not phones:
        for r in items:
            r["motivo"] = "sem_telefone_valido"
            review.append(r)
        continue

    if len(names) > 2:
        for r in items:
            r["motivo"] = "mesmo_cnpj_empresas_diferentes"
            review.append(r)
        continue

    chosen = items[-1]
    chosen["telefone"] = phones[-1]
    chosen["motivo"] = "ok_higienizado"
    clean.append(chosen)

fields = ["empresa","cnpj","telefone","obs","motivo","raw"]
OUT_CLEAN.parent.mkdir(parents=True, exist_ok=True)

with OUT_CLEAN.open("w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=fields)
    w.writeheader()
    w.writerows(clean)

with OUT_REVIEW.open("w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=fields)
    w.writeheader()
    w.writerows(review)

print("[TOTAL_RAW]", len(rows))
print("[CLEAN_OK]", len(clean))
print("[REVIEW]", len(review))
print("[OUT_CLEAN]", OUT_CLEAN)
print("[OUT_REVIEW]", OUT_REVIEW)
