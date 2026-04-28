import zipfile
import xml.etree.ElementTree as ET
from collections import defaultdict

FILE = "/root/sdr-vps/BASE_LEADS_COM_LINKEDIN.xlsx"

def parse_xlsx(path):
    with zipfile.ZipFile(path) as z:
        xml = z.read("xl/worksheets/sheet1.xml")
    root = ET.fromstring(xml)

    rows = []
    for row in root.iter("{http://schemas.openxmlformats.org/spreadsheetml/2006/main}row"):
        values = []
        for c in row:
            t = c.find(".//{http://schemas.openxmlformats.org/spreadsheetml/2006/main}t")
            v = c.find(".//{http://schemas.openxmlformats.org/spreadsheetml/2006/main}v")
            if t is not None:
                values.append(t.text)
            elif v is not None:
                values.append(v.text)
            else:
                values.append("")
        if values:
            rows.append(values)
    return rows

def normalize(s):
    return (s or "").strip().lower()

rows = parse_xlsx(FILE)

print(f"TOTAL LINHAS: {len(rows)}")

groups = defaultdict(list)

for r in rows[1:]:
    empresa = normalize(r[0])
    cnpj = normalize(r[3])
    telefone = normalize(r[4])

    key = cnpj if cnpj else empresa
    groups[key].append({
        "empresa": empresa,
        "cnpj": cnpj,
        "telefone": telefone,
        "raw": r
    })

safe = []
skip = []

for key, items in groups.items():
    if len(items) == 1:
        safe.append(items[0])
        continue

    telefones = set()
    cleaned = []
    for i in items:
        if i["telefone"] not in telefones:
            telefones.add(i["telefone"])
            cleaned.append(i)

    master = cleaned[0]

    if len(cleaned) > 5:
        skip.append({"key": key, "reason": "muitos participantes"})
        continue

    safe.append(master)

print(f"SAFE: {len(safe)}")
print(f"SKIP: {len(skip)}")

print("\\nEXEMPLO SAFE:")
for i in safe[:5]:
    print(i)

print("\\nEXEMPLO SKIP:")
for i in skip[:5]:
    print(i)
