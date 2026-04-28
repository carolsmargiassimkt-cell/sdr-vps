from collections import Counter
from crm.pipedrive_client import PipedriveClient
import re

PIPELINE_ID = 2
ALERT_PARTICIPANTS = 5

def extract_id(value):
    if isinstance(value, dict):
        try:
            return int(value.get("value") or value.get("id") or 0)
        except Exception:
            return 0
    try:
        return int(value or 0)
    except Exception:
        return 0

def normalize_name(v):
    return re.sub(r"\s+", " ", str(v or "").strip()).upper()

client = PipedriveClient()
deals = client.get_deals() or []
p2 = [d for d in deals if int(d.get("pipeline_id") or 0) == PIPELINE_ID]

total = len(p2)
with_org = 0
with_person = 0
bad_org = []
bad_person = []
high_participants = []
title_dupes = Counter()
org_deal_count = Counter()

for d in p2:
    deal_id = int(d.get("id") or 0)
    title = normalize_name(d.get("title"))
    title_dupes[title] += 1

    org_id = extract_id(d.get("org_id"))
    person_id = extract_id(d.get("person_id"))

    if org_id:
        with_org += 1
        org_deal_count[org_id] += 1
    else:
        bad_org.append({"deal_id": deal_id, "title": title})

    if person_id:
        with_person += 1
    else:
        bad_person.append({"deal_id": deal_id, "title": title})

    try:
        participants = client.get_deal_participants(deal_id) or []
    except Exception:
        participants = []

    if len(participants) >= ALERT_PARTICIPANTS:
        high_participants.append({
            "deal_id": deal_id,
            "title": title,
            "participants": len(participants),
            "org_id": org_id,
            "person_id": person_id,
        })

dup_titles = [(k,v) for k,v in title_dupes.items() if k and v > 1]
dup_orgs = [(org,v) for org,v in org_deal_count.items() if v > 1]

print("TOTAL_DEALS_PIPELINE_2 =", total)
print("DEALS_COM_ORG =", with_org)
print("DEALS_COM_PESSOA_PRINCIPAL =", with_person)
print("DEALS_SEM_ORG =", len(bad_org))
print("DEALS_SEM_PESSOA_PRINCIPAL =", len(bad_person))
print("DEALS_COM_PARTICIPANTES_ALTOS =", len(high_participants))
print("TITULOS_DUPLICADOS =", len(dup_titles))
print("ORGS_COM_MULTIPLOS_DEALS =", len(dup_orgs))

print("\nEXEMPLOS_DEALS_SEM_ORG:")
for x in bad_org[:20]:
    print(x)

print("\nEXEMPLOS_DEALS_SEM_PESSOA:")
for x in bad_person[:20]:
    print(x)

print("\nEXEMPLOS_PARTICIPANTES_ALTOS:")
for x in high_participants[:20]:
    print(x)

print("\nTOP_TITULOS_DUPLICADOS:")
for x in sorted(dup_titles, key=lambda t: -t[1])[:20]:
    print(x)

print("\nTOP_ORGS_COM_MULTIPLOS_DEALS:")
for x in sorted(dup_orgs, key=lambda t: -t[1])[:20]:
    print(x)
