from __future__ import annotations

from collections import defaultdict

from common import all_deals, all_orgs, all_persons, org_persons, severity, write_reports
from core.crm_hygiene import contamination_score, person_primary_email


def main():
    rows = []
    orgs = all_orgs()
    person_orgs: dict[int, set[int]] = defaultdict(set)
    org_people: dict[int, list[dict]] = {}

    for org in orgs:
        org_id = int(org.get("id") or 0)
        people = org_persons(org_id) if org_id > 0 else []
        org_people[org_id] = people
        for person in people:
            person_id = int(person.get("id") or 0)
            if person_id > 0:
                person_orgs[person_id].add(org_id)

    for org in orgs:
        org_id = int(org.get("id") or 0)
        people = org_people.get(org_id, [])
        shared_count = sum(1 for person in people if len(person_orgs.get(int(person.get("id") or 0), set())) > 1)
        score, reasons = contamination_score(org=org, persons=people, shared_person_count=shared_count)
        if score:
            rows.append({
                "entity_type": "org",
                "severity": severity(score),
                "risk_score": score,
                "entity_id": org_id,
                "entity_name": org.get("name") or "",
                "people_count": len(people),
                "shared_count": shared_count,
                "email": "",
                "deal_id": "",
                "reasons": ",".join(reasons),
            })

    for person in all_persons():
        person_id = int(person.get("id") or 0)
        score, reasons = contamination_score(person=person, shared_org_count=len(person_orgs.get(person_id, set())))
        if score:
            rows.append({
                "entity_type": "person",
                "severity": severity(score),
                "risk_score": score,
                "entity_id": person_id,
                "entity_name": person.get("name") or "",
                "people_count": "",
                "shared_count": len(person_orgs.get(person_id, set())),
                "email": person_primary_email(person),
                "deal_id": "",
                "reasons": ",".join(reasons),
            })

    for deal in all_deals():
        score, reasons = contamination_score(deal=deal)
        if score:
            rows.append({
                "entity_type": "deal",
                "severity": severity(score),
                "risk_score": score,
                "entity_id": deal.get("id") or "",
                "entity_name": deal.get("title") or "",
                "people_count": "",
                "shared_count": "",
                "email": person_primary_email(deal.get("person_id") if isinstance(deal.get("person_id"), dict) else {}),
                "deal_id": deal.get("id") or "",
                "reasons": ",".join(reasons),
            })

    rows.sort(key=lambda item: (-int(item["risk_score"]), str(item["entity_type"]), str(item["entity_name"])))
    write_reports("contamination_score", rows)


if __name__ == "__main__":
    main()
