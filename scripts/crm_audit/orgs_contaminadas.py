from __future__ import annotations

from collections import defaultdict

from common import all_orgs, org_persons, severity, write_reports
from core.crm_hygiene import contamination_score, email_domain, email_matches_company, person_primary_email


def main():
    orgs = all_orgs()
    org_people: dict[int, list[dict]] = {}
    person_orgs: dict[int, set[int]] = defaultdict(set)

    for org in orgs:
        org_id = int(org.get("id") or 0)
        if org_id <= 0:
            continue
        people = org_persons(org_id)
        org_people[org_id] = people
        for person in people:
            person_id = int(person.get("id") or 0)
            if person_id > 0:
                person_orgs[person_id].add(org_id)

    rows = []
    for org in orgs:
        org_id = int(org.get("id") or 0)
        people = org_people.get(org_id, [])
        shared_people = [int(p.get("id") or 0) for p in people if len(person_orgs.get(int(p.get("id") or 0), set())) > 1]
        domains = sorted({email_domain(person_primary_email(p)) for p in people if email_domain(person_primary_email(p))})
        mismatch = [
            person_primary_email(p)
            for p in people
            if person_primary_email(p) and not email_matches_company(person_primary_email(p), org.get("name"))
        ]
        score, reasons = contamination_score(org=org, persons=people, shared_person_count=len(shared_people))
        if score >= 30 or len(people) >= 40 or shared_people or len(domains) >= 8:
            rows.append({
                "severity": severity(score),
                "risk_score": score,
                "org_id": org_id,
                "org_name": org.get("name") or "",
                "people_count": len(people),
                "shared_person_count": len(shared_people),
                "shared_person_ids_sample": ",".join(str(x) for x in shared_people[:25]),
                "domain_count": len(domains),
                "domains_sample": ",".join(domains[:25]),
                "email_domain_mismatch_count": len(mismatch),
                "email_domain_mismatch_sample": ",".join(mismatch[:25]),
                "reasons": ",".join(reasons),
            })

    rows.sort(key=lambda item: (-int(item["risk_score"]), -int(item["people_count"]), str(item["org_name"])))
    write_reports("orgs_contaminadas", rows)


if __name__ == "__main__":
    main()
