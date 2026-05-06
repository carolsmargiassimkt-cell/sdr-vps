from __future__ import annotations

from collections import defaultdict

from common import all_orgs, org_persons, severity, write_reports
from core.crm_hygiene import contamination_score, email_domain, email_matches_company, person_primary_email


def main():
    orgs = {int(org.get("id") or 0): org for org in all_orgs()}
    person_seen: dict[int, dict] = {}
    person_orgs: dict[int, set[int]] = defaultdict(set)

    for org_id in list(orgs.keys()):
        if org_id <= 0:
            continue
        for person in org_persons(org_id):
            person_id = int(person.get("id") or 0)
            if person_id <= 0:
                continue
            person_seen.setdefault(person_id, person)
            person_orgs[person_id].add(org_id)

    rows = []
    for person_id, org_ids in person_orgs.items():
        if len(org_ids) <= 1:
            continue
        person = person_seen.get(person_id, {})
        org_names = [orgs.get(org_id, {}).get("name") or "" for org_id in sorted(org_ids)]
        email = person_primary_email(person)
        mismatch_count = sum(1 for org_id in org_ids if email and not email_matches_company(email, orgs.get(org_id, {}).get("name")))
        score, reasons = contamination_score(person=person, shared_org_count=len(org_ids))
        if mismatch_count:
            score = min(100, score + 20)
            reasons.append("person_email_mismatch_with_some_orgs")
        rows.append({
            "severity": severity(score),
            "risk_score": score,
            "person_id": person_id,
            "person_name": person.get("name") or "",
            "email": email,
            "email_domain": email_domain(email),
            "org_count": len(org_ids),
            "org_ids": ",".join(str(x) for x in sorted(org_ids)),
            "org_names": " | ".join(org_names[:20]),
            "mismatch_org_count": mismatch_count,
            "reasons": ",".join(reasons),
        })

    rows.sort(key=lambda item: (-int(item["risk_score"]), -int(item["org_count"]), str(item["person_name"])))
    write_reports("persons_suspeitas", rows)


if __name__ == "__main__":
    main()
