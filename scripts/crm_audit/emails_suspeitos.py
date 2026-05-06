from __future__ import annotations

from common import all_persons, severity, write_reports
from core.crm_hygiene import contamination_score, email_domain, is_generic_email, person_primary_email


def main():
    rows = []
    for person in all_persons():
        email = person_primary_email(person)
        if not is_generic_email(email):
            continue
        score, reasons = contamination_score(person=person)
        rows.append({
            "severity": severity(score),
            "risk_score": score,
            "person_id": person.get("id") or "",
            "person_name": person.get("name") or "",
            "email": email,
            "email_domain": email_domain(email),
            "org_id": (person.get("org_id") or {}).get("value") if isinstance(person.get("org_id"), dict) else person.get("org_id") or "",
            "org_name": (person.get("org_id") or {}).get("name") if isinstance(person.get("org_id"), dict) else "",
            "reasons": ",".join(reasons),
        })
    rows.sort(key=lambda item: (-int(item["risk_score"]), str(item["email"])))
    write_reports("emails_genericos", rows)


if __name__ == "__main__":
    main()
