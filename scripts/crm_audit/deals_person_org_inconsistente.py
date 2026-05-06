from __future__ import annotations

from common import all_deals, severity, write_reports
from core.crm_hygiene import company_tokens, contamination_score, email_matches_company, person_primary_email


def main():
    rows = []
    for deal in all_deals():
        person = deal.get("person_id") if isinstance(deal.get("person_id"), dict) else {}
        org = deal.get("org_id") if isinstance(deal.get("org_id"), dict) else {}
        email = person_primary_email(person)
        title_tokens = company_tokens(deal.get("title"))
        org_tokens = company_tokens(org.get("name"))
        title_org_mismatch = bool(title_tokens and org_tokens and not title_tokens & org_tokens)
        email_org_mismatch = bool(email and org and not email_matches_company(email, org.get("name")))
        if not title_org_mismatch and not email_org_mismatch:
            continue
        score, reasons = contamination_score(deal=deal)
        rows.append({
            "severity": severity(score),
            "risk_score": score,
            "deal_id": deal.get("id") or "",
            "deal_title": deal.get("title") or "",
            "status": deal.get("status") or "",
            "pipeline_id": deal.get("pipeline_id") or "",
            "stage_id": deal.get("stage_id") or "",
            "person_id": person.get("value") or person.get("id") or "",
            "person_name": person.get("name") or "",
            "email": email,
            "org_id": org.get("value") or org.get("id") or "",
            "org_name": org.get("name") or "",
            "title_org_mismatch": int(title_org_mismatch),
            "email_org_mismatch": int(email_org_mismatch),
            "reasons": ",".join(reasons),
        })
    rows.sort(key=lambda item: (-int(item["risk_score"]), str(item["deal_title"])))
    write_reports("deals_person_org_inconsistente", rows)


if __name__ == "__main__":
    main()
