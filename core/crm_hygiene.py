from __future__ import annotations

import re
from typing import Any


GENERIC_EMAIL_PREFIXES = {
    "adm",
    "admin",
    "administrativo",
    "atendimento",
    "cobranca",
    "comercial",
    "compras",
    "contabilidade",
    "contato",
    "dp",
    "financeiro",
    "fiscal",
    "juridico",
    "legalizacao",
    "lgpd",
    "nfe",
    "nf",
    "no-reply",
    "noreply",
    "ouvidoria",
    "paralegal",
    "postmaster",
    "rh",
    "sac",
    "suporte",
    "tributario",
}

FREE_EMAIL_DOMAINS = {
    "gmail.com",
    "hotmail.com",
    "outlook.com",
    "live.com",
    "icloud.com",
    "yahoo.com",
    "uol.com.br",
    "bol.com.br",
}

INTERNAL_DOMAINS = {"manddigital.com.br"}
HIGH_PEOPLE_COUNT = 40
CRITICAL_PEOPLE_COUNT = 80


def normalize_text(value: Any) -> str:
    text = str(value or "").strip().lower()
    replacements = {
        "á": "a",
        "à": "a",
        "ã": "a",
        "â": "a",
        "é": "e",
        "ê": "e",
        "í": "i",
        "ó": "o",
        "ô": "o",
        "õ": "o",
        "ú": "u",
        "ç": "c",
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    return re.sub(r"\s+", " ", text)


def email_value(raw: Any) -> str:
    if isinstance(raw, list):
        for item in raw:
            if isinstance(item, dict) and item.get("value"):
                return str(item.get("value") or "").strip().lower()
            if isinstance(item, str) and "@" in item:
                return item.strip().lower()
    if isinstance(raw, dict):
        return str(raw.get("value") or "").strip().lower()
    return str(raw or "").strip().lower()


def person_primary_email(person: dict[str, Any] | None) -> str:
    return email_value((person or {}).get("email"))


def email_domain(email: Any) -> str:
    value = email_value(email)
    if "@" not in value:
        return ""
    return value.rsplit("@", 1)[-1].strip().lower()


def email_local(email: Any) -> str:
    value = email_value(email)
    if "@" not in value:
        return ""
    return value.split("@", 1)[0].strip().lower()


def is_generic_email(email: Any) -> bool:
    value = email_value(email)
    if not value or "@" not in value:
        return True
    local = email_local(value)
    domain = email_domain(value)
    if domain in INTERNAL_DOMAINS:
        return True
    normalized = normalize_text(local)
    if normalized in GENERIC_EMAIL_PREFIXES:
        return True
    if any(normalized.startswith(prefix + ".") or normalized.startswith(prefix + "-") or normalized.startswith(prefix + "_") for prefix in GENERIC_EMAIL_PREFIXES):
        return True
    return False


def company_tokens(name: Any) -> set[str]:
    text = normalize_text(name)
    tokens = {
        token
        for token in re.split(r"[^a-z0-9]+", text)
        if len(token) >= 4 and token not in {"ltda", "eireli", "empresa", "grupo", "brasil", "comercio", "servicos"}
    }
    return tokens


def domain_tokens(domain: Any) -> set[str]:
    clean = str(domain or "").strip().lower()
    base = clean.split(".")[0] if clean else ""
    return {token for token in re.split(r"[^a-z0-9]+", normalize_text(base)) if len(token) >= 4}


def email_matches_company(email: Any, company_name: Any) -> bool:
    domain = email_domain(email)
    if not domain:
        return False
    if domain in FREE_EMAIL_DOMAINS or domain in INTERNAL_DOMAINS:
        return False
    c_tokens = company_tokens(company_name)
    d_tokens = domain_tokens(domain)
    if not c_tokens or not d_tokens:
        return False
    return bool(c_tokens & d_tokens)


def person_org_id(person: dict[str, Any] | None) -> int:
    raw = (person or {}).get("org_id")
    if isinstance(raw, dict):
        raw = raw.get("value") or raw.get("id")
    try:
        return int(raw or 0)
    except Exception:
        return 0


def detect_shared_person(person_id: Any, org_ids: list[Any] | set[Any] | tuple[Any, ...]) -> bool:
    clean_ids = {int(item) for item in org_ids if str(item or "").isdigit() and int(item) > 0}
    return int(person_id or 0) > 0 and len(clean_ids) > 1


def is_org_contaminated(org: dict[str, Any] | None, persons: list[dict[str, Any]] | None = None, shared_person_count: int = 0) -> bool:
    score, _reasons = contamination_score(org=org, persons=persons or [], shared_person_count=shared_person_count)
    return score >= 60


def is_safe_person_for_outbound(person: dict[str, Any] | None, org: dict[str, Any] | None = None, shared_org_count: int = 0) -> bool:
    email = person_primary_email(person)
    if not email or is_generic_email(email):
        return False
    if shared_org_count >= 2:
        return False
    if org and not email_matches_company(email, (org or {}).get("name")):
        domain = email_domain(email)
        if domain not in FREE_EMAIL_DOMAINS:
            return False
    return True


def is_safe_deal_for_outbound(deal: dict[str, Any] | None) -> bool:
    deal = deal or {}
    person = deal.get("person_id") if isinstance(deal.get("person_id"), dict) else {}
    org = deal.get("org_id") if isinstance(deal.get("org_id"), dict) else {}
    return is_safe_person_for_outbound(person, org=org)


def severity_from_score(score: int) -> str:
    if score >= 80:
        return "CRITICAL"
    if score >= 60:
        return "HIGH"
    if score >= 30:
        return "MEDIUM"
    return "LOW"


def contamination_score(
    *,
    org: dict[str, Any] | None = None,
    person: dict[str, Any] | None = None,
    deal: dict[str, Any] | None = None,
    persons: list[dict[str, Any]] | None = None,
    shared_person_count: int = 0,
    shared_org_count: int = 0,
) -> tuple[int, list[str]]:
    score = 0
    reasons: list[str] = []
    persons = persons or []

    people_count = len(persons)
    if people_count >= CRITICAL_PEOPLE_COUNT:
        score += 80
        reasons.append("org_people_count_critical")
    elif people_count >= HIGH_PEOPLE_COUNT:
        score += 45
        reasons.append("org_people_count_high")

    if shared_person_count >= 10:
        score += 80
        reasons.append("shared_persons_critical")
    elif shared_person_count >= 3:
        score += 50
        reasons.append("shared_persons_high")
    elif shared_person_count > 0:
        score += 25
        reasons.append("shared_persons")

    if shared_org_count >= 10:
        score += 80
        reasons.append("person_multiple_orgs_critical")
    elif shared_org_count >= 3:
        score += 50
        reasons.append("person_multiple_orgs_high")
    elif shared_org_count > 1:
        score += 25
        reasons.append("person_multiple_orgs")

    email = person_primary_email(person) if person else ""
    if deal and not email:
        embedded = deal.get("person_id") if isinstance(deal.get("person_id"), dict) else {}
        email = person_primary_email(embedded)
    if email:
        if is_generic_email(email):
            score += 30
            reasons.append("generic_email")
        target_org = org or (deal.get("org_id") if isinstance((deal or {}).get("org_id"), dict) else {})
        if target_org and not email_matches_company(email, target_org.get("name")):
            if email_domain(email) not in FREE_EMAIL_DOMAINS:
                score += 25
                reasons.append("email_domain_mismatch")
    elif person is not None or deal is not None:
        score += 15
        reasons.append("missing_email")

    if deal:
        title_tokens = company_tokens(deal.get("title"))
        org_name = ((deal.get("org_id") or {}).get("name") if isinstance(deal.get("org_id"), dict) else "")
        org_tokens = company_tokens(org_name)
        if title_tokens and org_tokens and not title_tokens & org_tokens:
            score += 20
            reasons.append("deal_org_title_mismatch")

    return min(100, score), reasons
