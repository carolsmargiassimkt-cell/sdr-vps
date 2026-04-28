from __future__ import annotations

import argparse
import json
import os
import re
import time
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import requests

from crm.pipedrive_client import PipedriveClient
from enrich_missing_cnpj_from_xlsx import normalize_cnpj, parse_xlsx


INPUT_XLSX = Path("/root/BASE_LEADS_COM_LINKEDIN.xlsx")
PEOPLE_INPUT_XLSX = Path("/root/people-25681240-20.xlsx")
REPORT_JSON = Path("/root/sdr-vps/logs/base_leads_linkedin_pipeline2_writeback_report.json")
OPP_CACHE_JSON = Path("/root/sdr-vps/runtime/base_leads_linkedin_opp_cache.json")
STATE_JSON = Path("/root/sdr-vps/runtime/base_leads_linkedin_pipeline2_state.json")

PIPELINE_ID = 2
PIPELINE_FETCH_LIMIT = 2000
PIPELINE_FETCH_PAGE_LIMIT = 100
DEFAULT_CRM_MIN_INTERVAL_SEC = 1.8
DEFAULT_OPP_MIN_INTERVAL_SEC = 3.2  # 20 req/min
TARGETED_NAME_FALLBACK_LIMIT_WITH_CNPJ = 0
TARGETED_NAME_FALLBACK_LIMIT_WITHOUT_CNPJ = 2
TARGETED_CONTACT_EMAIL_LIMIT = 1
TARGETED_CONTACT_PHONE_LIMIT = 1
TARGETED_FAST_SKIP_CONTACT_MISS = False
TARGETED_STRICT_NAME_FALLBACK_WITH_CNPJ = True
PREFIX_MERGE_MAX_SOURCE_TOKENS = 2
PREFIX_MERGE_MIN_TARGET_EXTRA_TOKENS = 3

DIRTY_PHONES = {
    "1804330972",
    "18043309723",
    "11999999999",
    "11999998888",
    "00999999977",
    "1155555555",
    "999999999",
}
POLLUTED_ORG_TOKENS = ("CEBRAC", "BRASIMAC", "PRECIOSO FRUTO", "JOSE DONIZETI", "RED DOOR")
SUPER_MINAS_LABEL_IDS = {"175", "162", "176", "177"}
SUPER_MINAS_LABEL_NAMES = {"SUPER_MINAS", "SUPER MINAS", "SUPERMINAS", "SUPER_MINAS_OPORTUNIDADE"}
CORPORATE_PERSON_TOKENS = {
    "administracao",
    "administradora",
    "alimentos",
    "atacado",
    "automacao",
    "automotiva",
    "brasil",
    "cia",
    "comercio",
    "comercial",
    "company",
    "distribuicao",
    "distribuidora",
    "eireli",
    "empresa",
    "exportacao",
    "farmaceutica",
    "grupo",
    "holding",
    "industria",
    "industrial",
    "investimentos",
    "ltda",
    "matriz",
    "sa",
    "servicos",
    "supermercado",
    "transportes",
}
NAME_FIXES = {
    "ALINTOS": "ALIMENTOS",
    "PAGANTOS": "PAGAMENTOS",
    "CORCIAL": "COMERCIAL",
}


def log(message: str) -> None:
    print(str(message), flush=True)


def normalize_text(value: Any) -> str:
    text = unicodedata.normalize("NFKD", str(value or "").strip().lower())
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return re.sub(r"[^a-z0-9]+", "", text)


def collapse_spaces(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def normalize_company_text(value: Any) -> str:
    text = collapse_spaces(value)
    text = re.sub(r"^lead\s+mand\s+digital\s*-\s*", "", text, flags=re.I)
    text = re.sub(r"^lead\s+mand\s+digital\s*", "", text, flags=re.I)
    text = re.sub(r"\s*-\s*lead\s*$", "", text, flags=re.I)
    text = re.sub(r"\s*-\s*deal\s*$", "", text, flags=re.I)
    text = re.sub(r"\s*-\s*neg[oó]cio\s*$", "", text, flags=re.I)
    text = re.sub(r"\bneg[oó]cio\b", " ", text, flags=re.I)
    text = collapse_spaces(text).strip(" -")
    for source, target in NAME_FIXES.items():
        text = re.sub(source, target, text, flags=re.I)
    return collapse_spaces(text)


def canonical_legal_name(*values: Any) -> str:
    for value in values:
        text = normalize_company_text(value)
        if text:
            return text.upper()
    return ""


def split_multi(value: Any) -> List[str]:
    text = str(value or "").strip()
    if not text:
        return []
    text = text.replace("\n", "|").replace("\r", "|")
    return [item.strip() for item in re.split(r"[|;,]+", text) if item.strip()]


def normalize_phone(value: Any) -> str:
    digits = re.sub(r"\D+", "", str(value or ""))
    if digits.startswith("55") and len(digits) > 11:
        digits = digits[2:]
    if len(digits) not in {10, 11}:
        return ""
    if len(digits) == 11 and digits[2] != "9":
        return ""
    if digits in DIRTY_PHONES:
        return ""
    if len(set(digits)) <= 2:
        return ""
    if "000000" in digits or "999999" in digits or "123456" in digits:
        return ""
    return digits


def normalize_email(value: Any) -> str:
    text = str(value or "").strip().lower()
    if "@" not in text:
        return ""
    local, _, domain = text.partition("@")
    if not local or "." not in domain:
        return ""
    if text.startswith(("http://", "https://")):
        return ""
    if text.endswith((".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp")):
        return ""
    return text


def collect_phones(*values: Any) -> List[str]:
    output: List[str] = []
    for value in values:
        raw_items = value if isinstance(value, list) else split_multi(value)
        if not raw_items:
            raw_items = [value]
        for raw in raw_items:
            phone = normalize_phone(raw)
            if phone and phone not in output:
                output.append(phone)
    return output


def collect_emails(*values: Any) -> List[str]:
    output: List[str] = []
    for value in values:
        raw_items = value if isinstance(value, list) else split_multi(value)
        if not raw_items:
            raw_items = [value]
        for raw in raw_items:
            email = normalize_email(raw)
            if email and email not in output:
                output.append(email)
    return output


def extract_id(value: Any) -> int:
    if isinstance(value, dict):
        try:
            return int(value.get("value") or value.get("id") or 0)
        except Exception:
            return 0
    try:
        return int(value or 0)
    except Exception:
        return 0


def polluted_name(value: Any) -> bool:
    upper = str(value or "").strip().upper()
    return any(token in upper for token in POLLUTED_ORG_TOKENS)


def person_display_name(value: Any) -> str:
    text = collapse_spaces(value)
    return text[:120]


def normalized_word_tokens(value: Any) -> List[str]:
    text = unicodedata.normalize("NFKD", str(value or "").strip().lower())
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return [token for token in re.split(r"[^a-z0-9]+", text) if token]


def looks_company_like_person_name(value: Any, org_aliases: Optional[Iterable[str]] = None) -> bool:
    clean = person_display_name(value)
    alias = normalize_text(clean)
    normalized_org_aliases = {str(item).strip() for item in list(org_aliases or []) if str(item).strip()}
    if not clean or not alias:
        return True
    if alias in normalized_org_aliases:
        return True
    tokens = normalized_word_tokens(clean)
    return any(token in CORPORATE_PERSON_TOKENS for token in tokens)


def choose_best_person_name(candidates: Iterable[Any], *, org_aliases: Optional[Iterable[str]] = None) -> str:
    normalized_org_aliases = {str(item).strip() for item in list(org_aliases or []) if str(item).strip()}
    best_name = ""
    best_key: Optional[Tuple[int, int, str]] = None
    for candidate in list(candidates or []):
        clean = person_display_name(candidate)
        alias = normalize_text(clean)
        if not clean or not alias:
            continue
        score = 0
        if alias not in normalized_org_aliases:
            score += 8
        if not looks_company_like_person_name(clean, normalized_org_aliases):
            score += 5
        score += min(name_token_count(clean), 4)
        key = (score, -len(clean), clean.lower())
        if best_key is None or key > best_key:
            best_key = key
            best_name = clean
    return best_name


def choose_group_person_name(sheet_group: Dict[str, Any], *, verified_only: bool = False) -> str:
    org_aliases = {str(item).strip() for item in list(sheet_group.get("aliases") or []) if str(item).strip()}
    canonical_alias = normalize_text(sheet_group.get("canonical_name"))
    if canonical_alias:
        org_aliases.add(canonical_alias)
    if verified_only:
        return choose_best_person_name(sheet_group.get("people_person_names") or [], org_aliases=org_aliases)
    return choose_best_person_name(
        list(sheet_group.get("people_person_names") or []) + list(sheet_group.get("person_names") or []),
        org_aliases=org_aliases,
    )


def company_aliases(*values: Any) -> Set[str]:
    aliases: Set[str] = set()
    for value in values:
        text = normalize_company_text(value)
        key = normalize_text(text)
        if key:
            aliases.add(key)
    return aliases


def deal_label_tokens(raw_label: Any) -> Tuple[Set[str], Set[str]]:
    raw_items = raw_label if isinstance(raw_label, list) else [raw_label]
    tokens: Set[str] = set()
    raw_ids: Set[str] = set()
    for item in raw_items:
        candidates: List[Any]
        if isinstance(item, dict):
            candidates = [item.get("id"), item.get("label"), item.get("name"), item.get("value"), item.get("text")]
        else:
            candidates = [item]
        for candidate in candidates:
            clean = str(candidate or "").strip()
            if not clean:
                continue
            tokens.add(clean.upper().replace("-", "_").replace(" ", "_"))
            if clean.isdigit():
                raw_ids.add(clean)
    return tokens, raw_ids


def is_super_minas_deal(deal: Dict[str, Any]) -> bool:
    tokens, raw_ids = deal_label_tokens((deal or {}).get("label"))
    normalized_super = {str(item).strip().upper().replace("-", "_").replace(" ", "_") for item in SUPER_MINAS_LABEL_NAMES}
    if tokens & normalized_super:
        return True
    return bool(raw_ids & SUPER_MINAS_LABEL_IDS)


def sorted_unique(values: Iterable[str]) -> List[str]:
    return sorted({str(value).strip() for value in values if str(value).strip()})


def name_token_count(value: Any) -> int:
    return len([token for token in re.split(r"[^a-z0-9]+", str(value or "").lower()) if token])


def group_signal_score(group: Dict[str, Any]) -> int:
    score = 0
    score += len(list(group.get("phones") or [])) * 5
    score += len(list(group.get("emails") or [])) * 5
    score += len(list(group.get("person_names") or [])) * 4
    score += len(list(group.get("statuses") or [])) * 3
    score += int(group.get("rows") or 0)
    score += min(name_token_count(group.get("canonical_name")), 6)
    return score


def group_has_matching_signal(group: Dict[str, Any]) -> bool:
    return bool(
        list(group.get("phones") or [])
        or list(group.get("emails") or [])
        or list(group.get("person_names") or [])
        or list(group.get("statuses") or [])
    )


def rebuild_alias_index(groups: Dict[str, Dict[str, Any]]) -> Dict[str, Set[str]]:
    alias_index: Dict[str, Set[str]] = defaultdict(set)
    for group_key, group in groups.items():
        for alias in list(group.get("aliases") or []):
            clean = str(alias or "").strip()
            if clean:
                alias_index[clean].add(group_key)
    return alias_index


def merge_prefix_duplicate_sheet_groups(groups: Dict[str, Dict[str, Any]]) -> Tuple[Dict[str, Dict[str, Any]], List[Dict[str, Any]]]:
    working = {key: dict(value or {}) for key, value in dict(groups or {}).items()}
    merge_rows: List[Dict[str, Any]] = []
    while True:
        merge_pair: Optional[Tuple[str, str]] = None
        for source_key, source in sorted(working.items()):
            source_name = normalize_company_text(source.get("canonical_name"))
            source_alias = normalize_text(source_name)
            source_tokens = name_token_count(source_name)
            if not source_alias or source_tokens <= 0:
                continue
            if source_tokens > PREFIX_MERGE_MAX_SOURCE_TOKENS:
                continue
            if group_has_matching_signal(source):
                continue
            if int(source.get("rows") or 0) > 1:
                continue

            candidates: List[Tuple[int, str]] = []
            for target_key, target in working.items():
                if target_key == source_key:
                    continue
                target_name = normalize_company_text(target.get("canonical_name"))
                target_alias = normalize_text(target_name)
                if not target_alias or target_alias == source_alias:
                    continue
                if not target_alias.startswith(source_alias):
                    continue
                target_tokens = name_token_count(target_name)
                if target_tokens < source_tokens + PREFIX_MERGE_MIN_TARGET_EXTRA_TOKENS:
                    continue
                if not group_has_matching_signal(target):
                    continue
                candidates.append((group_signal_score(target), target_key))
            candidates.sort(reverse=True)
            if len(candidates) == 1:
                merge_pair = (source_key, candidates[0][1])
                break
        if not merge_pair:
            break

        source_key, target_key = merge_pair
        source = working.pop(source_key)
        target = working[target_key]

        target.setdefault("merged_group_keys", set()).add(source_key)
        target.setdefault("merged_cnpjs", set())
        if normalize_cnpj(source.get("cnpj")):
            target["merged_cnpjs"].add(normalize_cnpj(source.get("cnpj")))
        if normalize_cnpj(target.get("cnpj")):
            target["merged_cnpjs"].add(normalize_cnpj(target.get("cnpj")))
        target["aliases"].update(source.get("aliases") or set())
        source_name = collapse_spaces(source.get("canonical_name"))
        if source_name:
            target["aliases"].add(normalize_text(source_name))
            target["org_names"].add(source_name)
        target["org_names"].update(source.get("org_names") or set())
        target["deal_titles"].update(source.get("deal_titles") or set())
        target["person_names"].update(source.get("person_names") or set())
        target.setdefault("base_person_names", set()).update(source.get("base_person_names") or set())
        target.setdefault("people_person_names", set()).update(source.get("people_person_names") or set())
        target.setdefault("people_org_names", set()).update(source.get("people_org_names") or set())
        target["phones"].update(source.get("phones") or set())
        target["emails"].update(source.get("emails") or set())
        target.setdefault("source_phones", set()).update(source.get("source_phones") or set())
        target.setdefault("source_emails", set()).update(source.get("source_emails") or set())
        target["statuses"].update(source.get("statuses") or set())
        target["rows"] = int(target.get("rows") or 0) + int(source.get("rows") or 0)
        target["people_rows"] = int(target.get("people_rows") or 0) + int(source.get("people_rows") or 0)
        merge_rows.append(
            {
                "source_group_key": source_key,
                "source_cnpj": source.get("cnpj"),
                "source_name": source.get("canonical_name"),
                "target_group_key": target_key,
                "target_cnpj": target.get("cnpj"),
                "target_name": target.get("canonical_name"),
            }
        )
    return working, merge_rows


def iter_targeted_search_names(sheet_group: Dict[str, Any], *, target_cnpj: str = "") -> List[str]:
    output: List[str] = []
    seen: Set[str] = set()
    candidates = [
        sheet_group.get("canonical_name"),
        sheet_group.get("opp_name"),
        *list(sheet_group.get("org_names") or []),
        *list(sheet_group.get("deal_titles") or []),
    ]
    for value in candidates:
        clean = collapse_spaces(value)
        key = normalize_text(clean)
        if not clean or not key or key in seen:
            continue
        seen.add(key)
        output.append(clean)
    limit = TARGETED_NAME_FALLBACK_LIMIT_WITH_CNPJ if target_cnpj else TARGETED_NAME_FALLBACK_LIMIT_WITHOUT_CNPJ
    return output[: max(0, int(limit or 0))]


def search_organizations_cached(
    client: PipedriveClient,
    cache: Dict[Tuple[str, str], List[Dict[str, Any]]],
    term: str,
    *,
    field_key: Optional[str] = None,
) -> List[Dict[str, Any]]:
    clean = collapse_spaces(term)
    if not clean:
        return []
    cache_key = (str(field_key or ""), clean.lower())
    if cache_key not in cache:
        cache[cache_key] = [dict(item) for item in list(client.search_organizations(clean, field_key=field_key) or []) if isinstance(item, dict)]
    return [dict(item) for item in list(cache.get(cache_key) or []) if isinstance(item, dict)]


def search_people_cached(
    client: PipedriveClient,
    cache: Dict[str, List[Dict[str, Any]]],
    term: str,
) -> List[Dict[str, Any]]:
    clean = str(term or "").strip().lower()
    if not clean:
        return []
    if clean not in cache:
        cache[clean] = [dict(item) for item in list(client.find_person_by_term(clean) or []) if isinstance(item, dict)]
    return [dict(item) for item in list(cache.get(clean) or []) if isinstance(item, dict)]


def load_state(path: Path = STATE_JSON) -> Dict[str, Any]:
    if not path.exists():
        return {"done": [], "counters": {}, "results": []}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"done": [], "counters": {}, "results": []}
    return payload if isinstance(payload, dict) else {"done": [], "counters": {}, "results": []}


def save_state(state: Dict[str, Any], path: Path = STATE_JSON) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


class RateLimiter:
    def __init__(self, min_interval_sec: float) -> None:
        self.min_interval_sec = max(0.0, float(min_interval_sec or 0.0))
        self._last_at = 0.0

    def wait(self) -> None:
        if self.min_interval_sec <= 0:
            return
        now = time.time()
        elapsed = now - float(self._last_at or 0.0)
        if elapsed < self.min_interval_sec:
            time.sleep(self.min_interval_sec - elapsed)
        self._last_at = time.time()


class OportunidadesClient:
    BASE_URL = "https://app.oportunidados.com.br/api/v1/brazilian_companies/{cnpj}/company"

    def __init__(self, token: str = "", *, cache_path: Path = OPP_CACHE_JSON, min_interval_sec: float = DEFAULT_OPP_MIN_INTERVAL_SEC) -> None:
        self.token = str(token or "").strip()
        self.cache_path = Path(cache_path)
        self.rate_limiter = RateLimiter(min_interval_sec)
        self.cache = self._load_cache()

    def _load_cache(self) -> Dict[str, Any]:
        if not self.cache_path.exists():
            return {}
        try:
            payload = json.loads(self.cache_path.read_text(encoding="utf-8"))
        except Exception:
            return {}
        return payload if isinstance(payload, dict) else {}

    def _save_cache(self) -> None:
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        self.cache_path.write_text(json.dumps(self.cache, ensure_ascii=False, indent=2), encoding="utf-8")

    def fetch_company(self, cnpj: str) -> Dict[str, Any]:
        clean = normalize_cnpj(cnpj)
        if not clean or not self.token:
            return {}
        cached = self.cache.get(clean)
        if isinstance(cached, dict) and int(cached.get("status_code") or 0) in {200, 404}:
            payload = dict(cached)
            payload["_from_cache"] = True
            return payload
        self.rate_limiter.wait()
        try:
            response = requests.get(
                self.BASE_URL.format(cnpj=clean),
                headers={"Authorization": f"Bearer {self.token}", "Accept": "application/json"},
                timeout=30,
            )
            payload = {"status_code": int(response.status_code or 0), "_from_cache": False}
            if int(response.status_code or 0) == 200:
                payload["raw"] = response.json()
            else:
                payload["error"] = str(response.text or "")[:400]
        except Exception as exc:
            payload = {"status_code": 0, "error": str(exc)[:400], "_from_cache": False}
        self.cache[clean] = payload
        if not bool(payload.get("_from_cache")):
            self._save_cache()
        return payload

    @staticmethod
    def canonical_name(payload: Dict[str, Any]) -> str:
        raw = payload.get("raw") if isinstance(payload.get("raw"), dict) else {}
        company = raw.get("company") if isinstance(raw.get("company"), dict) else {}
        return canonical_legal_name(company.get("razao_social"), company.get("nome_fantasia"))


def enrich_sheet_groups_from_people_rows(
    groups: Dict[str, Dict[str, Any]],
    alias_index: Dict[str, Set[str]],
    people_rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    stats: Counter[str] = Counter()
    ambiguous_rows: List[Dict[str, Any]] = []
    unmatched_rows_sample: List[str] = []
    matched_groups: Set[str] = set()
    groups_with_contacts: Set[str] = set()

    for row in list(people_rows or []):
        org_name = collapse_spaces(row.get("Pessoa - Organização"))
        aliases = company_aliases(org_name)
        if not aliases:
            stats["rows_ignored_sem_org"] += 1
            continue
        candidate_keys: Set[str] = set()
        for alias in aliases:
            candidate_keys.update(alias_index.get(alias) or set())
        if not candidate_keys:
            stats["rows_unmatched"] += 1
            if org_name and len(unmatched_rows_sample) < 25:
                unmatched_rows_sample.append(org_name)
            continue
        if len(candidate_keys) > 1:
            stats["rows_ambiguous"] += 1
            if len(ambiguous_rows) < 25:
                ambiguous_rows.append({"org_name": org_name, "candidate_groups": sorted(candidate_keys)})
            continue

        group_key = next(iter(candidate_keys))
        group = groups.get(group_key) or {}
        if not group:
            continue

        stats["rows_matched_unique_group"] += 1
        matched_groups.add(group_key)
        group["people_rows"] = int(group.get("people_rows") or 0) + 1
        group["aliases"].update(aliases)
        if org_name:
            group["people_org_names"].add(org_name)

        person_name = person_display_name(row.get("Pessoa - Nome"))
        if person_name:
            group["person_names"].add(person_name)
            group["people_person_names"].add(person_name)

        phones = collect_phones(
            row.get("Pessoa - Telefone - Trabalho"),
            row.get("Pessoa - Telefone - Residencial"),
            row.get("Pessoa - Telefone - Celular"),
            row.get("Pessoa - Telefone - Outros"),
        )
        emails = collect_emails(
            row.get("Pessoa - E-mail - Trabalho"),
            row.get("Pessoa - E-mail - Residencial"),
            row.get("Pessoa - E-mail - Outros"),
        )
        if phones or emails:
            groups_with_contacts.add(group_key)
        group["phones"].update(phones)
        group["emails"].update(emails)
        group["source_phones"].update(phones)
        group["source_emails"].update(emails)

    return {
        "rows_total": len(list(people_rows or [])),
        "rows_matched_unique_group": int(stats["rows_matched_unique_group"]),
        "rows_unmatched": int(stats["rows_unmatched"]),
        "rows_ambiguous": int(stats["rows_ambiguous"]),
        "rows_ignored_sem_org": int(stats["rows_ignored_sem_org"]),
        "matched_groups": len(matched_groups),
        "groups_with_contacts": len(groups_with_contacts),
        "ambiguous_rows_sample": ambiguous_rows,
        "unmatched_rows_sample": unmatched_rows_sample,
    }


def build_sheet_groups(
    rows: List[Dict[str, Any]],
    people_rows: Optional[List[Dict[str, Any]]] = None,
    opp_client: Optional[OportunidadesClient] = None,
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Set[str]], Dict[str, Any]]:
    groups: Dict[str, Dict[str, Any]] = {}
    sheet_stats: Counter[str] = Counter()

    for row in list(rows or []):
        org_name = collapse_spaces(row.get("Organização - Nome"))
        deal_title = collapse_spaces(row.get("Negócio - Título"))
        api_razao = collapse_spaces(row.get("API_RAZAO"))
        cnpj = normalize_cnpj(row.get("cnpj_formatado"))
        canonical_name = canonical_legal_name(api_razao, org_name, deal_title)
        aliases = company_aliases(api_razao, org_name, deal_title)
        if not cnpj and not aliases:
            sheet_stats["rows_ignored_sem_chave"] += 1
            continue
        group_key = f"cnpj:{cnpj}" if cnpj else f"name:{sorted(aliases)[0]}"
        group = groups.setdefault(
            group_key,
            {
                "group_key": group_key,
                "cnpj": cnpj,
                "canonical_name": "",
                "opp_name": "",
                "org_names": set(),
                "deal_titles": set(),
                "person_names": set(),
                "base_person_names": set(),
                "people_person_names": set(),
                "people_org_names": set(),
                "aliases": set(),
                "phones": set(),
                "emails": set(),
                "source_phones": set(),
                "source_emails": set(),
                "statuses": set(),
                "rows": 0,
                "people_rows": 0,
            },
        )
        group["rows"] += 1
        group["aliases"].update(aliases)
        if org_name:
            group["org_names"].add(org_name)
        if deal_title:
            group["deal_titles"].add(deal_title)
        person_name = person_display_name(row.get("Pessoa - Nome"))
        if person_name:
            group["person_names"].add(person_name)
            group["base_person_names"].add(person_name)
        base_phones = collect_phones(
            row.get("Pessoa - Telefone - Trabalho"),
            row.get("Pessoa - Telefone - Celular"),
            row.get("API_TEL_OFICIAL"),
        )
        base_emails = collect_emails(row.get("API_EMAIL"))
        group["phones"].update(base_phones)
        group["emails"].update(base_emails)
        group["source_phones"].update(base_phones)
        group["source_emails"].update(base_emails)
        status_value = collapse_spaces(row.get("API_SITUACAO"))
        if status_value:
            group["statuses"].add(status_value)
        if api_razao:
            group["canonical_name"] = canonical_legal_name(api_razao)
        elif not group["canonical_name"] and canonical_name:
            group["canonical_name"] = canonical_name
    if opp_client is not None:
        for group in groups.values():
            cnpj = normalize_cnpj(group.get("cnpj"))
            if not cnpj:
                continue
            payload = opp_client.fetch_company(cnpj)
            if int(payload.get("status_code") or 0) != 200:
                continue
            opp_name = opp_client.canonical_name(payload)
            if opp_name:
                group["opp_name"] = opp_name
                if not group.get("canonical_name"):
                    group["canonical_name"] = opp_name
                group["aliases"].add(normalize_text(opp_name))

    groups, prefix_merge_rows = merge_prefix_duplicate_sheet_groups(groups)
    alias_index = rebuild_alias_index(groups)
    people_summary = enrich_sheet_groups_from_people_rows(groups, alias_index, list(people_rows or []))
    alias_index = rebuild_alias_index(groups)

    phone_owners: Dict[str, Set[str]] = defaultdict(set)
    email_owners: Dict[str, Set[str]] = defaultdict(set)
    source_phone_owners: Dict[str, Set[str]] = defaultdict(set)
    source_email_owners: Dict[str, Set[str]] = defaultdict(set)
    for group in groups.values():
        for phone in group["phones"]:
            phone_owners[phone].add(group["group_key"])
        for email in group["emails"]:
            email_owners[email].add(group["group_key"])
        for phone in group["source_phones"]:
            source_phone_owners[phone].add(group["group_key"])
        for email in group["source_emails"]:
            source_email_owners[email].add(group["group_key"])

    duplicate_cnpj_groups: List[Dict[str, Any]] = []
    duplicate_name_groups: List[Dict[str, Any]] = []
    for group in groups.values():
        key = str(group.get("group_key") or "")
        group["phones"] = sorted_unique(group["phones"])
        group["emails"] = sorted_unique(group["emails"])
        group["org_names"] = sorted_unique(group["org_names"])
        group["deal_titles"] = sorted_unique(group["deal_titles"])
        group["person_names"] = sorted_unique(group["person_names"])
        group["base_person_names"] = sorted_unique(group["base_person_names"])
        group["people_person_names"] = sorted_unique(group["people_person_names"])
        group["people_org_names"] = sorted_unique(group["people_org_names"])
        group["statuses"] = sorted_unique(group["statuses"])
        group["aliases"] = sorted_unique(group["aliases"])
        group["source_phones"] = sorted_unique(group["source_phones"])
        group["source_emails"] = sorted_unique(group["source_emails"])
        group["exclusive_phones"] = [phone for phone in group["phones"] if phone_owners.get(phone) == {key}]
        group["exclusive_emails"] = [email for email in group["emails"] if email_owners.get(email) == {key}]
        group["exclusive_source_phones"] = [phone for phone in group["source_phones"] if source_phone_owners.get(phone) == {key}]
        group["exclusive_source_emails"] = [email for email in group["source_emails"] if source_email_owners.get(email) == {key}]
        group["preferred_person_name"] = choose_group_person_name(group)
        group["verified_person_name"] = choose_group_person_name(group, verified_only=True)
        if key.startswith("cnpj:") and int(group.get("rows") or 0) > 1:
            duplicate_cnpj_groups.append(
                {
                    "group_key": key,
                    "cnpj": group.get("cnpj"),
                    "rows": int(group.get("rows") or 0),
                    "canonical_name": group.get("canonical_name"),
                }
            )
        if key.startswith("name:") and int(group.get("rows") or 0) > 1:
            duplicate_name_groups.append(
                {
                    "group_key": key,
                    "rows": int(group.get("rows") or 0),
                    "canonical_name": group.get("canonical_name"),
                }
            )

    sheet_summary = {
        "rows_total": len(rows),
        "groups_total": len(groups),
        "groups_with_cnpj": sum(1 for item in groups.values() if normalize_cnpj(item.get("cnpj"))),
        "groups_with_api_razao": sum(1 for item in groups.values() if str(item.get("canonical_name") or "").strip()),
        "exclusive_phones_total": sum(len(item.get("exclusive_phones") or []) for item in groups.values()),
        "exclusive_emails_total": sum(len(item.get("exclusive_emails") or []) for item in groups.values()),
        "exclusive_source_phones_total": sum(len(item.get("exclusive_source_phones") or []) for item in groups.values()),
        "exclusive_source_emails_total": sum(len(item.get("exclusive_source_emails") or []) for item in groups.values()),
        "groups_with_people_rows": sum(1 for item in groups.values() if int(item.get("people_rows") or 0) > 0),
        "duplicate_cnpj_groups": duplicate_cnpj_groups,
        "duplicate_name_groups": duplicate_name_groups,
        "prefix_merged_groups": prefix_merge_rows,
        "people_summary": people_summary,
    }
    return groups, alias_index, sheet_summary


def get_org_cached(client: PipedriveClient, cache: Dict[int, Dict[str, Any]], org_id: int) -> Dict[str, Any]:
    if int(org_id or 0) <= 0:
        return {}
    if int(org_id) in cache:
        return dict(cache[int(org_id)] or {})
    org = client.get_organization(int(org_id)) or {}
    cache[int(org_id)] = dict(org or {})
    return dict(org or {})


def get_person_cached(client: PipedriveClient, cache: Dict[int, Dict[str, Any]], person_id: int) -> Dict[str, Any]:
    if int(person_id or 0) <= 0:
        return {}
    if int(person_id) in cache:
        return dict(cache[int(person_id)] or {})
    person = client.get_person_details(int(person_id)) or {}
    cache[int(person_id)] = dict(person or {})
    return dict(person or {})


def get_org_people_cached(client: PipedriveClient, cache: Dict[int, List[Dict[str, Any]]], org_id: int) -> List[Dict[str, Any]]:
    if int(org_id or 0) <= 0:
        return []
    if int(org_id) in cache:
        return [dict(item) for item in list(cache[int(org_id)] or []) if isinstance(item, dict)]
    people = client.get_organization_persons(int(org_id), limit=100) or []
    normalized = [dict(item) for item in list(people or []) if isinstance(item, dict)]
    cache[int(org_id)] = normalized
    return [dict(item) for item in normalized]


def fetch_pipeline_deals_raw(
    client: PipedriveClient,
    *,
    deal_status: str = "open",
    total_limit: int = PIPELINE_FETCH_LIMIT,
    page_limit: int = PIPELINE_FETCH_PAGE_LIMIT,
    min_interval_sec: float = DEFAULT_CRM_MIN_INTERVAL_SEC,
    max_backoff_sec: int = 300,
) -> List[Dict[str, Any]]:
    api_token = str(getattr(client, "api_token", "") or getattr(client, "token", "") or "").strip()
    if not api_token:
        return []
    url = "https://api.pipedrive.com/v1/deals"
    limiter = RateLimiter(min_interval_sec)
    deals: List[Dict[str, Any]] = []
    seen_ids: Set[int] = set()
    start = 0
    backoff_sec = 65
    while len(deals) < max(1, int(total_limit or PIPELINE_FETCH_LIMIT)):
        limiter.wait()
        params = {
            "api_token": api_token,
            "status": str(deal_status or "open"),
            "pipeline_id": int(PIPELINE_ID),
            "limit": max(1, min(int(page_limit or PIPELINE_FETCH_PAGE_LIMIT), 100)),
            "start": int(start),
            "sort": "id ASC",
        }
        try:
            response = requests.get(url, params=params, timeout=30)
        except Exception as exc:
            client.last_http_status = 0
            client.last_http_body = str(exc)
            log(f"[PIPELINE_FETCH_ERROR] start={start} erro={str(exc)[:180]}")
            break
        client.last_http_status = int(response.status_code or 0)
        client.last_http_body = str(response.text or "")
        if int(response.status_code or 0) == 429:
            log(f"[PIPELINE_FETCH_RATE_LIMIT] start={start} backoff={backoff_sec}s")
            time.sleep(backoff_sec)
            backoff_sec = min(int(backoff_sec * 1.5), int(max_backoff_sec or 300))
            continue
        backoff_sec = 65
        if int(response.status_code or 0) == 401:
            log("[PIPELINE_FETCH_UNAUTHORIZED]")
            break
        if int(response.status_code or 0) != 200:
            log(f"[PIPELINE_FETCH_HTTP] start={start} status={int(response.status_code or 0)}")
            break
        try:
            payload = response.json() if response.content else {}
        except Exception:
            payload = {}
        chunk = list((payload or {}).get("data") or [])
        if not chunk:
            break
        added = 0
        for deal in chunk:
            if not isinstance(deal, dict):
                continue
            deal_id = int(deal.get("id") or 0)
            if deal_id <= 0 or deal_id in seen_ids:
                continue
            seen_ids.add(deal_id)
            deals.append(dict(deal))
            added += 1
            if len(deals) >= int(total_limit or PIPELINE_FETCH_LIMIT):
                break
        log(f"[PIPELINE_FETCH_OK] start={start} recebidos={len(chunk)} acumulado={len(deals)}")
        pagination = ((payload or {}).get("additional_data") or {}).get("pagination") or {}
        if len(deals) >= int(total_limit or PIPELINE_FETCH_LIMIT):
            break
        if not bool(pagination.get("more_items_in_collection")):
            break
        next_start = pagination.get("next_start")
        start = int(next_start or (int(start) + int(params["limit"])))
        if added <= 0:
            break
    return deals


def person_phones(person: Dict[str, Any]) -> List[str]:
    output: List[str] = []
    for item in list((person or {}).get("phone") or []):
        value = item.get("value") if isinstance(item, dict) else item
        phone = normalize_phone(value)
        if phone and phone not in output:
            output.append(phone)
    return output


def person_emails(person: Dict[str, Any]) -> List[str]:
    output: List[str] = []
    for item in list((person or {}).get("email") or []):
        value = item.get("value") if isinstance(item, dict) else item
        email = normalize_email(value)
        if email and email not in output:
            output.append(email)
    return output


def select_master_deal(deals: List[Dict[str, Any]]) -> Dict[str, Any]:
    ordered = sorted(
        list(deals or []),
        key=lambda item: (str((item or {}).get("add_time") or "9999-12-31 23:59:59"), int((item or {}).get("id") or 0)),
    )
    return dict(ordered[0] or {}) if ordered else {}


def choose_sheet_match(cnpj: str, aliases: Iterable[str], sheet_groups: Dict[str, Dict[str, Any]], alias_index: Dict[str, Set[str]]) -> Optional[str]:
    clean_cnpj = normalize_cnpj(cnpj)
    if clean_cnpj and f"cnpj:{clean_cnpj}" in sheet_groups:
        return f"cnpj:{clean_cnpj}"
    candidates: Set[str] = set()
    for alias in list(aliases or []):
        if alias:
            candidates.update(alias_index.get(alias) or set())
    if len(candidates) == 1:
        return next(iter(candidates))
    return None


def choose_target_org(
    group: Dict[str, Any],
    client: PipedriveClient,
    org_cache: Dict[int, Dict[str, Any]],
    canonical_name: str,
    canonical_cnpj: str,
) -> Dict[str, Any]:
    candidates: List[Tuple[int, int, Dict[str, Any]]] = []
    master_org_id = extract_id((group.get("master") or {}).get("org_id"))
    for org_id in sorted(int(item) for item in list(group.get("org_ids") or []) if int(item or 0) > 0):
        org = get_org_cached(client, org_cache, org_id)
        score = 0
        org_cnpj = normalize_cnpj(client.extract_cnpj(org))
        org_name = str(org.get("name") or "").strip()
        if canonical_cnpj and org_cnpj == canonical_cnpj:
            score += 8
        if canonical_name and normalize_text(org_name) == normalize_text(canonical_name):
            score += 5
        if org_id == master_org_id:
            score += 3
        if org_name and not polluted_name(org_name):
            score += 1
        candidates.append((-score, org_id, org))
    if not candidates and master_org_id:
        return get_org_cached(client, org_cache, master_org_id)
    candidates.sort(key=lambda item: (item[0], item[1]))
    return dict(candidates[0][2] or {}) if candidates else {}


def choose_primary_person(
    client: PipedriveClient,
    person_cache: Dict[int, Dict[str, Any]],
    group: Dict[str, Any],
    target_org_id: int,
    target_person_name: str,
    target_phones: List[str],
    target_emails: List[str],
) -> Dict[str, Any]:
    if int(target_org_id or 0) <= 0:
        return {}
    normalized_target_phones = {normalize_phone(phone) for phone in list(target_phones or []) if normalize_phone(phone)}
    normalized_target_emails = {normalize_email(email) for email in list(target_emails or []) if normalize_email(email)}
    normalized_target_name = normalize_text(target_person_name)
    if not normalized_target_phones and not normalized_target_emails and not normalized_target_name:
        return {}

    for person in list(group.get("org_people") or []):
        if extract_id(person.get("org_id")) != int(target_org_id):
            continue
        if normalized_target_phones and normalized_target_phones & set(person_phones(person)):
            return dict(person)
    for person in list(group.get("org_people") or []):
        if extract_id(person.get("org_id")) != int(target_org_id):
            continue
        if normalized_target_emails and normalized_target_emails & set(person_emails(person)):
            return dict(person)

    master_person_id = extract_id((group.get("master") or {}).get("person_id"))
    master_person = get_person_cached(client, person_cache, master_person_id) if master_person_id else {}
    if master_person and extract_id(master_person.get("org_id")) == int(target_org_id):
        if normalized_target_phones and normalized_target_phones & set(person_phones(master_person)):
            return master_person
        if normalized_target_emails and normalized_target_emails & set(person_emails(master_person)):
            return master_person
        if normalized_target_name and normalize_text(master_person.get("name")) == normalized_target_name:
            return master_person

    for person in list(group.get("org_people") or []):
        if extract_id(person.get("org_id")) != int(target_org_id):
            continue
        if normalized_target_name and normalize_text(person.get("name")) == normalized_target_name:
            return dict(person)

    for phone in list(target_phones or []):
        found = client.find_person(org_id=target_org_id, phone=phone) or {}
        found_id = extract_id(found.get("id"))
        if found_id:
            return get_person_cached(client, person_cache, found_id)
    for email in list(target_emails or []):
        found = client.find_person(org_id=target_org_id, email=email) or {}
        found_id = extract_id(found.get("id"))
        if found_id:
            return get_person_cached(client, person_cache, found_id)
    return {}


def hydrate_live_group_people(
    client: PipedriveClient,
    group: Dict[str, Any],
    *,
    org_people_cache: Dict[int, List[Dict[str, Any]]],
    person_cache: Dict[int, Dict[str, Any]],
) -> None:
    if bool(group.get("_people_loaded")):
        return
    group.setdefault("org_people", [])
    group.setdefault("live_phones", set())
    group.setdefault("live_emails", set())
    seen_person_ids: Set[int] = {
        extract_id(item.get("id"))
        for item in list(group.get("org_people") or [])
        if isinstance(item, dict) and extract_id(item.get("id")) > 0
    }
    for org_id in sorted(int(item) for item in list(group.get("org_ids") or []) if int(item or 0) > 0):
        for person in get_org_people_cached(client, org_people_cache, org_id):
            person_id = extract_id(person.get("id"))
            if person_id > 0 and person_id in seen_person_ids:
                continue
            if person_id > 0:
                seen_person_ids.add(person_id)
                person_cache[person_id] = dict(person)
            group["org_people"].append(dict(person))
            for phone in person_phones(person):
                group["live_phones"].add(phone)
            for email in person_emails(person):
                group["live_emails"].add(email)
    group["_people_loaded"] = True


def build_targeted_live_index(
    client: PipedriveClient,
    sheet_groups: Dict[str, Dict[str, Any]],
    sheet_alias_index: Dict[str, Set[str]],
    *,
    deal_status: str = "open",
    fetch_total_limit: int = PIPELINE_FETCH_LIMIT,
    fetch_page_limit: int = PIPELINE_FETCH_PAGE_LIMIT,
    fetch_min_interval_sec: float = DEFAULT_CRM_MIN_INTERVAL_SEC,
) -> Dict[str, Any]:
    org_cache: Dict[int, Dict[str, Any]] = {}
    org_people_cache: Dict[int, List[Dict[str, Any]]] = {}
    person_cache: Dict[int, Dict[str, Any]] = {}
    live_groups: Dict[str, Dict[str, Any]] = {}
    sheet_group_index: Dict[str, Set[str]] = defaultdict(set)
    cnpj_index: Dict[str, Set[str]] = defaultdict(set)
    live_alias_index: Dict[str, Set[str]] = defaultdict(set)

    deals = client.get_deals(
        status=str(deal_status or "open"),
        limit=max(1, int(fetch_total_limit or PIPELINE_FETCH_LIMIT)),
        pipeline_id=int(PIPELINE_ID),
    )
    for deal in deals:
        deal_id = int(deal.get("id") or 0)
        if deal_id <= 0:
            continue
        org_id = extract_id(deal.get("org_id"))
        org = get_org_cached(client, org_cache, org_id) if org_id else {}
        cnpj = normalize_cnpj(client.extract_cnpj(org))
        aliases = company_aliases(
            org.get("name"),
            deal.get("title"),
            (deal.get("org_id") or {}).get("name") if isinstance(deal.get("org_id"), dict) else "",
            deal.get("org_name"),
        )
        sheet_key = choose_sheet_match(cnpj, aliases, sheet_groups, sheet_alias_index)
        group_key = sheet_key or (f"cnpj:{cnpj}" if cnpj else f"name:{sorted(aliases)[0] if aliases else deal_id}")
        group = live_groups.setdefault(
            group_key,
            {
                "group_key": group_key,
                "sheet_key": sheet_key or "",
                "deals": [],
                "org_ids": set(),
                "aliases": set(),
                "live_cnpj": cnpj,
                "org_people": [],
                "live_phones": set(),
                "live_emails": set(),
                "polluted_orgs": set(),
                "_people_loaded": False,
            },
        )
        group["deals"].append(dict(deal))
        if org_id:
            group["org_ids"].add(org_id)
        group["aliases"].update(aliases)
        if cnpj and not group.get("live_cnpj"):
            group["live_cnpj"] = cnpj
        if org and polluted_name(org.get("name")):
            group["polluted_orgs"].add(str(org.get("name") or "").strip())
        current_sheet_key = str(group.get("sheet_key") or "").strip()
        if not current_sheet_key and sheet_key:
            group["sheet_key"] = sheet_key

    for live_group_key, live_group in live_groups.items():
        sheet_key = str(live_group.get("sheet_key") or "").strip()
        if sheet_key:
            sheet_group_index[sheet_key].add(live_group_key)
        live_cnpj = normalize_cnpj(live_group.get("live_cnpj"))
        if live_cnpj:
            cnpj_index[live_cnpj].add(live_group_key)
        for alias in list(live_group.get("aliases") or []):
            clean = str(alias or "").strip()
            if clean:
                live_alias_index[clean].add(live_group_key)

    return {
        "deals_total": len(deals),
        "live_groups": live_groups,
        "sheet_group_index": sheet_group_index,
        "cnpj_index": cnpj_index,
        "live_alias_index": live_alias_index,
        "org_cache": org_cache,
        "org_people_cache": org_people_cache,
        "person_cache": person_cache,
    }


def live_group_matches_super_minas(live_group: Dict[str, Any]) -> bool:
    return any(is_super_minas_deal(deal) for deal in list(live_group.get("deals") or []) if isinstance(deal, dict))


def build_live_group_super_minas_map(live_index: Dict[str, Any]) -> Dict[str, bool]:
    output: Dict[str, bool] = {}
    for live_group in dict(live_index.get("live_groups") or {}).values():
        group_key = str((live_group or {}).get("sheet_key") or (live_group or {}).get("group_key") or "").strip()
        if not group_key:
            continue
        output[group_key] = bool(output.get(group_key)) or live_group_matches_super_minas(live_group)
    return output


def targeted_live_group_score(sheet_group: Dict[str, Any], live_group: Dict[str, Any]) -> Tuple[int, int]:
    score = 0
    shared_alias_count = 0
    target_key = str(sheet_group.get("group_key") or "").strip()
    target_cnpj = normalize_cnpj(sheet_group.get("cnpj"))
    target_aliases = {str(item).strip() for item in list(sheet_group.get("aliases") or []) if str(item).strip()}
    live_group_key = str(live_group.get("group_key") or "").strip()
    live_sheet_key = str(live_group.get("sheet_key") or "").strip()
    live_cnpj = normalize_cnpj(live_group.get("live_cnpj"))
    live_aliases = {str(item).strip() for item in list(live_group.get("aliases") or []) if str(item).strip()}

    if target_key and target_key == live_sheet_key:
        score += 40
    if target_key and target_key == live_group_key:
        score += 25
    if target_cnpj and live_cnpj and live_cnpj == target_cnpj:
        score += 100

    shared_alias_count = len(target_aliases & live_aliases)
    if shared_alias_count:
        score += 20 + min(shared_alias_count, 5)
        canonical_alias = normalize_text(sheet_group.get("canonical_name"))
        if canonical_alias and canonical_alias in live_aliases:
            score += 5
    return score, shared_alias_count


def pick_unique_live_group_candidate(records: List[Dict[str, Any]], *, min_score: int) -> Dict[str, Any]:
    eligible = [dict(item) for item in list(records or []) if int(item.get("score") or 0) >= int(min_score or 0)]
    if not eligible:
        return {}
    eligible.sort(key=lambda item: (-int(item.get("score") or 0), -int(item.get("shared_alias_count") or 0), str(item.get("key") or "")))
    best = dict(eligible[0] or {})
    if len(eligible) > 1:
        runner_up = dict(eligible[1] or {})
        if (
            int(runner_up.get("score") or 0) == int(best.get("score") or 0)
            and int(runner_up.get("shared_alias_count") or 0) == int(best.get("shared_alias_count") or 0)
        ):
            return {}
    return dict(best.get("group") or {})


def find_preloaded_live_group(sheet_group: Dict[str, Any], live_index: Dict[str, Any]) -> Dict[str, Any]:
    live_groups = dict(live_index.get("live_groups") or {})
    if not live_groups:
        return {}

    sheet_group_index = live_index.get("sheet_group_index") or {}
    cnpj_index = live_index.get("cnpj_index") or {}
    live_alias_index = live_index.get("live_alias_index") or {}

    target_key = str(sheet_group.get("group_key") or "").strip()
    target_cnpj = normalize_cnpj(sheet_group.get("cnpj"))
    target_aliases = {str(item).strip() for item in list(sheet_group.get("aliases") or []) if str(item).strip()}

    candidate_keys: Set[str] = set()
    if target_key:
        candidate_keys.update(sheet_group_index.get(target_key) or set())
        if target_key in live_groups:
            candidate_keys.add(target_key)
    if target_cnpj:
        candidate_keys.update(cnpj_index.get(target_cnpj) or set())
    for alias in target_aliases:
        candidate_keys.update(live_alias_index.get(alias) or set())
    if not candidate_keys:
        return {}

    candidate_rows: List[Dict[str, Any]] = []
    for candidate_key in sorted(candidate_keys):
        live_group = live_groups.get(candidate_key) or {}
        if not live_group:
            continue
        live_cnpj = normalize_cnpj(live_group.get("live_cnpj"))
        score, shared_alias_count = targeted_live_group_score(sheet_group, live_group)
        if target_cnpj and live_cnpj and live_cnpj != target_cnpj:
            continue
        if not score:
            continue
        candidate_rows.append(
            {
                "key": candidate_key,
                "group": live_group,
                "score": score,
                "shared_alias_count": shared_alias_count,
                "live_cnpj": live_cnpj,
            }
        )

    if not candidate_rows:
        return {}

    if target_cnpj:
        exact_cnpj = [row for row in candidate_rows if str(row.get("live_cnpj") or "") == target_cnpj]
        matched = pick_unique_live_group_candidate(exact_cnpj, min_score=100)
        if matched:
            return matched
        strict_name_fallback = [row for row in candidate_rows if not str(row.get("live_cnpj") or "") and int(row.get("shared_alias_count") or 0) > 0]
        return pick_unique_live_group_candidate(strict_name_fallback, min_score=20)

    alias_matches = [row for row in candidate_rows if int(row.get("shared_alias_count") or 0) > 0]
    return pick_unique_live_group_candidate(alias_matches, min_score=20)


def build_person_payload(existing_person: Dict[str, Any], *, target_org_id: int, target_name: str, target_phones: List[str], target_emails: List[str]) -> Dict[str, Any]:
    payload: Dict[str, Any] = {}
    current_name = person_display_name(existing_person.get("name"))
    if target_name and normalize_text(target_name) != normalize_text(current_name):
        payload["name"] = target_name
    if target_org_id and extract_id(existing_person.get("org_id")) != int(target_org_id):
        payload["org_id"] = int(target_org_id)

    current_phones = person_phones(existing_person)
    current_emails = person_emails(existing_person)
    if list(target_phones or [])[:5] != list(current_phones or [])[:5]:
        payload["phone"] = [{"value": phone, "primary": index == 0, "label": "work"} for index, phone in enumerate(list(target_phones or [])[:5])]
    if list(target_emails or [])[:5] != list(current_emails or [])[:5]:
        payload["email"] = [{"value": email, "primary": index == 0, "label": "work"} for index, email in enumerate(list(target_emails or [])[:5])]
    return payload


def build_create_person_payload(target_org_id: int, target_name: str, target_phones: List[str], target_emails: List[str]) -> Dict[str, Any]:
    payload: Dict[str, Any] = {"org_id": int(target_org_id), "name": target_name or "Contato"}
    if target_phones:
        payload["phone"] = [{"value": phone, "primary": index == 0, "label": "work"} for index, phone in enumerate(target_phones[:5])]
    if target_emails:
        payload["email"] = [{"value": email, "primary": index == 0, "label": "work"} for index, email in enumerate(target_emails[:5])]
    return payload


def merge_unique(primary: Iterable[str], secondary: Iterable[str]) -> List[str]:
    output: List[str] = []
    for value in list(primary or []) + list(secondary or []):
        clean = str(value or "").strip()
        if clean and clean not in output:
            output.append(clean)
    return output


def serialize_duplicate_deal_meta(duplicate_deals: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    output: List[Dict[str, Any]] = []
    for deal in list(duplicate_deals or []):
        deal_id = int((deal or {}).get("id") or 0)
        if deal_id <= 0:
            continue
        output.append(
            {
                "id": deal_id,
                "stage_id": int((deal or {}).get("stage_id") or 0),
                "title": str((deal or {}).get("title") or "").strip(),
                "org_id": extract_id((deal or {}).get("org_id")),
            }
        )
    return output


def iter_sheet_groups(sheet_groups: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    ordered = list(sheet_groups.values())
    ordered.sort(
        key=lambda item: (
            0 if normalize_cnpj(item.get("cnpj")) else 1,
            -int(item.get("rows") or 0),
            str(item.get("canonical_name") or ""),
            str(item.get("group_key") or ""),
        )
    )
    return ordered


def fetch_org_deals(
    client: PipedriveClient,
    org_id: int,
    *,
    deal_status: str = "open",
    limit: int = 100,
) -> List[Dict[str, Any]]:
    if int(org_id or 0) <= 0:
        return []
    start = 0
    results: List[Dict[str, Any]] = []
    seen_ids: Set[int] = set()
    while True:
        response = client._request(
            "GET",
            f"organizations/{int(org_id)}/deals",
            params={"status": str(deal_status or "open"), "limit": max(1, min(int(limit or 100), 100)), "start": int(start)},
        )
        chunk = list((response or {}).get("data") or []) if isinstance(response, dict) else []
        if not chunk:
            break
        added = 0
        for deal in chunk:
            if not isinstance(deal, dict):
                continue
            if int(deal.get("pipeline_id") or 0) != PIPELINE_ID:
                continue
            deal_id = int(deal.get("id") or 0)
            if deal_id <= 0 or deal_id in seen_ids:
                continue
            seen_ids.add(deal_id)
            results.append(dict(deal))
            added += 1
        pagination = ((response or {}).get("additional_data") or {}).get("pagination") or {}
        if not bool(pagination.get("more_items_in_collection")):
            break
        next_start = pagination.get("next_start")
        start = int(next_start or (int(start) + int(limit or 100)))
        if added <= 0:
            break
    return results


def find_matching_orgs_for_sheet_group(
    client: PipedriveClient,
    sheet_group: Dict[str, Any],
    search_cache: Dict[Tuple[str, str], List[Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    seen_ids: Set[int] = set()
    output: List[Dict[str, Any]] = []
    target_cnpj = normalize_cnpj(sheet_group.get("cnpj"))
    target_aliases = set(str(item).strip() for item in list(sheet_group.get("aliases") or []) if str(item).strip())
    target_names = iter_targeted_search_names(sheet_group, target_cnpj=target_cnpj)

    if target_cnpj:
        for item in search_organizations_cached(client, search_cache, target_cnpj, field_key=client.CNPJ_FIELD_KEY):
            org_stub = dict(item.get("item") or {})
            org_id = extract_id(org_stub.get("id"))
            if org_id <= 0 or org_id in seen_ids:
                continue
            stub_cnpj = normalize_cnpj(client.extract_cnpj(org_stub))
            if stub_cnpj and stub_cnpj != target_cnpj:
                continue
            seen_ids.add(org_id)
            output.append(org_stub)
        if not output and TARGETED_STRICT_NAME_FALLBACK_WITH_CNPJ:
            strict_name_candidates = [
                collapse_spaces(sheet_group.get("canonical_name")),
                collapse_spaces(sheet_group.get("opp_name")),
            ]
            strict_name_candidates = [name for name in strict_name_candidates if name]
            for clean_name in strict_name_candidates[:1]:
                exact_matches: List[Dict[str, Any]] = []
                exact_ids: Set[int] = set()
                for item in search_organizations_cached(client, search_cache, clean_name):
                    org_stub = dict(item.get("item") or {})
                    org_id = extract_id(org_stub.get("id"))
                    if org_id <= 0 or org_id in seen_ids or org_id in exact_ids:
                        continue
                    org_name = str(org_stub.get("name") or "").strip()
                    org_alias = normalize_text(org_name)
                    stub_cnpj = normalize_cnpj(client.extract_cnpj(org_stub))
                    if target_aliases and org_alias not in target_aliases:
                        continue
                    if stub_cnpj and stub_cnpj != target_cnpj:
                        continue
                    exact_ids.add(org_id)
                    exact_matches.append(org_stub)
                if len(exact_matches) == 1:
                    seen_ids.add(extract_id(exact_matches[0].get("id")))
                    output.append(dict(exact_matches[0]))
                break

    for name in target_names:
        clean_name = str(name or "").strip()
        if not clean_name:
            continue
        for item in search_organizations_cached(client, search_cache, clean_name):
            org_stub = dict(item.get("item") or {})
            org_id = extract_id(org_stub.get("id"))
            if org_id <= 0 or org_id in seen_ids:
                continue
            org_name = str(org_stub.get("name") or "").strip()
            org_alias = normalize_text(org_name)
            if target_aliases and org_alias and org_alias not in target_aliases:
                continue
            seen_ids.add(org_id)
            output.append(org_stub)
    return output


def find_candidate_orgs_from_sheet_contacts(
    client: PipedriveClient,
    sheet_group: Dict[str, Any],
    people_search_cache: Dict[str, List[Dict[str, Any]]],
) -> Tuple[List[Dict[str, Any]], bool, bool]:
    output: List[Dict[str, Any]] = []
    seen_org_ids: Set[int] = set()
    target_emails = [normalize_email(item) for item in list(sheet_group.get("exclusive_emails") or []) if normalize_email(item)]
    target_phones = [normalize_phone(item) for item in list(sheet_group.get("exclusive_phones") or []) if normalize_phone(item)]
    lookup_attempted = False
    lookup_rate_limited = False

    for email in target_emails[: max(0, int(TARGETED_CONTACT_EMAIL_LIMIT or 0))]:
        lookup_attempted = True
        for item in search_people_cached(client, people_search_cache, email):
            person = dict(item.get("item") or {})
            org_ref = person.get("org_id")
            org_id = extract_id(org_ref)
            if org_id <= 0 or org_id in seen_org_ids:
                continue
            seen_org_ids.add(org_id)
            output.append({"id": org_id, "name": (org_ref.get("name") if isinstance(org_ref, dict) else "") or ""})
        if client.last_http_endpoint == "persons/search" and int(client.last_http_status or 0) == 429:
            lookup_rate_limited = True

    for phone in target_phones[: max(0, int(TARGETED_CONTACT_PHONE_LIMIT or 0))]:
        lookup_attempted = True
        phone_terms = set(client._phone_search_terms(phone))
        for item in search_people_cached(client, people_search_cache, phone):
            person = dict(item.get("item") or {})
            if phone_terms and not (client._person_phone_tokens(person) & phone_terms):
                continue
            org_ref = person.get("org_id")
            org_id = extract_id(org_ref)
            if org_id <= 0 or org_id in seen_org_ids:
                continue
            seen_org_ids.add(org_id)
            output.append({"id": org_id, "name": (org_ref.get("name") if isinstance(org_ref, dict) else "") or ""})
        if client.last_http_endpoint == "persons/search" and int(client.last_http_status or 0) == 429:
            lookup_rate_limited = True
    return output, lookup_attempted, lookup_rate_limited


def add_candidate_orgs_to_live_group(
    client: PipedriveClient,
    group: Dict[str, Any],
    candidate_orgs: List[Dict[str, Any]],
    *,
    deal_status: str,
    target_cnpj: str,
    target_aliases: Set[str],
    org_cache: Dict[int, Dict[str, Any]],
    org_people_cache: Dict[int, List[Dict[str, Any]]],
    person_cache: Dict[int, Dict[str, Any]],
    processed_org_ids: Set[int],
) -> None:
    for org in candidate_orgs:
        org_id = extract_id(org.get("id"))
        if org_id <= 0 or org_id in processed_org_ids:
            continue
        processed_org_ids.add(org_id)
        full_org = get_org_cached(client, org_cache, org_id) or {}
        org_payload = dict(full_org or org or {})
        org_cnpj = normalize_cnpj(client.extract_cnpj(org_payload))
        org_name = str(org_payload.get("name") or org.get("name") or "").strip()
        matched_aliases = {normalize_text(org_name), normalize_text(org.get("name"))}
        if target_cnpj and org_cnpj and org_cnpj != target_cnpj:
            continue
        if target_aliases and matched_aliases.isdisjoint(target_aliases):
            continue
        org_deals = fetch_org_deals(client, org_id, deal_status=deal_status, limit=100)
        if not org_deals:
            continue
        group["org_ids"].add(org_id)
        group["aliases"].add(normalize_text(org_name))
        if org_cnpj and not group["live_cnpj"]:
            group["live_cnpj"] = org_cnpj
        if polluted_name(org_name):
            group["polluted_orgs"].add(org_name)
        for deal in org_deals:
            if isinstance(deal, dict):
                group["deals"].append(dict(deal))
        for person in get_org_people_cached(client, org_people_cache, org_id):
            person_id = extract_id(person.get("id"))
            if person_id > 0:
                person_cache[person_id] = dict(person)
            group["org_people"].append(dict(person))
            for phone in person_phones(person):
                group["live_phones"].add(phone)
            for email in person_emails(person):
                group["live_emails"].add(email)


def build_live_group_from_sheet(
    client: PipedriveClient,
    sheet_group: Dict[str, Any],
    *,
    deal_status: str,
    org_cache: Dict[int, Dict[str, Any]],
    org_people_cache: Dict[int, List[Dict[str, Any]]],
    person_cache: Dict[int, Dict[str, Any]],
    search_cache: Dict[Tuple[str, str], List[Dict[str, Any]]],
    people_search_cache: Dict[str, List[Dict[str, Any]]],
) -> Dict[str, Any]:
    target_cnpj = normalize_cnpj(sheet_group.get("cnpj"))
    target_aliases = set(str(item).strip() for item in list(sheet_group.get("aliases") or []) if str(item).strip())
    group = {
        "group_key": str(sheet_group.get("group_key") or ""),
        "sheet_key": str(sheet_group.get("group_key") or ""),
        "deals": [],
        "org_ids": set(),
        "aliases": set(sheet_group.get("aliases") or []),
        "live_cnpj": "",
        "org_people": [],
        "live_phones": set(),
        "live_emails": set(),
        "polluted_orgs": set(),
        "contact_miss_fast_skip": False,
    }
    processed_org_ids: Set[int] = set()

    contact_candidate_orgs, contact_lookup_attempted, contact_lookup_rate_limited = find_candidate_orgs_from_sheet_contacts(
        client,
        sheet_group,
        people_search_cache,
    )
    if contact_candidate_orgs:
        add_candidate_orgs_to_live_group(
            client,
            group,
            contact_candidate_orgs,
            deal_status=deal_status,
            target_cnpj=target_cnpj,
            target_aliases=target_aliases,
            org_cache=org_cache,
            org_people_cache=org_people_cache,
            person_cache=person_cache,
            processed_org_ids=processed_org_ids,
        )
    if (
        TARGETED_FAST_SKIP_CONTACT_MISS
        and target_cnpj
        and contact_lookup_attempted
        and not contact_lookup_rate_limited
        and not contact_candidate_orgs
        and not list(group.get("deals") or [])
    ):
        group["contact_miss_fast_skip"] = True
        return group
    if not list(group.get("deals") or []):
        candidate_orgs = find_matching_orgs_for_sheet_group(client, sheet_group, search_cache)
        if not candidate_orgs:
            return group
        add_candidate_orgs_to_live_group(
            client,
            group,
            candidate_orgs,
            deal_status=deal_status,
            target_cnpj=target_cnpj,
            target_aliases=target_aliases,
            org_cache=org_cache,
            org_people_cache=org_people_cache,
            person_cache=person_cache,
            processed_org_ids=processed_org_ids,
        )
    return group


def build_plan_for_group(
    client: PipedriveClient,
    group: Dict[str, Any],
    sheet_group: Dict[str, Any],
    *,
    org_cache: Dict[int, Dict[str, Any]],
    person_cache: Dict[int, Dict[str, Any]],
    live_phone_owners: Optional[Dict[str, Set[str]]] = None,
    live_email_owners: Optional[Dict[str, Set[str]]] = None,
) -> Dict[str, Any]:
    master = select_master_deal(group.get("deals") or [])
    group["master"] = dict(master or {})
    duplicate_deals = [
        dict(item)
        for item in list(group.get("deals") or [])
        if int((item or {}).get("id") or 0) > 0 and int((item or {}).get("id") or 0) != int((master or {}).get("id") or 0)
    ]

    canonical_name = canonical_legal_name(
        sheet_group.get("canonical_name"),
        sheet_group.get("opp_name"),
        get_org_cached(client, org_cache, extract_id(master.get("org_id"))).get("name"),
        master.get("title"),
    )
    canonical_cnpj = normalize_cnpj(sheet_group.get("cnpj") or group.get("live_cnpj"))
    exclusive_sheet_phones = list(sheet_group.get("exclusive_source_phones") or sheet_group.get("exclusive_phones") or [])
    exclusive_sheet_emails = list(sheet_group.get("exclusive_source_emails") or sheet_group.get("exclusive_emails") or [])
    preferred_person_name = str(sheet_group.get("preferred_person_name") or "").strip() or choose_group_person_name(sheet_group)
    verified_person_name = str(sheet_group.get("verified_person_name") or "").strip() or choose_group_person_name(sheet_group, verified_only=True)
    has_verified_contact_signal = bool(exclusive_sheet_phones or exclusive_sheet_emails)
    has_verified_person_signal = bool(verified_person_name)
    has_verified_source = bool(has_verified_contact_signal or has_verified_person_signal)

    if live_phone_owners is None:
        live_phone_owners = {phone: {group.get("group_key")} for phone in sorted_unique(group.get("live_phones") or [])}
    if live_email_owners is None:
        live_email_owners = {email: {group.get("group_key")} for email in sorted_unique(group.get("live_emails") or [])}
    target_phones = list(exclusive_sheet_phones)[:5]
    target_emails = list(exclusive_sheet_emails)[:5]
    target_person_name = preferred_person_name

    target_org = choose_target_org(group, client, org_cache, canonical_name, canonical_cnpj)
    target_org_id = extract_id(target_org.get("id"))
    target_person = choose_primary_person(client, person_cache, group, target_org_id, target_person_name, target_phones, target_emails)
    target_person_id = extract_id(target_person.get("id"))

    update_person_name = verified_person_name or person_display_name(target_person.get("name")) or preferred_person_name or canonical_name or "Contato"
    create_person_name = preferred_person_name or verified_person_name or canonical_name or "Contato"
    final_phones = list(target_phones)[:5]
    final_emails = list(target_emails)[:5]

    org_payload: Dict[str, Any] = {}
    if canonical_name and normalize_text(target_org.get("name")) != normalize_text(canonical_name):
        org_payload["name"] = canonical_name
    current_target_cnpj = normalize_cnpj(client.extract_cnpj(target_org))
    if canonical_cnpj and canonical_cnpj != current_target_cnpj:
        org_payload[client.CNPJ_FIELD_KEY] = canonical_cnpj

    person_payload = {}
    create_person_payload = {}
    if target_person_id > 0 and has_verified_source:
        person_payload = build_person_payload(
            target_person,
            target_org_id=target_org_id,
            target_name=update_person_name,
            target_phones=final_phones,
            target_emails=final_emails,
        )
    elif int(target_org_id or 0) > 0 and has_verified_source:
        create_person_payload = build_create_person_payload(target_org_id, create_person_name, final_phones, final_emails)

    deal_payload: Dict[str, Any] = {}
    if canonical_name and normalize_text(master.get("title")) != normalize_text(canonical_name):
        deal_payload["title"] = canonical_name
    if target_org_id and extract_id(master.get("org_id")) != target_org_id:
        deal_payload["org_id"] = target_org_id
    if has_verified_source and target_person_id and extract_id(master.get("person_id")) != target_person_id:
        deal_payload["person_id"] = target_person_id

    participant_cleanup: List[Dict[str, Any]] = []
    participant_add: List[int] = []
    master_id = int(master.get("id") or 0)
    if master_id > 0:
        participants = client.get_deal_participants(master_id, limit=100) or []
        seen_participants: Set[int] = set()
        for participant in list(participants or []):
            participant_id = extract_id(participant.get("id"))
            person_id = extract_id(participant.get("person_id"))
            if not participant_id or not person_id:
                continue
            person = get_person_cached(client, person_cache, person_id)
            reason = ""
            if person_id in seen_participants:
                reason = "duplicate_person"
            elif target_org_id and extract_id(person.get("org_id")) != target_org_id:
                reason = "org_mismatch"
            elif polluted_name((person.get("org_id") or {}).get("name") if isinstance(person.get("org_id"), dict) else "") or polluted_name(person.get("name")):
                reason = "polluted_token"
            if reason:
                participant_cleanup.append({"participant_id": participant_id, "person_id": person_id, "reason": reason})
            else:
                seen_participants.add(person_id)

        for duplicate in duplicate_deals:
            duplicate_person_id = extract_id(duplicate.get("person_id"))
            if not duplicate_person_id:
                continue
            duplicate_person = get_person_cached(client, person_cache, duplicate_person_id)
            if target_org_id and extract_id(duplicate_person.get("org_id")) == target_org_id and duplicate_person_id not in participant_add:
                participant_add.append(duplicate_person_id)

    return {
        "group_key": group.get("group_key"),
        "sheet_key": group.get("sheet_key"),
        "master_deal_id": master_id,
        "duplicate_deal_ids": sorted(int(item.get("id") or 0) for item in duplicate_deals if int(item.get("id") or 0) > 0),
        "duplicate_deals_meta": serialize_duplicate_deal_meta(duplicate_deals),
        "canonical_name": canonical_name,
        "canonical_cnpj": canonical_cnpj,
        "target_org_id": target_org_id,
        "target_person_id": target_person_id,
        "sheet_match": bool(sheet_group),
        "sheet_rows": int(sheet_group.get("rows") or 0),
        "people_rows": int(sheet_group.get("people_rows") or 0),
        "target_phones": final_phones,
        "target_emails": final_emails,
        "target_person_name": update_person_name,
        "preferred_person_name": preferred_person_name,
        "verified_person_name": verified_person_name,
        "has_verified_source": has_verified_source,
        "crm_contact_fallback_blocked": True,
        "org_payload": org_payload,
        "person_payload": person_payload,
        "create_person_payload": create_person_payload,
        "deal_payload": deal_payload,
        "participant_cleanup": participant_cleanup,
        "participant_add": sorted(set(participant_add)),
        "polluted_orgs": sorted_unique(group.get("polluted_orgs") or []),
    }


def build_plan(
    client: PipedriveClient,
    sheet_groups: Dict[str, Dict[str, Any]],
    alias_index: Dict[str, Set[str]],
    *,
    deal_status: str = "open",
    fetch_total_limit: int = PIPELINE_FETCH_LIMIT,
    fetch_page_limit: int = PIPELINE_FETCH_PAGE_LIMIT,
    fetch_min_interval_sec: float = DEFAULT_CRM_MIN_INTERVAL_SEC,
    limit_groups: int = 0,
) -> Dict[str, Any]:
    deals = fetch_pipeline_deals_raw(
        client,
        deal_status=str(deal_status or "open"),
        total_limit=int(fetch_total_limit or PIPELINE_FETCH_LIMIT),
        page_limit=int(fetch_page_limit or PIPELINE_FETCH_PAGE_LIMIT),
        min_interval_sec=float(fetch_min_interval_sec or DEFAULT_CRM_MIN_INTERVAL_SEC),
    )

    org_cache: Dict[int, Dict[str, Any]] = {}
    org_people_cache: Dict[int, List[Dict[str, Any]]] = {}
    person_cache: Dict[int, Dict[str, Any]] = {}

    live_groups: Dict[str, Dict[str, Any]] = {}
    for deal in deals:
        deal_id = int(deal.get("id") or 0)
        if deal_id <= 0:
            continue
        org_id = extract_id(deal.get("org_id"))
        org = get_org_cached(client, org_cache, org_id) if org_id else {}
        cnpj = normalize_cnpj(client.extract_cnpj(org))
        aliases = company_aliases(org.get("name"), deal.get("title"), (deal.get("org_id") or {}).get("name") if isinstance(deal.get("org_id"), dict) else "", deal.get("org_name"))
        sheet_key = choose_sheet_match(cnpj, aliases, sheet_groups, alias_index)
        group_key = sheet_key or (f"cnpj:{cnpj}" if cnpj else f"name:{sorted(aliases)[0] if aliases else deal_id}")
        group = live_groups.setdefault(
            group_key,
            {
                "group_key": group_key,
                "sheet_key": sheet_key,
                "deals": [],
                "org_ids": set(),
                "aliases": set(),
                "live_cnpj": cnpj,
                "org_people": [],
                "live_phones": set(),
                "live_emails": set(),
                "polluted_orgs": set(),
            },
        )
        group["deals"].append(dict(deal))
        if org_id:
            group["org_ids"].add(org_id)
        group["aliases"].update(aliases)
        if org and polluted_name(org.get("name")):
            group["polluted_orgs"].add(str(org.get("name") or "").strip())

    live_phone_owners: Dict[str, Set[str]] = defaultdict(set)
    live_email_owners: Dict[str, Set[str]] = defaultdict(set)
    for group in live_groups.values():
        people_seen: Set[int] = set()
        for org_id in sorted(int(item) for item in list(group.get("org_ids") or []) if int(item or 0) > 0):
            for person in get_org_people_cached(client, org_people_cache, org_id):
                person_id = extract_id(person.get("id"))
                if person_id and person_id in people_seen:
                    continue
                if person_id:
                    people_seen.add(person_id)
                    person_cache[person_id] = dict(person)
                group["org_people"].append(dict(person))
                for phone in person_phones(person):
                    group["live_phones"].add(phone)
                    live_phone_owners[phone].add(group["group_key"])
                for email in person_emails(person):
                    group["live_emails"].add(email)
                    live_email_owners[email].add(group["group_key"])

    plans: List[Dict[str, Any]] = []
    counters: Counter[str] = Counter()
    ordered_groups = sorted(live_groups.values(), key=lambda item: min(int((deal or {}).get("id") or 0) for deal in list(item.get("deals") or []) if int((deal or {}).get("id") or 0) > 0))
    if limit_groups:
        ordered_groups = ordered_groups[: max(1, int(limit_groups))]

    total_groups = len(ordered_groups)
    for index, group in enumerate(ordered_groups, start=1):
        sheet_group = sheet_groups.get(str(group.get("sheet_key") or group.get("group_key") or "")) or {}
        plan = build_plan_for_group(
            client,
            group,
            sheet_group,
            org_cache=org_cache,
            person_cache=person_cache,
            live_phone_owners=live_phone_owners,
            live_email_owners=live_email_owners,
        )
        plans.append(plan)

        counters["groups"] += 1
        counters["masters"] += 1
        if sheet_group:
            counters["matched_sheet_groups"] += 1
        else:
            counters["unmatched_sheet_groups"] += 1
        if plan.get("canonical_cnpj"):
            counters["groups_with_cnpj"] += 1
        if plan["duplicate_deal_ids"]:
            counters["duplicate_groups"] += 1
            counters["duplicate_deals"] += len(plan["duplicate_deal_ids"])
        if plan.get("org_payload"):
            counters["org_update_plan"] += 1
        if plan.get("person_payload"):
            counters["person_update_plan"] += 1
        if plan.get("create_person_payload"):
            counters["person_create_plan"] += 1
        if plan.get("deal_payload"):
            counters["deal_update_plan"] += 1
        if plan.get("participant_cleanup"):
            counters["participant_remove_plan"] += len(list(plan.get("participant_cleanup") or []))
        if plan.get("participant_add"):
            counters["participant_add_plan"] += len(list(plan.get("participant_add") or []))
        counters["exclusive_phones_selected"] += len(list(plan.get("target_phones") or []))
        counters["exclusive_emails_selected"] += len(list(plan.get("target_emails") or []))
        if index % 25 == 0:
            log(
                f"[PLAN_PROGRESS] {index}/{total_groups} "
                f"duplicates={counters['duplicate_deals']} org_updates={counters['org_update_plan']} "
                f"deal_updates={counters['deal_update_plan']}"
            )

    report = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "pipeline_id": PIPELINE_ID,
        "deal_status": str(deal_status or "open"),
        "fetch_total_limit": int(fetch_total_limit or PIPELINE_FETCH_LIMIT),
        "fetch_page_limit": int(fetch_page_limit or PIPELINE_FETCH_PAGE_LIMIT),
        "crm_deals_total": len(deals),
        "sheet_summary": {
            "groups_total": len(sheet_groups),
            "groups_with_cnpj": sum(1 for item in sheet_groups.values() if normalize_cnpj(item.get("cnpj"))),
            "rows_total": sum(int(item.get("rows") or 0) for item in sheet_groups.values()),
        },
        "plan_counters": dict(sorted(counters.items())),
        "plans": plans,
    }
    return report


def run_targeted(
    client: PipedriveClient,
    sheet_groups: Dict[str, Dict[str, Any]],
    sheet_alias_index: Dict[str, Set[str]],
    *,
    deal_status: str = "open",
    apply: bool = False,
    report_path: Path = REPORT_JSON,
    state_path: Path = STATE_JSON,
    limit_groups: int = 0,
    deactivate_duplicates: bool = False,
    fetch_total_limit: int = PIPELINE_FETCH_LIMIT,
    fetch_page_limit: int = PIPELINE_FETCH_PAGE_LIMIT,
    fetch_min_interval_sec: float = DEFAULT_CRM_MIN_INTERVAL_SEC,
    super_minas_only: bool = False,
    exclude_super_minas: bool = False,
) -> Dict[str, Any]:
    live_index = build_targeted_live_index(
        client,
        sheet_groups,
        sheet_alias_index,
        deal_status=deal_status,
        fetch_total_limit=int(fetch_total_limit or PIPELINE_FETCH_LIMIT),
        fetch_page_limit=int(fetch_page_limit or PIPELINE_FETCH_PAGE_LIMIT),
        fetch_min_interval_sec=float(fetch_min_interval_sec or DEFAULT_CRM_MIN_INTERVAL_SEC),
    )
    org_cache: Dict[int, Dict[str, Any]] = dict(live_index.get("org_cache") or {})
    org_people_cache: Dict[int, List[Dict[str, Any]]] = dict(live_index.get("org_people_cache") or {})
    person_cache: Dict[int, Dict[str, Any]] = dict(live_index.get("person_cache") or {})
    search_cache: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    people_search_cache: Dict[str, List[Dict[str, Any]]] = {}
    state = load_state(state_path)
    done: Set[str] = {str(item).strip() for item in list(state.get("done") or []) if str(item).strip()}
    counters: Counter[str] = Counter(dict(state.get("counters") or {}))
    results: List[Dict[str, Any]] = [dict(item) for item in list(state.get("results") or []) if isinstance(item, dict)]
    live_group_super_minas = build_live_group_super_minas_map(live_index)
    log(
        f"[TARGETED_LIVE_INDEX] deals={int(live_index.get('deals_total') or 0)} "
        f"live_groups={len(dict(live_index.get('live_groups') or {}))} "
        f"sheet_hits={len(dict(live_index.get('sheet_group_index') or {}))}"
    )

    ordered_groups: List[Dict[str, Any]] = []
    for sheet_group in iter_sheet_groups(sheet_groups):
        group_key = str(sheet_group.get("group_key") or "").strip()
        if not group_key or group_key in done:
            continue
        preloaded_is_super_minas = live_group_super_minas.get(group_key)
        if super_minas_only and preloaded_is_super_minas is False:
            counters["filtered_preloaded_non_super_minas"] += 1
            continue
        if exclude_super_minas and preloaded_is_super_minas is True:
            counters["filtered_preloaded_super_minas"] += 1
            continue
        ordered_groups.append(sheet_group)
    if limit_groups:
        ordered_groups = ordered_groups[: max(1, int(limit_groups))]
    total_groups = len(ordered_groups)

    for index, sheet_group in enumerate(ordered_groups, start=1):
        group_key = str(sheet_group.get("group_key") or "").strip()
        log(
            f"[TARGETED_START] {index}/{total_groups} group={group_key} "
            f"cnpj={normalize_cnpj(sheet_group.get('cnpj')) or 'na'} nome={str(sheet_group.get('canonical_name') or '')[:80]}"
        )
        live_group = find_preloaded_live_group(sheet_group, live_index)
        if live_group:
            hydrate_live_group_people(
                client,
                live_group,
                org_people_cache=org_people_cache,
                person_cache=person_cache,
            )
        elif list(sheet_group.get("exclusive_phones") or []) or list(sheet_group.get("exclusive_emails") or []):
            live_group = build_live_group_from_sheet(
                client,
                sheet_group,
                deal_status=deal_status,
                org_cache=org_cache,
                org_people_cache=org_people_cache,
                person_cache=person_cache,
                search_cache=search_cache,
                people_search_cache=people_search_cache,
            )
        else:
            live_group = {
                "group_key": group_key,
                "sheet_key": group_key,
                "deals": [],
                "org_ids": set(),
                "aliases": set(sheet_group.get("aliases") or []),
                "live_cnpj": "",
                "org_people": [],
                "live_phones": set(),
                "live_emails": set(),
                "polluted_orgs": set(),
                "contact_miss_fast_skip": False,
            }
        if not list(live_group.get("deals") or []):
            counters["sheet_groups_without_live_deals"] += 1
            if bool(live_group.get("contact_miss_fast_skip")):
                counters["sheet_groups_without_live_deals_fast_skip"] += 1
            result = {
                "group_key": group_key,
                "status": "no_live_deals_contact_miss" if bool(live_group.get("contact_miss_fast_skip")) else "no_live_deals",
                "canonical_name": sheet_group.get("canonical_name"),
                "cnpj": sheet_group.get("cnpj"),
            }
            results.append(result)
            done.add(group_key)
            save_state({"done": sorted(done), "counters": dict(sorted(counters.items())), "results": results[-1000:]}, state_path)
            continue

        live_group_is_super_minas = live_group_matches_super_minas(live_group)
        if super_minas_only and not live_group_is_super_minas:
            counters["filtered_runtime_non_super_minas"] += 1
            result = {
                "group_key": group_key,
                "status": "filtered_non_super_minas",
                "canonical_name": sheet_group.get("canonical_name"),
                "cnpj": sheet_group.get("cnpj"),
            }
            results.append(result)
            done.add(group_key)
            save_state({"done": sorted(done), "counters": dict(sorted(counters.items())), "results": results[-1000:]}, state_path)
            continue
        if exclude_super_minas and live_group_is_super_minas:
            counters["filtered_runtime_super_minas"] += 1
            result = {
                "group_key": group_key,
                "status": "filtered_super_minas",
                "canonical_name": sheet_group.get("canonical_name"),
                "cnpj": sheet_group.get("cnpj"),
            }
            results.append(result)
            done.add(group_key)
            save_state({"done": sorted(done), "counters": dict(sorted(counters.items())), "results": results[-1000:]}, state_path)
            continue

        live_phone_owners: Dict[str, Set[str]] = defaultdict(set)
        live_email_owners: Dict[str, Set[str]] = defaultdict(set)
        for phone in sorted_unique(live_group.get("live_phones") or []):
            live_phone_owners[phone].add(group_key)
        for email in sorted_unique(live_group.get("live_emails") or []):
            live_email_owners[email].add(group_key)

        plan = build_plan_for_group(
            client,
            live_group,
            sheet_group,
            org_cache=org_cache,
            person_cache=person_cache,
            live_phone_owners=live_phone_owners,
            live_email_owners=live_email_owners,
        )

        if int(plan.get("master_deal_id") or 0) <= 0:
            counters["targeted_skip_invalid_master"] += 1
            result = {
                "group_key": group_key,
                "status": "invalid_master",
                "canonical_name": plan.get("canonical_name") or sheet_group.get("canonical_name"),
                "cnpj": plan.get("canonical_cnpj") or sheet_group.get("cnpj"),
            }
            results.append(result)
            done.add(group_key)
            save_state({"done": sorted(done), "counters": dict(sorted(counters.items())), "results": results[-1000:]}, state_path)
            continue

        counters["groups"] += 1
        counters["masters"] += 1
        if plan.get("canonical_cnpj"):
            counters["groups_with_cnpj"] += 1
        if plan.get("org_payload"):
            counters["org_update_plan"] += 1
        if plan.get("person_payload"):
            counters["person_update_plan"] += 1
        if plan.get("create_person_payload"):
            counters["person_create_plan"] += 1
        if plan.get("deal_payload"):
            counters["deal_update_plan"] += 1
        counters["exclusive_phones_selected"] += len(list(plan.get("target_phones") or []))
        counters["exclusive_emails_selected"] += len(list(plan.get("target_emails") or []))
        if list(plan.get("duplicate_deal_ids") or []):
            counters["duplicate_groups"] += 1
            counters["duplicate_deals"] += len(list(plan.get("duplicate_deal_ids") or []))

        apply_summary: Dict[str, Any] = {}
        if apply:
            if int(plan.get("target_org_id") or 0) > 0 and plan.get("org_payload"):
                if client.update_organization(int(plan["target_org_id"]), dict(plan.get("org_payload") or {})):
                    counters["org_updated"] += 1
                    apply_summary["org_updated"] = True

            target_person_id = int(plan.get("target_person_id") or 0)
            if target_person_id > 0 and plan.get("person_payload"):
                if client.update_person(target_person_id, dict(plan.get("person_payload") or {})):
                    counters["person_updated"] += 1
                    apply_summary["person_updated"] = True
            elif target_person_id <= 0 and plan.get("create_person_payload"):
                created = client.create_person(dict(plan.get("create_person_payload") or {})) or {}
                created_id = extract_id(created.get("id"))
                if created_id > 0:
                    target_person_id = created_id
                    plan["target_person_id"] = created_id
                    counters["person_created"] += 1
                    apply_summary["person_created"] = created_id

            master_deal_payload = dict(plan.get("deal_payload") or {})
            if target_person_id > 0:
                master_deal_payload["person_id"] = target_person_id
            if master_deal_payload and client.update_deal(int(plan["master_deal_id"]), master_deal_payload):
                counters["deal_updated"] += 1
                apply_summary["deal_updated"] = True

            for item in list(plan.get("participant_cleanup") or []):
                participant_id = int(item.get("participant_id") or 0)
                if participant_id > 0 and client.remove_deal_participant(int(plan["master_deal_id"]), participant_id):
                    counters["participant_removed"] += 1
            for person_id in list(plan.get("participant_add") or []):
                if int(person_id or 0) > 0 and int(person_id or 0) != int(plan.get("target_person_id") or 0):
                    if client.add_deal_participant(int(plan["master_deal_id"]), int(person_id)):
                        counters["participant_added"] += 1

            if deactivate_duplicates:
                for duplicate_meta in list(plan.get("duplicate_deals_meta") or []):
                    duplicate_id = int((duplicate_meta or {}).get("id") or 0)
                    if int(duplicate_id or 0) <= 0:
                        continue
                    payload = {
                        "status": "lost",
                        "status_bot": "perdido",
                        "lost_reason": f"duplicado_master_{int(plan['master_deal_id'])}",
                    }
                    current_stage_id = int((duplicate_meta or {}).get("stage_id") or 0)
                    if current_stage_id > 0:
                        payload["stage_id"] = current_stage_id
                    if client.update_deal(int(duplicate_id), payload):
                        counters["duplicate_deals_lost"] += 1
                        client.add_note(
                            deal_id=int(duplicate_id),
                            content=f"Deal duplicado movido para perdido. Master preservado: #{int(plan['master_deal_id'])}.",
                        )

            counters["apply_ok"] += 1

        result = {
            "group_key": group_key,
            "status": "applied" if apply else "planned",
            "canonical_name": plan.get("canonical_name"),
            "cnpj": plan.get("canonical_cnpj"),
            "master_deal_id": plan.get("master_deal_id"),
            "duplicate_deal_ids": plan.get("duplicate_deal_ids"),
            "target_org_id": plan.get("target_org_id"),
            "target_person_id": plan.get("target_person_id"),
            "people_rows": plan.get("people_rows"),
            "has_verified_source": plan.get("has_verified_source"),
            "target_phones": plan.get("target_phones"),
            "target_emails": plan.get("target_emails"),
            "apply_summary": apply_summary,
        }
        results.append(result)
        done.add(group_key)
        if index % 10 == 0:
            log(
                f"[TARGETED_PROGRESS] {index}/{total_groups} "
                f"done={len(done)} duplicates={counters['duplicate_deals']} "
                f"org_updated={counters['org_updated']} deal_updated={counters['deal_updated']}"
            )
        save_state({"done": sorted(done), "counters": dict(sorted(counters.items())), "results": results[-1000:]}, state_path)

    report = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "mode": "targeted_apply" if apply else "targeted_dry_run",
        "pipeline_id": PIPELINE_ID,
        "deal_status": str(deal_status or "open"),
        "sheet_summary": {
            "groups_total": len(sheet_groups),
            "groups_with_cnpj": sum(1 for item in sheet_groups.values() if normalize_cnpj(item.get("cnpj"))),
            "rows_total": sum(int(item.get("rows") or 0) for item in sheet_groups.values()),
        },
        "plan_counters": dict(sorted(counters.items())),
        "results": results[-1000:],
    }
    save_report(report_path, report)
    return report


def apply_plan(client: PipedriveClient, report: Dict[str, Any], *, delete_duplicates: bool = False) -> Counter[str]:
    counters: Counter[str] = Counter()
    for plan in list(report.get("plans") or []):
        master_deal_id = int(plan.get("master_deal_id") or 0)
        target_org_id = int(plan.get("target_org_id") or 0)
        if master_deal_id <= 0 or target_org_id <= 0:
            counters["skip_invalid_plan"] += 1
            continue

        if plan.get("org_payload"):
            if client.update_organization(target_org_id, dict(plan.get("org_payload") or {})):
                counters["org_updated"] += 1

        target_person_id = int(plan.get("target_person_id") or 0)
        if target_person_id > 0 and plan.get("person_payload"):
            if client.update_person(target_person_id, dict(plan.get("person_payload") or {})):
                counters["person_updated"] += 1
        elif target_person_id <= 0 and plan.get("create_person_payload"):
            created = client.create_person(dict(plan.get("create_person_payload") or {})) or {}
            created_id = extract_id(created.get("id"))
            if created_id > 0:
                target_person_id = created_id
                counters["person_created"] += 1

        deal_payload = dict(plan.get("deal_payload") or {})
        if target_person_id > 0:
            deal_payload["person_id"] = target_person_id
        if deal_payload:
            if client.update_deal(master_deal_id, deal_payload):
                counters["deal_updated"] += 1

        for participant_id in [item.get("participant_id") for item in list(plan.get("participant_cleanup") or [])]:
            if int(participant_id or 0) > 0 and client.remove_deal_participant(master_deal_id, int(participant_id)):
                counters["participant_removed"] += 1

        for person_id in list(plan.get("participant_add") or []):
            if int(person_id or 0) <= 0 or int(person_id) == int(target_person_id or 0):
                continue
            if client.add_deal_participant(master_deal_id, int(person_id)):
                counters["participant_added"] += 1

        if delete_duplicates:
            for duplicate_id in list(plan.get("duplicate_deal_ids") or []):
                if int(duplicate_id or 0) > 0 and client.delete_deal(int(duplicate_id)):
                    counters["duplicate_deals_deleted"] += 1

        counters["master_processed"] += 1
    return counters


def save_report(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=str(INPUT_XLSX))
    parser.add_argument("--people-input", default=str(PEOPLE_INPUT_XLSX))
    parser.add_argument("--report", default=str(REPORT_JSON))
    parser.add_argument("--state", default=str(STATE_JSON))
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--delete-duplicates", action="store_true")
    parser.add_argument("--deactivate-duplicates", action="store_true")
    parser.add_argument("--sheet-only-report", action="store_true")
    parser.add_argument("--validate-opp", action="store_true")
    parser.add_argument("--opp-token", default="")
    parser.add_argument("--mode", choices=("targeted", "global"), default="targeted")
    parser.add_argument("--crm-min-interval", type=float, default=DEFAULT_CRM_MIN_INTERVAL_SEC)
    parser.add_argument("--opp-min-interval", type=float, default=DEFAULT_OPP_MIN_INTERVAL_SEC)
    parser.add_argument("--limit-groups", type=int, default=0)
    parser.add_argument("--deal-status", default="open")
    parser.add_argument("--fetch-total-limit", type=int, default=PIPELINE_FETCH_LIMIT)
    parser.add_argument("--fetch-page-limit", type=int, default=PIPELINE_FETCH_PAGE_LIMIT)
    parser.add_argument("--super-minas-only", action="store_true")
    parser.add_argument("--exclude-super-minas", action="store_true")
    args = parser.parse_args()

    if bool(args.super_minas_only) and bool(args.exclude_super_minas):
        raise SystemExit("use apenas um filtro: --super-minas-only ou --exclude-super-minas")

    input_path = Path(args.input)
    people_input_path = Path(args.people_input)
    report_path = Path(args.report)
    state_path = Path(args.state)
    rows = parse_xlsx(input_path)
    people_rows = parse_xlsx(people_input_path) if people_input_path.exists() else []
    opp_token = str(args.opp_token or os.environ.get("OPORTUNIDADOS_API_TOKEN") or "").strip()
    opp_client = None
    if args.validate_opp and opp_token:
        opp_client = OportunidadesClient(opp_token, min_interval_sec=float(args.opp_min_interval or DEFAULT_OPP_MIN_INTERVAL_SEC))

    sheet_groups, alias_index, sheet_summary = build_sheet_groups(rows, people_rows=people_rows, opp_client=opp_client)
    if args.sheet_only_report:
        payload = {
            "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "mode": "sheet_only_report",
            "sheet_summary": sheet_summary,
            "sheet_groups": [
                {
                    "group_key": item.get("group_key"),
                    "cnpj": item.get("cnpj"),
                    "canonical_name": item.get("canonical_name"),
                    "rows": item.get("rows"),
                    "people_rows": item.get("people_rows"),
                    "exclusive_phones": item.get("exclusive_phones"),
                    "exclusive_emails": item.get("exclusive_emails"),
                    "exclusive_source_phones": item.get("exclusive_source_phones"),
                    "exclusive_source_emails": item.get("exclusive_source_emails"),
                    "preferred_person_name": item.get("preferred_person_name"),
                    "verified_person_name": item.get("verified_person_name"),
                }
                for item in sorted(sheet_groups.values(), key=lambda group: (-int(group.get("rows") or 0), str(group.get("canonical_name") or "")))
            ],
        }
        save_report(report_path, payload)
        log(
            "[SHEET_REPORT] "
            f"rows={sheet_summary['rows_total']} groups={sheet_summary['groups_total']} "
            f"dup_cnpj={len(sheet_summary['duplicate_cnpj_groups'])} dup_name={len(sheet_summary['duplicate_name_groups'])}"
        )
        return 0

    PipedriveClient.GLOBAL_MIN_INTERVAL_SEC = max(float(PipedriveClient.GLOBAL_MIN_INTERVAL_SEC or 0.0), float(args.crm_min_interval or DEFAULT_CRM_MIN_INTERVAL_SEC))
    client = PipedriveClient()
    if str(args.mode or "targeted") == "targeted":
        report = run_targeted(
            client,
            sheet_groups,
            alias_index,
            deal_status=str(args.deal_status or "open"),
            apply=bool(args.apply),
            report_path=report_path,
            state_path=state_path,
            limit_groups=int(args.limit_groups or 0),
            deactivate_duplicates=bool(args.deactivate_duplicates),
            fetch_total_limit=int(args.fetch_total_limit or PIPELINE_FETCH_LIMIT),
            fetch_page_limit=int(args.fetch_page_limit or PIPELINE_FETCH_PAGE_LIMIT),
            fetch_min_interval_sec=float(args.crm_min_interval or DEFAULT_CRM_MIN_INTERVAL_SEC),
            super_minas_only=bool(args.super_minas_only),
            exclude_super_minas=bool(args.exclude_super_minas),
        )
    else:
        report = build_plan(
            client,
            sheet_groups,
            alias_index,
            deal_status=str(args.deal_status or "open"),
            fetch_total_limit=int(args.fetch_total_limit or PIPELINE_FETCH_LIMIT),
            fetch_page_limit=int(args.fetch_page_limit or PIPELINE_FETCH_PAGE_LIMIT),
            fetch_min_interval_sec=float(args.crm_min_interval or DEFAULT_CRM_MIN_INTERVAL_SEC),
            limit_groups=int(args.limit_groups or 0),
        )
        report["mode"] = "apply" if args.apply else "dry_run"
        report["sheet_summary_full"] = sheet_summary
        if args.apply:
            apply_counters = apply_plan(client, report, delete_duplicates=bool(args.delete_duplicates))
            report["apply_counters"] = dict(sorted(apply_counters.items()))
        save_report(report_path, report)

    log(
        "[WRITEBACK_PLAN] "
        f"mode={report['mode']} groups={report.get('plan_counters', {}).get('groups', 0)} "
        f"duplicates={report.get('plan_counters', {}).get('duplicate_deals', 0)} "
        f"org_updates={report.get('plan_counters', {}).get('org_update_plan', 0)} "
        f"deal_updates={report.get('plan_counters', {}).get('deal_update_plan', 0)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
