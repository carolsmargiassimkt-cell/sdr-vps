from __future__ import annotations

import json
import re
import time
import unicodedata
from collections import Counter, defaultdict
from datetime import datetime
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Dict, Iterable, List

import openpyxl

from config.config_loader import get_config_value
from enrich_missing_cnpj_from_xlsx import normalize_cnpj, normalize_phone, parse_xlsx
from reset_pipeline2_participants import CACHE_JSON as CASA_CACHE_JSON
from reset_pipeline2_participants import collect_casa_fields, fetch_casa_cnpj
from resolve_missing_cnpj_and_reset import choose_cnpj, load_cache as load_name_cache, variants_from_name


INPUT_XLSX = Path(r"C:\Users\Asus\Downloads\deals-25681240-11.xlsx")
OUTPUT_XLSX = Path(r"C:\Users\Asus\Downloads\deals_pronto_writeback.xlsx")
BLOCKED_PHONES = {"1804330972", "18043309723", "8731990856", "11999999999", "00999999977", "1155555555"}
GENERIC_NAMES = {"", "lead", "negocio", "sem empre", "empresa", "supermercado", "loja", "comercio"}


def log(message: str) -> None:
    print(str(message), flush=True)


def normalize_key(value: Any) -> str:
    text = unicodedata.normalize("NFKD", str(value or "").strip().lower())
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return re.sub(r"[^a-z0-9]+", "", text)


def row_get(row: Dict[str, Any], label: str) -> str:
    target = normalize_key(label)
    for key, value in row.items():
        if normalize_key(key) == target:
            return str(value or "").strip()
    return ""


def clean_name(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"^lead\s+mand\s+digital\s*-\s*", "", text, flags=re.I)
    text = re.sub(r"^lead\s+mand\s+digital\s*", "", text, flags=re.I)
    text = re.sub(r"\blead\b", " ", text, flags=re.I)
    text = re.sub(r"\bneg[oó]cio\b", " ", text, flags=re.I)
    text = re.sub(r"\s*-\s*neg[oó]cio\s*$", "", text, flags=re.I)
    text = re.sub(r"[^a-z0-9áàãâéêíóôõúç\s./&-]+", " ", text, flags=re.I)
    return re.sub(r"\s+", " ", text).strip(" -")


def title_name(value: Any) -> str:
    text = clean_name(value)
    small = {"de", "da", "do", "das", "dos", "e", "em", "a", "o", "s/a", "sa", "ltda"}
    parts = []
    for token in text.split():
        lower = token.lower()
        if lower in small:
            parts.append("S/A" if lower == "s/a" else ("S.A." if lower == "sa" else ("LTDA" if lower == "ltda" else lower)))
        else:
            parts.append(lower[:1].upper() + lower[1:])
    return " ".join(parts).strip()


def clean_phone(value: Any) -> str:
    phone = normalize_phone(value)
    if len(phone) == 11 and phone.startswith("0"):
        phone = phone[1:]
    if not phone or phone in BLOCKED_PHONES:
        return ""
    if len(phone) < 10 or phone.startswith("00") or len(set(phone)) <= 2:
        return ""
    return phone


def clean_email(value: Any) -> str:
    email = str(value or "").strip().lower()
    return email if "@" in email and "." in email else ""


def choose_best_phone(phones: Iterable[str]) -> str:
    unique = []
    for phone in phones:
        phone = clean_phone(phone)
        if phone and phone not in unique:
            unique.append(phone)
    if not unique:
        return ""
    mobiles = [phone for phone in unique if len(phone) == 11 and phone[2] == "9"]
    return (mobiles or unique)[0]


def load_casa_cache() -> Dict[str, Any]:
    if CASA_CACHE_JSON.exists():
        try:
            payload = json.loads(CASA_CACHE_JSON.read_text(encoding="utf-8"))
            return payload if isinstance(payload, dict) else {}
        except Exception:
            return {}
    return {}


def save_casa_cache(cache: Dict[str, Any]) -> None:
    CASA_CACHE_JSON.parent.mkdir(parents=True, exist_ok=True)
    CASA_CACHE_JSON.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")


def casa_for_cnpj(cnpj: str, cache: Dict[str, Any]) -> Dict[str, Any]:
    cnpj = normalize_cnpj(cnpj)
    if not cnpj:
        return {}
    cached = cache.get(cnpj)
    if isinstance(cached, dict) and int(cached.get("status_code") or 0) == 200:
        return cached
    api_key = str(get_config_value("casa_dos_dados_api_key", "") or "").strip()
    payload = fetch_casa_cnpj(cnpj, api_key)
    cache[cnpj] = payload
    save_casa_cache(cache)
    if int(payload.get("status_code") or 0) == 200:
        log(f"[CASA_OK] cnpj={cnpj}")
    else:
        log(f"[CASA_ERRO] cnpj={cnpj} status={payload.get('status_code')}")
    time.sleep(0.1)
    return payload


def company_similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, normalize_key(a), normalize_key(b)).ratio()


def classify_funnel(stage: str) -> str:
    value = normalize_key(stage)
    if "arquiv" in value or "perdid" in value:
        return "ARQUIVADO"
    if value == "entrada":
        return "FUNIL_PROSPECCAO"
    if value:
        return "FUNIL_VENDAS"
    return "FUNIL_PROSPECCAO"


def consolidate_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    deals: Dict[int, Dict[str, Any]] = {}
    for row in rows:
        try:
            deal_id = int(row_get(row, "Negócio - ID") or 0)
            org_id = int(row_get(row, "Organização - ID") or 0)
        except Exception:
            continue
        if not deal_id or not org_id:
            continue
        deal = deals.setdefault(
            deal_id,
            {
                "deal_id": deal_id,
                "org_id": org_id,
                "raw_names": [],
                "cnpjs": [],
                "phones": [],
                "emails": [],
                "stages": [],
                "person_ids": [],
                "stage_changed_at": "",
            },
        )
        for label in ("Negócio - Título", "Negócio - Organização", "Organização - Nome"):
            name = clean_name(row_get(row, label))
            if name and name not in deal["raw_names"]:
                deal["raw_names"].append(name)
        for label in ("Organização - CNPJ", "Pessoa - CNPJ"):
            cnpj = normalize_cnpj(row_get(row, label))
            if cnpj and cnpj not in deal["cnpjs"]:
                deal["cnpjs"].append(cnpj)
        for label in (
            "Pessoa - Telefone - Celular",
            "Pessoa - Telefone - Trabalho",
            "Pessoa - Telefone - Outros",
            "Pessoa - Telefone - Residencial",
        ):
            phone = clean_phone(row_get(row, label))
            if phone and phone not in deal["phones"]:
                deal["phones"].append(phone)
        email = clean_email(row_get(row, "Pessoa - E-mail") or row_get(row, "Pessoa - Email"))
        if email and email not in deal["emails"]:
            deal["emails"].append(email)
        stage = row_get(row, "Negócio - Etapa")
        if stage and stage not in deal["stages"]:
            deal["stages"].append(stage)
        stage_changed_at = row_get(row, "Negócio - Última alteração de etapa")
        if stage_changed_at and not deal.get("stage_changed_at"):
            deal["stage_changed_at"] = stage_changed_at
        person_id = row_get(row, "Pessoa - ID")
        if person_id and person_id not in deal["person_ids"]:
            deal["person_ids"].append(person_id)
    result = []
    for deal in deals.values():
        name = next((item for item in deal["raw_names"] if normalize_key(item) not in GENERIC_NAMES), "")
        deal["nome_final"] = title_name(name)
        deal["nome_key"] = normalize_key(name)
        deal["cnpj"] = deal["cnpjs"][0] if deal["cnpjs"] else ""
        deal["telefone"] = choose_best_phone(deal["phones"])
        deal["email"] = deal["emails"][0] if deal["emails"] else ""
        deal["stage"] = deal["stages"][0] if deal["stages"] else ""
        deal["funil_atual"] = classify_funnel(deal["stage"])
        result.append(deal)
    return result


def enrich_deals(deals: List[Dict[str, Any]]) -> Counter[str]:
    counters: Counter[str] = Counter()
    casa_cache = load_casa_cache()
    name_cache = load_name_cache()
    for deal in deals:
        cnpj = normalize_cnpj(deal.get("cnpj"))
        if cnpj:
            casa = casa_for_cnpj(cnpj, casa_cache)
            if int(casa.get("status_code") or 0) != 200:
                deal["cnpj"] = ""
                cnpj = ""
                counters["cnpj_invalido_removido"] += 1
        if not cnpj and deal.get("nome_final"):
            variants = variants_from_name(str(deal.get("nome_final") or ""), name_cache)
            found = choose_cnpj(str(deal.get("nome_final") or ""), variants, name_cache)
            cnpj = normalize_cnpj(found.get("cnpj"))
            if cnpj:
                deal["cnpj"] = cnpj
                counters["cnpj_encontrado"] += 1
                log(f"[CNPJ_ENCONTRADO] deal={deal['deal_id']} cnpj={cnpj} score={found.get('score')}")
        if cnpj:
            casa = casa_for_cnpj(cnpj, casa_cache)
            if int(casa.get("status_code") or 0) == 200:
                counters["casa_ok"] += 1
                casa_phone = choose_best_phone(casa.get("phones") or [])
                casa_email = next((clean_email(email) for email in casa.get("emails") or [] if clean_email(email)), "")
                company = title_name(casa.get("company_name") or "")
                if company and (not deal.get("nome_final") or company_similarity(str(deal.get("nome_final")), company) >= 0.55):
                    deal["nome_final"] = company
                    deal["nome_key"] = normalize_key(company)
                if (not clean_phone(deal.get("telefone"))) and casa_phone:
                    deal["telefone"] = casa_phone
                    counters["telefone_casa"] += 1
                if not clean_email(deal.get("email")) and casa_email:
                    deal["email"] = casa_email
                    counters["email_casa"] += 1
        deal["telefone"] = clean_phone(deal.get("telefone"))
        deal["email"] = clean_email(deal.get("email"))
    save_casa_cache(casa_cache)
    return counters


def assign_actions(deals: List[Dict[str, Any]]) -> Counter[str]:
    counters: Counter[str] = Counter()
    groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for deal in deals:
        cnpj = normalize_cnpj(deal.get("cnpj"))
        name_key = str(deal.get("nome_key") or "")
        key = f"cnpj::{cnpj}" if cnpj else (f"name::{name_key}" if name_key and name_key not in GENERIC_NAMES else f"deal::{deal['deal_id']}")
        deal["dedupe_key"] = key
        groups[key].append(deal)
    for group in groups.values():
        group.sort(key=lambda item: int(item["deal_id"]))
        master = group[0]
        if not clean_phone(master.get("telefone")):
            for item in group[1:]:
                phone = clean_phone(item.get("telefone"))
                if phone:
                    master["telefone"] = phone
                    counters["telefone_consolidado_master"] += 1
                    break
        if not clean_email(master.get("email")):
            for item in group[1:]:
                email = clean_email(item.get("email"))
                if email:
                    master["email"] = email
                    counters["email_consolidado_master"] += 1
                    break
        if not normalize_cnpj(master.get("cnpj")):
            for item in group[1:]:
                cnpj = normalize_cnpj(item.get("cnpj"))
                if cnpj:
                    master["cnpj"] = cnpj
                    counters["cnpj_consolidado_master"] += 1
                    break
        group_has_sales = any(item.get("funil_atual") == "FUNIL_VENDAS" for item in group)
        for item in group:
            item["master_deal_id"] = master["deal_id"]
            archived_today = item.get("funil_atual") == "ARQUIVADO" and str(item.get("stage_changed_at") or "").startswith("2026-04-07")
            if item is master:
                if archived_today:
                    item["acao"] = "RESTAURAR_VENDAS"
                    item["funil_destino"] = "FUNIL_VENDAS"
                    item["restaurar_motivo"] = "arquivado_em_2026-04-07"
                    counters["restaurar_vendas"] += 1
                    counters["restaurar_por_data"] += 1
                elif group_has_sales and item.get("funil_atual") != "FUNIL_VENDAS":
                    item["acao"] = "RESTAURAR_VENDAS"
                    item["funil_destino"] = "FUNIL_VENDAS"
                    item["restaurar_motivo"] = "grupo_com_deal_em_vendas"
                    counters["restaurar_vendas"] += 1
                else:
                    item["acao"] = "UPDATE"
                    item["funil_destino"] = item.get("funil_atual") or "FUNIL_PROSPECCAO"
                    item["restaurar_motivo"] = ""
                    counters["update"] += 1
                counters["master"] += 1
            else:
                item["acao"] = "ARCHIVE"
                item["funil_destino"] = "ARQUIVADO"
                item["restaurar_motivo"] = "duplicado"
                counters["duplicado"] += 1
    return counters


def write_output(deals: List[Dict[str, Any]], counters: Counter[str]) -> None:
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "writeback"
    headers = [
        "deal_id", "org_id", "master_deal_id", "nome_final", "cnpj", "telefone", "email",
        "funil_atual", "funil_destino", "acao", "restaurar_motivo", "stage_atual", "stage_changed_at", "dedupe_key", "score",
    ]
    ws.append(headers)
    order = {"RESTAURAR_VENDAS": 0, "UPDATE": 1, "ARCHIVE": 3}
    for deal in sorted(deals, key=lambda item: (order.get(item.get("acao"), 9), int(item["deal_id"]))):
        score = 0
        score += 2 if clean_phone(deal.get("telefone")) else 0
        score += 2 if normalize_cnpj(deal.get("cnpj")) else 0
        score += 1 if clean_email(deal.get("email")) else 0
        score += 1 if normalize_key(deal.get("nome_final")) not in GENERIC_NAMES else 0
        ws.append([
            deal.get("deal_id"),
            deal.get("org_id"),
            deal.get("master_deal_id"),
            deal.get("nome_final"),
            deal.get("cnpj"),
            deal.get("telefone"),
            deal.get("email"),
            deal.get("funil_atual"),
            deal.get("funil_destino"),
            deal.get("acao"),
            deal.get("restaurar_motivo"),
            deal.get("stage"),
            deal.get("stage_changed_at"),
            deal.get("dedupe_key"),
            score,
        ])
    metrics = wb.create_sheet("resumo")
    metrics.append(["metrica", "valor"])
    for key, value in sorted(counters.items()):
        metrics.append([key, value])
    metrics.append(["total_deals", len(deals)])
    metrics.append(["com_cnpj", sum(1 for item in deals if normalize_cnpj(item.get("cnpj")))])
    metrics.append(["com_telefone", sum(1 for item in deals if clean_phone(item.get("telefone")))])
    metrics.append(["com_email", sum(1 for item in deals if clean_email(item.get("email")))])
    OUTPUT_XLSX.parent.mkdir(parents=True, exist_ok=True)
    try:
        wb.save(OUTPUT_XLSX)
    except PermissionError:
        fallback = OUTPUT_XLSX.with_name(OUTPUT_XLSX.stem + "_v2.xlsx")
        try:
            wb.save(fallback)
            print(f"[EXPORTADO_FALLBACK] {fallback}", flush=True)
        except PermissionError:
            stamped = OUTPUT_XLSX.with_name(OUTPUT_XLSX.stem + "_" + datetime.now().strftime("%Y%m%d_%H%M%S") + ".xlsx")
            try:
                wb.save(stamped)
                print(f"[EXPORTADO_FALLBACK] {stamped}", flush=True)
            except PermissionError:
                data_fallback = Path(r"C:\Users\Asus\legacy\bot_sdr_ai\data") / stamped.name
                wb.save(data_fallback)
                print(f"[EXPORTADO_FALLBACK] {data_fallback}", flush=True)


def main() -> int:
    raw = parse_xlsx(INPUT_XLSX)
    deals = consolidate_rows(raw)
    counters = Counter({"linhas_origem": len(raw), "deals_unicos": len(deals)})
    log(f"[CARREGADO] linhas={len(raw)} deals_unicos={len(deals)}")
    counters.update(enrich_deals(deals))
    counters.update(assign_actions(deals))
    write_output(deals, counters)
    log("[RESUMO] " + " ".join(f"{key}={value}" for key, value in sorted(counters.items())))
    log(f"[EXPORTADO] {OUTPUT_XLSX}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
