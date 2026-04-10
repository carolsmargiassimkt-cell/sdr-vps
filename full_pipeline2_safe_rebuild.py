from __future__ import annotations

import argparse
import logging
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List

from crm.crm_orchestrator import CRMEnrichmentLoop, _clean_company_display_name, _normalizar_telefone_br
from crm.pipedrive_client import PipedriveClient


PIPELINE_ID = 2
DEFAULT_BATCH_SIZE = 20
DEFAULT_BATCH_DELAY_SEC = 15.0
DEFAULT_DEAL_TIMEOUT_SEC = 180.0
DEFAULT_INITIAL_FETCH_RETRIES = 5
DEFAULT_INITIAL_FETCH_DELAY_SEC = 90.0


def _chunked(items: List[Dict[str, Any]], size: int) -> Iterable[List[Dict[str, Any]]]:
    for index in range(0, len(items), max(1, int(size))):
        yield items[index:index + max(1, int(size))]


class Pipeline2SafeRebuild:
    def __init__(self, base_dir: Path, *, batch_size: int, batch_delay_sec: float) -> None:
        self.base_dir = Path(base_dir)
        self.batch_size = max(1, int(batch_size))
        self.batch_delay_sec = max(0.0, float(batch_delay_sec))
        self.logger = self._build_logger()
        self.crm = PipedriveClient(logger=self.logger)
        self.loop = CRMEnrichmentLoop(self.crm, self.logger, self.base_dir)
        self.loop.FETCH_LIMIT = 2000
        self.loop.MAX_EMPRESAS = 2000
        self.loop.TURBO_WORKERS = 1
        self.phone_owner_registry: Dict[str, Dict[str, Any]] = {}
        self.org_cache: Dict[int, Dict[str, Any]] = {}
        self.org_people_cache: Dict[int, List[Dict[str, Any]]] = {}
        self.person_cache: Dict[int, Dict[str, Any]] = {}
        self.org_snapshot_cache: Dict[int, Dict[str, Any]] = {}
        self.deal_timeout_sec = DEFAULT_DEAL_TIMEOUT_SEC
        self.initial_fetch_retries = DEFAULT_INITIAL_FETCH_RETRIES
        self.initial_fetch_delay_sec = DEFAULT_INITIAL_FETCH_DELAY_SEC

    def _build_logger(self) -> logging.Logger:
        logs_dir = self.base_dir / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)
        logger = logging.getLogger("pipeline2_safe_rebuild")
        logger.setLevel(logging.INFO)
        logger.handlers = []
        formatter = logging.Formatter("%(message)s")
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_handler = logging.FileHandler(logs_dir / f"pipeline2_safe_rebuild_{stamp}.log", encoding="utf-8")
        file_handler.setFormatter(formatter)
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)
        logger.propagate = False
        return logger

    def log(self, message: str) -> None:
        self.logger.info(str(message))

    def run(self) -> int:
        self.log("[RUN_START] pipeline=2")
        self._reset_runtime_caches()
        deals = self._fetch_pipeline_deals()
        if not deals:
            self.log("[RUN_ABORT] motivo=sem_deals_pipeline_2")
            return 1

        self._initial_cleanup(deals)
        masters = self._prepare_master_deals_fast(deals)
        if not masters:
            self.log("[RUN_ABORT] motivo=sem_master_deals")
            return 1
        masters = self._prioritize_masters(masters)

        counters = {
            "masters": 0,
            "cnpj_encontrado": 0,
            "cnpj_nao_encontrado": 0,
            "casa_ok": 0,
            "enrichment_completo": 0,
        }

        total_batches = (len(masters) + self.batch_size - 1) // self.batch_size
        for batch_index, batch in enumerate(_chunked(masters, self.batch_size), start=1):
            self.log(f"[BATCH_START] bloco={batch_index}/{total_batches} tamanho={len(batch)}")
            for deal in batch:
                result = self._process_master_deal_with_guard(deal)
                counters["masters"] += 1
                for key in ("cnpj_encontrado", "cnpj_nao_encontrado", "casa_ok", "enrichment_completo"):
                    counters[key] += int(result.get(key) or 0)
            self.log(f"[PROGRESSO] lote={batch_index} total_processado={counters['masters']}")
            self.log(f"[BATCH_DONE] bloco={batch_index}/{total_batches}")
            if batch_index < total_batches and self.batch_delay_sec > 0:
                time.sleep(self.batch_delay_sec)

        self.log(
            "[RUN_DONE] "
            f"masters={counters['masters']} "
            f"cnpj_encontrado={counters['cnpj_encontrado']} "
            f"cnpj_nao_encontrado={counters['cnpj_nao_encontrado']} "
            f"casa_ok={counters['casa_ok']} "
            f"enrichment_completo={counters['enrichment_completo']}"
        )
        return 0

    def _reset_runtime_caches(self) -> None:
        state = self.loop._get_runtime_state()
        state[self.loop.PHONE_CACHE_KEY] = {}
        state[self.loop.PHONE_RESULTS_KEY] = {}
        state[self.loop.CNPJ_CACHE_KEY] = {}
        self.loop._set_runtime_state(state)
        self.log("[CACHE_RESET] escopo=runtime_enrichment")

    def _fetch_pipeline_deals(self) -> List[Dict[str, Any]]:
        for attempt in range(1, self.initial_fetch_retries + 1):
            deals = self.loop._safe_pipedrive_call(
                lambda: self.crm.get_deals(status="all_not_deleted", limit=2000, pipeline_id=PIPELINE_ID)
            ) or []
            output = [
                dict(deal)
                for deal in list(deals or [])
                if isinstance(deal, dict) and int(deal.get("pipeline_id") or 0) == PIPELINE_ID
            ]
            if output:
                self.log(f"[FETCH_OK] pipeline=2 deals={len(output)}")
                return output
            if int(getattr(self.crm, "last_http_status", 0) or 0) == 429 and attempt < self.initial_fetch_retries:
                self.log(
                    f"[FETCH_RETRY] tentativa={attempt}/{self.initial_fetch_retries} "
                    f"aguardando={int(self.initial_fetch_delay_sec)}s motivo=rate_limit"
                )
                time.sleep(self.initial_fetch_delay_sec)
                continue
            break
        self.log("[FETCH_OK] pipeline=2 deals=0")
        return []

    def _initial_cleanup(self, deals: List[Dict[str, Any]]) -> None:
        self.log("[LIMPEZA_INICIAL]")
        removed_phones = self._cleanup_contaminated_phones(deals)
        removed_addresses = 0
        self.log(
            f"[LIMPEZA_OK] orgs={len({self.loop._extract_id((deal or {}).get('org_id') or {}) for deal in list(deals or [])})} telefones_removidos={removed_phones} "
            f"enderecos_removidos={removed_addresses}"
        )

    def _cleanup_contaminated_phones(self, deals: List[Dict[str, Any]]) -> int:
        person_orgs: Dict[int, Dict[str, Any]] = {}
        for deal in list(deals or []):
            person = (deal or {}).get("person_id") or {}
            person_id = self.loop._extract_id(person)
            org = (deal or {}).get("org_id") or {}
            org_id = self.loop._extract_id(org)
            org_name = str((org or {}).get("name") if isinstance(org, dict) else "").strip()
            if person_id <= 0 or org_id <= 0:
                continue
            entry = person_orgs.setdefault(person_id, {"org_ids": set(), "org_names": set()})
            entry["org_ids"].add(org_id)
            if org_name:
                entry["org_names"].add(self._relation_key(org_name, ""))

        conflict_person_ids = [
            person_id
            for person_id, entry in person_orgs.items()
            if len(entry.get("org_ids") or set()) > 1 and len(entry.get("org_names") or set()) > 1
        ]
        if not conflict_person_ids:
            return 0

        removed = 0
        for person_id in conflict_person_ids:
            person = self._get_cached_person(person_id)
            phones = self._normalize_person_phone_list(person)
            if not phones:
                continue
            impacted_orgs = sorted(int(item) for item in list(person_orgs.get(person_id, {}).get("org_ids") or []))
            for phone in phones:
                if self._remove_phone_from_person(person_id, phone):
                    removed += 1
                    self.log(
                        f"[TELEFONE_REMOVIDO] person={person_id} telefone={phone} "
                        f"motivo=contaminado orgs={','.join(str(org_id) for org_id in impacted_orgs[:20])}"
                    )
        return removed

    def _remove_phone_from_person(self, person_id: int, phone_to_remove: str) -> bool:
        if not person_id:
            return False
        person = self._get_cached_person(person_id)
        if not person:
            return False
        merged: List[str] = []
        for item in person.get("phone") or []:
            value = item.get("value") if isinstance(item, dict) else item
            normalized = _normalizar_telefone_br(value, self.loop.whatsapp)
            if normalized and normalized != phone_to_remove and normalized not in merged:
                merged.append(normalized)
        return self._update_person_if_changed(person_id, person, {"phone": merged})

    def _process_master_deal(self, deal: Dict[str, Any]) -> Dict[str, int]:
        counters = {
            "cnpj_encontrado": 0,
            "cnpj_nao_encontrado": 0,
            "casa_ok": 0,
            "enrichment_completo": 0,
        }
        deal_id = int((deal or {}).get("id") or 0)
        org = (deal or {}).get("org_id") or {}
        org_id = self.loop._extract_id(org)
        if not deal_id or not org_id:
            return counters

        organization = self._get_cached_org(org_id, deal=deal)
        org_name = str((organization or {}).get("name") or (org or {}).get("name") or "").strip()
        if not org_name:
            return counters

        self.log(f"[PROCESSANDO] deal={deal_id} empresa={org_name}")
        self.log(f"[PROCESSANDO_MASTER] deal={deal_id} org={org_id} nome={org_name}")
        org_people = self._get_cached_org_people(org_id)
        current_cnpj = re.sub(r"\D+", "", str(self.crm.extract_cnpj(organization) or ""))
        target_cnpj = current_cnpj
        if target_cnpj:
            counters["cnpj_encontrado"] += 1
            self.log(f"[CNPJ_ENCONTRADO] org={org_id} cnpj={target_cnpj} origem=crm")
        else:
            target_cnpj = self.loop._discover_cnpj(org_name)
            if target_cnpj:
                counters["cnpj_encontrado"] += 1
                self.log(f"[CNPJ_ENCONTRADO] org={org_id} cnpj={target_cnpj} origem=discovery")
            else:
                counters["cnpj_nao_encontrado"] += 1
                self.log(f"[CNPJ_NAO_ENCONTRADO] org={org_id} nome={org_name}")

        enrichment = None
        if target_cnpj:
            enrichment = self.loop._enrich_by_cnpj(target_cnpj)
            if enrichment:
                counters["casa_ok"] += 1
                self.log(f"[CASA_DADOS_OK] org={org_id} cnpj={target_cnpj}")
                self._update_organization_if_needed_fast(organization, enrichment)
                self._update_address_if_missing(organization, enrichment)

        ranked_phones = self.loop._get_ranked_company_phones(org_name, known_cnpj=target_cnpj)
        safe_ranked_phones = self._filter_phone_conflicts(ranked_phones, org_id=org_id, org_name=org_name, cnpj=target_cnpj)
        if enrichment and getattr(enrichment, "phones", None) and not safe_ranked_phones:
            self.log(f"[TELEFONE_DESCARTADO] org={org_id} motivo=sem_telefone_seguro_pos_validacao")

        person_action = self.loop._ensure_person_for_deal(
            deal,
            org_id,
            list(org_people or []),
            enrichment=enrichment,
            ranked_phones=safe_ranked_phones,
            candidate_name="",
        )

        if safe_ranked_phones:
            self.loop._store_phone_result(org_id, org_name, safe_ranked_phones)
            self.loop._mark_phone_enriched_today(org_id, safe_ranked_phones)
        else:
            self.loop._mark_phone_enriched_today(org_id, [])

        has_contact = bool(safe_ranked_phones) or bool(org_people) or person_action in {"created", "updated", "exists"}
        if target_cnpj or has_contact:
            counters["enrichment_completo"] += 1
            self.log(
                f"[ENRICHMENT_COMPLETO] org={org_id} deal={deal_id} "
                f"cnpj={target_cnpj or 'na'} contato={int(has_contact)}"
            )
        self.log(f"[DEAL_OK] deal={deal_id}")
        return counters

    def _process_master_deal_with_guard(self, deal: Dict[str, Any]) -> Dict[str, int]:
        started_at = time.perf_counter()
        deal_id = int((deal or {}).get("id") or 0)
        try:
            result = self._process_master_deal(deal)
            elapsed = time.perf_counter() - started_at
            if elapsed > float(self.deal_timeout_sec):
                self.log(f"[DEAL_TIMEOUT_SKIP] deal={deal_id} elapsed_sec={int(elapsed)}")
            return result
        except Exception as exc:
            elapsed = time.perf_counter() - started_at
            self.log(
                f"[DEAL_TIMEOUT_SKIP] deal={deal_id} elapsed_sec={int(elapsed)} motivo={str(exc)[:160]}"
            )
            return {
                "cnpj_encontrado": 0,
                "cnpj_nao_encontrado": 0,
                "casa_ok": 0,
                "enrichment_completo": 0,
            }

    def _prepare_master_deals_fast(self, deals: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        groups: Dict[str, List[Dict[str, Any]]] = {}
        for deal in list(deals or []):
            key = self._duplicate_group_key_fast(deal)
            if not key:
                continue
            groups.setdefault(key, []).append(dict(deal))
        masters: List[Dict[str, Any]] = []
        for grouped_deals in groups.values():
            master = self.loop._select_master_deal(grouped_deals)
            if not master:
                continue
            duplicate_deals = [deal for deal in grouped_deals if int((deal or {}).get("id") or 0) != int((master or {}).get("id") or 0)]
            if duplicate_deals:
                self.log(
                    f"[DEAL_MASTER_DEFINIDO] master={int((master or {}).get('id') or 0)} "
                    f"duplicados={','.join(str(int((deal or {}).get('id') or 0)) for deal in duplicate_deals)}"
                )
                self.loop._consolidate_duplicates_into_master(master, duplicate_deals)
            masters.append(master)
        return masters

    def _duplicate_group_key_fast(self, deal: Dict[str, Any]) -> str:
        org = deal.get("org_id") or {}
        org_id = self.loop._extract_id(org)
        organization = self._get_cached_org(org_id, deal=deal) if org_id else {}
        cnpj = re.sub(r"\D+", "", str(self.crm.extract_cnpj(organization) or ""))
        if cnpj:
            return f"cnpj:{cnpj}"
        org_name = str((organization or {}).get("name") or (org or {}).get("name") or deal.get("org_name") or "").strip()
        normalized = self.loop._normalize_org_name(_clean_company_display_name(org_name) or org_name)
        return f"name:{normalized}" if normalized else ""

    def _prioritize_masters(self, deals: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        ordered = sorted(list(deals or []), key=self._master_priority_key)
        self.log(f"[PRIORIDADE_OK] masters={len(ordered)}")
        return ordered

    def _master_priority_key(self, deal: Dict[str, Any]) -> tuple[int, int, int]:
        org = deal.get("org_id") or {}
        org_id = self.loop._extract_id(org)
        organization = self._get_cached_org(org_id, deal=deal)
        people = self.org_people_cache.get(org_id) or []
        cnpj = re.sub(r"\D+", "", str(self.crm.extract_cnpj(organization) or ""))
        has_phone = any(
            _normalizar_telefone_br(
                item.get("value") if isinstance(item, dict) else item,
                self.loop.whatsapp,
            )
            for person in list(people)
            for item in list((person or {}).get("phone") or [])
        )
        has_address = bool(str((organization or {}).get("address") or "").strip())
        missing_score = 0
        if not cnpj:
            missing_score += 4
        if not has_phone:
            missing_score += 2
        if not has_address:
            missing_score += 1
        return (-missing_score, int(not bool(cnpj)), int((deal or {}).get("id") or 0))

    def _get_cached_org(self, org_id: int, deal: Dict[str, Any] | None = None) -> Dict[str, Any]:
        if org_id <= 0:
            return {}
        cached = self.org_cache.get(org_id)
        if isinstance(cached, dict):
            return dict(cached)
        org_name = ""
        if isinstance(deal, dict):
            org = deal.get("org_id") or {}
            org_name = str((org or {}).get("name") if isinstance(org, dict) else "").strip()
        organization = self.loop._get_org_cached(org_id)
        if isinstance(organization, dict):
            self.org_cache[org_id] = dict(organization)
            return dict(organization)
        fallback = {"id": int(org_id)}
        if org_name:
            fallback["name"] = org_name
        self.org_cache[org_id] = dict(fallback)
        return dict(fallback)

    def _get_cached_org_people(self, org_id: int) -> List[Dict[str, Any]]:
        cached = self.org_people_cache.get(org_id)
        if isinstance(cached, list):
            return [dict(person) for person in list(cached) if isinstance(person, dict)]
        people = self.loop._safe_pipedrive_call(lambda org_id=org_id: self.crm.get_organization_persons(org_id, limit=100)) or []
        normalized_people = [dict(person) for person in list(people or []) if isinstance(person, dict)]
        self.org_people_cache[org_id] = normalized_people
        for person in normalized_people:
            person_id = int(person.get("id") or 0)
            if person_id:
                self.person_cache[person_id] = dict(person)
        return [dict(person) for person in normalized_people]

    def _get_cached_person(self, person_id: int) -> Dict[str, Any]:
        if person_id <= 0:
            return {}
        cached = self.person_cache.get(person_id)
        if isinstance(cached, dict):
            return dict(cached)
        person = self.loop._safe_pipedrive_call(lambda person_id=person_id: self.crm.get_person_details(person_id))
        if isinstance(person, dict):
            self.person_cache[person_id] = dict(person)
        return dict(person or {})

    def _normalize_person_phone_list(self, person: Dict[str, Any]) -> List[str]:
        output: List[str] = []
        for item in person.get("phone") or []:
            value = item.get("value") if isinstance(item, dict) else item
            normalized = _normalizar_telefone_br(value, self.loop.whatsapp)
            if normalized and normalized not in output:
                output.append(normalized)
        return output[:5]

    def _update_person_if_changed(self, person_id: int, current_person: Dict[str, Any], payload: Dict[str, Any]) -> bool:
        if not person_id or not payload:
            return False
        normalized_payload = dict(payload)
        if "phone" in normalized_payload:
            new_phones = [phone for phone in list(normalized_payload.get("phone") or []) if phone]
            if self._normalize_person_phone_list(current_person) == new_phones[:5]:
                self.log(f"[SKIP_UPDATE] person={person_id} campo=phone motivo=igual")
                return False
            normalized_payload["phone"] = new_phones[:5]
        updated = bool(self.loop._safe_pipedrive_call(lambda: self.crm.update_person(person_id, normalized_payload)))
        if updated:
            refreshed = dict(current_person)
            if "phone" in normalized_payload:
                refreshed["phone"] = [{"value": phone} for phone in list(normalized_payload.get("phone") or [])]
            self.person_cache[person_id] = refreshed
        return updated

    def _update_org_if_changed(self, org_id: int, current_org: Dict[str, Any], payload: Dict[str, Any]) -> bool:
        if not org_id or not payload:
            return False
        normalized_payload: Dict[str, Any] = {}
        for key, value in dict(payload or {}).items():
            current_value = current_org.get(key)
            if str(current_value or "").strip() == str(value or "").strip():
                self.log(f"[SKIP_UPDATE] org={org_id} campo={key} motivo=igual")
                continue
            normalized_payload[key] = value
        if not normalized_payload:
            return False
        updated = bool(self.loop._safe_pipedrive_call(lambda: self.crm.update_organization(org_id, normalized_payload)))
        if updated:
            refreshed = dict(current_org)
            refreshed.update(normalized_payload)
            self.org_cache[org_id] = refreshed
            self.loop._cycle_org_cache[org_id] = dict(refreshed)
        return updated

    def _update_organization_if_needed_fast(self, organization: Dict[str, Any], enrichment: Any) -> bool:
        if not organization or not enrichment:
            return False
        payload: Dict[str, Any] = {}
        existing_cnpj = self.crm.extract_cnpj(organization)
        if not existing_cnpj and getattr(enrichment, "cnpj", ""):
            payload.update(self.crm.build_org_fields(enrichment.cnpj, enrichment.primary_cnae))
        elif not str(organization.get(self.crm.CNAE_FIELD_KEY) or "").strip() and getattr(enrichment, "primary_cnae", ""):
            payload[self.crm.CNAE_FIELD_KEY] = enrichment.primary_cnae

        current_name = str(organization.get("name") or "").strip()
        preferred_name = _clean_company_display_name(
            getattr(enrichment, "trade_name", "") or getattr(enrichment, "company_name", "") or current_name
        )
        current_name_clean = _clean_company_display_name(current_name)
        if preferred_name and (
            self.loop._is_generic_org_name(current_name)
            or (current_name_clean and preferred_name.lower() != current_name_clean.lower() and len(preferred_name.split()) <= len(current_name_clean.split()))
        ):
            payload["name"] = preferred_name
            self.log(f"[NOME_FANTASIA_DEFINIDO] org={int(organization.get('id') or 0)} nome={preferred_name}")

        sanitized: Dict[str, Any] = {}
        for key, value in payload.items():
            if value in (None, "", [], {}):
                continue
            if key == self.crm.CNAE_FIELD_KEY:
                value = str(value).strip()[:64]
            if key == "name":
                value = str(value).strip()[:200]
            if value not in (None, "", [], {}):
                sanitized[key] = value
        return self._update_org_if_changed(int(organization.get("id") or 0), organization, sanitized)

    def _filter_phone_conflicts(
        self,
        ranked_phones: List[Dict[str, Any]],
        *,
        org_id: int,
        org_name: str,
        cnpj: str,
    ) -> List[Dict[str, Any]]:
        filtered: List[Dict[str, Any]] = []
        owner_key = self._relation_key(org_name, cnpj)
        for item in list(ranked_phones or []):
            phone = _normalizar_telefone_br(item.get("phone"), self.loop.whatsapp)
            if not phone:
                continue
            existing_owner = self.phone_owner_registry.get(phone) or {}
            existing_key = self._relation_key(existing_owner.get("name"), existing_owner.get("cnpj"))
            if existing_owner and existing_key and owner_key and existing_key != owner_key:
                self.log(
                    f"[TELEFONE_DESCARTADO] org={org_id} telefone={phone} motivo=conflito_outra_empresa"
                )
                continue
            self.phone_owner_registry[phone] = {
                "org_id": int(org_id),
                "name": str(org_name or "").strip(),
                "cnpj": re.sub(r"\D+", "", str(cnpj or "")),
            }
            filtered.append(dict(item))
        return filtered[:3]

    def _update_address_if_missing(self, organization: Dict[str, Any], enrichment: Any) -> None:
        current_address = str((organization or {}).get("address") or "").strip()
        if current_address:
            return
        raw = getattr(enrichment, "raw", {}) or {}
        address = raw.get("endereco") or {}
        if not isinstance(address, dict):
            return
        parts = [
            str(address.get("logradouro") or "").strip(),
            str(address.get("numero") or "").strip(),
            str(address.get("bairro") or "").strip(),
            str(address.get("municipio") or "").strip(),
            str(address.get("uf") or "").strip(),
            str(address.get("cep") or "").strip(),
        ]
        formatted = ", ".join([part for part in parts if part])
        if not formatted:
            return
        org_id = int((organization or {}).get("id") or 0)
        if not org_id:
            return
        updated = self._update_org_if_changed(org_id, organization, {"address": formatted[:255]})
        if updated:
            self.log(f"[ENDERECO_ATUALIZADO] org={org_id} endereco={formatted[:255]}")

    @staticmethod
    def _normalize_address(value: Any) -> str:
        raw = str(value or "").strip().lower()
        raw = re.sub(r"[^a-z0-9]+", " ", raw)
        raw = re.sub(r"\s+", " ", raw).strip()
        return raw if len(raw) >= 12 else ""

    @staticmethod
    def _relation_key(name: Any, cnpj: Any) -> str:
        clean_cnpj = re.sub(r"\D+", "", str(cnpj or ""))
        if len(clean_cnpj) == 14:
            return f"cnpj:{clean_cnpj}"
        normalized_name = re.sub(r"[^a-z0-9]+", "", _clean_company_display_name(name).lower())
        return f"name:{normalized_name}" if normalized_name else ""


def main() -> int:
    parser = argparse.ArgumentParser(description="Reconstrucao segura do enrichment do pipeline 2")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--batch-delay-sec", type=float, default=DEFAULT_BATCH_DELAY_SEC)
    args = parser.parse_args()
    runner = Pipeline2SafeRebuild(Path(__file__).resolve().parent, batch_size=args.batch_size, batch_delay_sec=args.batch_delay_sec)
    return runner.run()


if __name__ == "__main__":
    raise SystemExit(main())
