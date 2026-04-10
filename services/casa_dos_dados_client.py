from __future__ import annotations

import json
import re
import ssl
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List

from config.config_loader import get_config_value
from core.local_crm_cache import LocalCRMCache


class CasaDosDadosError(RuntimeError):
    pass


@dataclass
class CasaDosDadosResult:
    cnpj: str
    company_name: str
    trade_name: str
    phones: List[str]
    emails: List[str]
    websites: List[str]
    decision_makers: List[str]
    city: str
    state: str
    primary_cnae: str
    main_partner: str
    raw: Dict[str, Any]

    def as_record_patch(self) -> Dict[str, Any]:
        return {
            "empresa": self.trade_name or self.company_name,
            "nome": self.trade_name or self.company_name,
            "telefone": self.phones[0] if self.phones else "",
            "email": self.emails[0] if self.emails else "",
            "phones": self.phones,
            "emails": self.emails,
            "decision_makers": self.decision_makers,
            "websites": self.websites,
            "cidade": self.city,
            "estado": self.state,
            "cnae": self.primary_cnae,
            "socio_principal": self.main_partner,
            "enriched_source": "casa_dos_dados",
            "raw_enrichment": self.raw,
        }


class CasaDosDadosClient:
    def __init__(self, *, timeout: float = 30.0) -> None:
        self.timeout = timeout
        self.api_key = str(get_config_value("casa_dos_dados_api_key", "") or "").strip()
        self.base_url = "https://api.casadosdados.com.br"
        if not self.api_key:
            raise CasaDosDadosError("CASA_DOS_DADOS_API_KEY ausente")

    def fetch_by_cnpj(self, cnpj: str) -> CasaDosDadosResult:
        normalized = LocalCRMCache.normalize_cnpj(cnpj)
        if not normalized:
            raise CasaDosDadosError("CNPJ inválido")
        url = f"{self.base_url}/v4/cnpj/{urllib.parse.quote(normalized)}"
        req = urllib.request.Request(
            url,
            headers={
                "api-key": self.api_key,
                "accept": "application/json",
                "user-agent": "bot-sdr-ai/1.0",
            },
            method="GET",
        )
        context = ssl.create_default_context()
        try:
            with urllib.request.urlopen(req, timeout=self.timeout, context=context) as response:
                raw = json.loads(response.read().decode("utf-8", errors="replace"))
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise CasaDosDadosError(f"HTTP {exc.code}: {body[:400]}") from exc
        except urllib.error.URLError as exc:
            raise CasaDosDadosError(f"Falha de rede: {exc}") from exc

        payload = raw if isinstance(raw, dict) else {}
        company_name = self._first_text(
            payload,
            "razao_social",
            "nome_empresarial",
            "company_name",
            "nome",
        )
        trade_name = self._first_text(
            payload,
            "nome_fantasia",
            "fantasia",
            "trade_name",
        )
        phones = self._extract_phones(payload)
        emails = self._extract_emails(payload)
        websites = self._extract_websites(payload)
        decision_makers = self._extract_contacts(payload)
        city, state = self._extract_city_state(payload)
        primary_cnae = self._extract_primary_cnae(payload)
        main_partner = self._extract_main_partner(payload, decision_makers)
        return CasaDosDadosResult(
            cnpj=normalized,
            company_name=company_name,
            trade_name=trade_name,
            phones=phones,
            emails=emails,
            websites=websites,
            decision_makers=decision_makers,
            city=city,
            state=state,
            primary_cnae=primary_cnae,
            main_partner=main_partner,
            raw=payload,
        )

    def discover_cnpj_by_google(self, org_name: str) -> str:
        query = urllib.parse.quote_plus(f'"{str(org_name or "").strip()}" CNPJ')
        url = f"https://www.google.com/search?q={query}&hl=pt-BR&num=5"
        payload = self._fetch_text(url)
        return self._extract_first_cnpj(payload)

    def discover_cnpj_by_casa_search(self, org_name: str) -> str:
        query = urllib.parse.quote_plus(str(org_name or "").strip())
        candidates = [
            f"https://www.casadosdados.com.br/solucao/cnpj?q={query}",
            f"https://www.casadosdados.com.br/consulta-cnpj/{query}",
        ]
        for url in candidates:
            payload = self._fetch_text(url)
            cnpj = self._extract_first_cnpj(payload)
            if cnpj:
                return cnpj
        return ""

    def fetch_receitaws_by_cnpj(self, cnpj: str) -> Dict[str, Any]:
        normalized = LocalCRMCache.normalize_cnpj(cnpj)
        if not normalized:
            raise CasaDosDadosError("CNPJ inválido")
        url = f"https://www.receitaws.com.br/v1/cnpj/{urllib.parse.quote(normalized)}"
        raw = self._fetch_json(url)
        return raw if isinstance(raw, dict) else {}

    def enrich_company(self, cnpj: str) -> CasaDosDadosResult:
        try:
            return self.fetch_by_cnpj(cnpj)
        except Exception:
            fallback = self.fetch_receitaws_by_cnpj(cnpj)
            merged = self._merge_receitaws_payload(cnpj, fallback)
            return merged

    def _fetch_text(self, url: str) -> str:
        req = urllib.request.Request(
            url,
            headers={
                "accept": "text/html,application/json",
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) bot-sdr-ai/1.0",
            },
            method="GET",
        )
        context = ssl.create_default_context()
        try:
            with urllib.request.urlopen(req, timeout=self.timeout, context=context) as response:
                return response.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise CasaDosDadosError(f"HTTP {exc.code}: {body[:400]}") from exc
        except urllib.error.URLError as exc:
            raise CasaDosDadosError(f"Falha de rede: {exc}") from exc

    def _fetch_json(self, url: str) -> Dict[str, Any]:
        payload = self._fetch_text(url)
        try:
            raw = json.loads(payload)
        except Exception as exc:
            raise CasaDosDadosError(f"JSON inválido: {payload[:200]}") from exc
        return raw if isinstance(raw, dict) else {}

    @staticmethod
    def _extract_first_cnpj(text: str) -> str:
        for match in re.finditer(r"\b(\d{2}\.?\d{3}\.?\d{3}/?\d{4}-?\d{2})\b", str(text or "")):
            normalized = LocalCRMCache.normalize_cnpj(match.group(1))
            if normalized:
                return normalized
        return ""

    def _merge_receitaws_payload(self, cnpj: str, payload: Dict[str, Any]) -> CasaDosDadosResult:
        company_name = self._first_text(payload, "nome")
        trade_name = self._first_text(payload, "fantasia")
        phone = self._first_text(payload, "telefone")
        email = self._first_text(payload, "email")
        city = self._first_text(payload, "municipio")
        state = self._first_text(payload, "uf")
        primary_cnae = ""
        activities = payload.get("atividade_principal") or []
        if isinstance(activities, list) and activities:
            first = activities[0]
            if isinstance(first, dict):
                primary_cnae = self._first_text(first, "code", "text")
        main_partner = ""
        qsa = payload.get("qsa") or []
        if isinstance(qsa, list) and qsa:
            first = qsa[0]
            if isinstance(first, dict):
                main_partner = self._first_text(first, "nome", "qual")
        phones = [LocalCRMCache.normalize_phone(phone)] if LocalCRMCache.normalize_phone(phone) else []
        emails = [LocalCRMCache.normalize_email(email)] if LocalCRMCache.normalize_email(email) else []
        decision_makers = [main_partner] if main_partner else []
        return CasaDosDadosResult(
            cnpj=LocalCRMCache.normalize_cnpj(cnpj),
            company_name=company_name,
            trade_name=trade_name,
            phones=phones,
            emails=emails,
            websites=[],
            decision_makers=decision_makers,
            city=city,
            state=state,
            primary_cnae=primary_cnae,
            main_partner=main_partner,
            raw=payload if isinstance(payload, dict) else {},
        )

    @staticmethod
    def _walk(node: Any) -> Iterable[Any]:
        if isinstance(node, dict):
            yield node
            for value in node.values():
                yield from CasaDosDadosClient._walk(value)
        elif isinstance(node, list):
            for item in node:
                yield from CasaDosDadosClient._walk(item)

    @staticmethod
    def _first_text(payload: Dict[str, Any], *keys: str) -> str:
        lowered = {str(key).lower(): value for key, value in payload.items()}
        for key in keys:
            value = lowered.get(key.lower())
            if isinstance(value, str) and value.strip():
                return value.strip()
        return ""

    @staticmethod
    def _extract_emails(payload: Dict[str, Any]) -> List[str]:
        output: List[str] = []
        seen = set()
        for node in CasaDosDadosClient._walk(payload):
            if not isinstance(node, dict):
                continue
            for key, value in node.items():
                if "mail" not in str(key).lower():
                    continue
                candidates = CasaDosDadosClient._value_candidates(value, ("email", "endereco", "address", "valor", "value"))
                for candidate in candidates:
                    email = LocalCRMCache.normalize_email(candidate)
                    if email and email not in seen and "@" in email:
                        seen.add(email)
                        output.append(email)
        return output

    @staticmethod
    def _extract_phones(payload: Dict[str, Any]) -> List[str]:
        output: List[str] = []
        seen = set()
        for node in CasaDosDadosClient._walk(payload):
            if not isinstance(node, dict):
                continue
            for key, value in node.items():
                lowered = str(key).lower()
                if not any(token in lowered for token in ("telefone", "celular", "fone", "whatsapp")):
                    continue
                candidates = CasaDosDadosClient._value_candidates(value, ("telefone", "numero", "completo", "valor", "value", "whatsapp"))
                for candidate in candidates:
                    phone = LocalCRMCache.normalize_phone(candidate)
                    if phone and len(phone) >= 10 and phone not in seen:
                        seen.add(phone)
                        output.append(phone)
        return output

    @staticmethod
    def _extract_websites(payload: Dict[str, Any]) -> List[str]:
        output: List[str] = []
        seen = set()
        for node in CasaDosDadosClient._walk(payload):
            if not isinstance(node, dict):
                continue
            for key, value in node.items():
                lowered = str(key).lower()
                if not any(token in lowered for token in ("site", "website", "dominio", "url")):
                    continue
                candidates = CasaDosDadosClient._value_candidates(value, ("site", "website", "dominio", "url", "valor", "value"))
                for candidate in candidates:
                    text = str(candidate or "").strip()
                    if text and text not in seen:
                        seen.add(text)
                        output.append(text)
        return output

    @staticmethod
    def _extract_contacts(payload: Dict[str, Any]) -> List[str]:
        output: List[str] = []
        seen = set()
        contact_keys = ("socio", "administrador", "contato", "responsavel", "diretor", "decisor")
        for node in CasaDosDadosClient._walk(payload):
            if not isinstance(node, dict):
                continue
            lowered_keys = {str(key).lower() for key in node.keys()}
            if not any(any(token in key for token in contact_keys) for key in lowered_keys):
                continue
            for value in node.values():
                if isinstance(value, dict):
                    name = CasaDosDadosClient._first_text(value, "nome", "name")
                    role = CasaDosDadosClient._first_text(value, "qualificacao", "cargo", "role")
                    label = " - ".join(part for part in (name, role) if part).strip()
                    if label and label not in seen:
                        seen.add(label)
                        output.append(label)
                elif isinstance(value, list):
                    for item in value:
                        if not isinstance(item, dict):
                            continue
                        name = CasaDosDadosClient._first_text(item, "nome", "name")
                        role = CasaDosDadosClient._first_text(item, "qualificacao", "cargo", "role")
                        label = " - ".join(part for part in (name, role) if part).strip()
                        if label and label not in seen:
                            seen.add(label)
                            output.append(label)
        return output

    @staticmethod
    def _extract_city_state(payload: Dict[str, Any]) -> tuple[str, str]:
        city = CasaDosDadosClient._first_text(payload, "municipio", "cidade", "city")
        state = CasaDosDadosClient._first_text(payload, "uf", "estado", "state")
        if city and state:
            return city, state
        for node in CasaDosDadosClient._walk(payload):
            if not isinstance(node, dict):
                continue
            if not city:
                city = CasaDosDadosClient._first_text(node, "municipio", "cidade", "city")
            if not state:
                state = CasaDosDadosClient._first_text(node, "uf", "estado", "state")
            if city and state:
                break
        return city, state

    @staticmethod
    def _extract_primary_cnae(payload: Dict[str, Any]) -> str:
        for node in CasaDosDadosClient._walk(payload):
            if not isinstance(node, dict):
                continue
            lowered_keys = {str(key).lower() for key in node.keys()}
            if not any("cnae" in key for key in lowered_keys):
                continue
            text = CasaDosDadosClient._first_text(node, "cnae", "descricao", "texto", "text", "code")
            if text:
                return text
        return ""

    @staticmethod
    def _extract_main_partner(payload: Dict[str, Any], decision_makers: List[str]) -> str:
        if decision_makers:
            return str(decision_makers[0] or "").strip()
        for node in CasaDosDadosClient._walk(payload):
            if not isinstance(node, dict):
                continue
            lowered_keys = {str(key).lower() for key in node.keys()}
            if not any("socio" in key or "administrador" in key for key in lowered_keys):
                continue
            name = CasaDosDadosClient._first_text(node, "nome", "name")
            if name:
                return name
        return ""

    @staticmethod
    def _value_candidates(value: Any, priority_keys: Iterable[str]) -> List[str]:
        output: List[str] = []
        if isinstance(value, dict):
            lowered = {str(key).lower(): raw for key, raw in value.items()}
            for priority_key in priority_keys:
                for key, raw in lowered.items():
                    if priority_key in key:
                        output.extend(CasaDosDadosClient._value_candidates(raw, ()))
            if not output:
                for raw in value.values():
                    output.extend(CasaDosDadosClient._value_candidates(raw, ()))
            return output
        if isinstance(value, list):
            for item in value:
                output.extend(CasaDosDadosClient._value_candidates(item, priority_keys))
            return output
        text = str(value or "").strip()
        return [text] if text else []
