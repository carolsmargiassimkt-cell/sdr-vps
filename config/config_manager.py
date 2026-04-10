import logging
import os
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from config.config_loader import get_config_value, load_config
from utils.safe_json import safe_read_json, safe_write_json


SECRET_ENV_MAP = {
    "api4com_token": "API4COM_TOKEN",
    "pipedrive_token": "PIPEDRIVE_TOKEN",
    "gemini_api_key": "GEMINI_API_KEY",
    "gemini_api_key_primary": "GEMINI_API_KEY_PRIMARY",
    "gemini_api_key_fallback": "GEMINI_API_KEY_FALLBACK",
    "casa_dos_dados_api_key": "CASA_DOS_DADOS_API_KEY",
    "smtp_password": "SMTP_PASSWORD",
}

SECRET_FIELDS = set(SECRET_ENV_MAP)


@dataclass
class AppConfig:
    api4com_token: str = ""
    pipedrive_token: str = ""
    gemini_api_key: str = ""
    gemini_api_key_primary: str = ""
    gemini_api_key_fallback: str = ""
    casa_dos_dados_api_key: str = ""
    master_sheet_url: str = ""
    api4com_base_url: str = "https://api4com.com.br/api/v1"
    pipedrive_base_url: str = "https://api.pipedrive.com/v1"
    pitch: str = "Ola, sou o assistente virtual da equipe comercial."
    knowledge_base: str = ""
    voice_reference_path: str = "audio/voice_reference.ogg"
    voice_provider: str = "openvoice"
    max_calls: int = 20
    interval_between_calls: int = 5
    noise_reduction_enabled: bool = True
    vad_enabled: bool = True
    chamadas_concorrentes: int = 2
    max_call_duration_sec: int = 180
    max_silence_rounds: int = 2
    leads_path: str = "config/leads.json"
    log_file: str = "logs/bot.log"
    request_timeout_sec: int = 15
    retry_attempts: int = 3
    retry_backoff_sec: float = 1.2
    target_latency_ms: int = 1500
    gemini_model: str = "gemini-1.5-flash"
    stage_agendamento_id: int = 0
    stage_sem_interesse_id: int = 0
    timestamp_last_save: str = ""
    extra: Dict[str, Any] = field(default_factory=dict)


class ConfigManager:
    def __init__(self, config_path: str = "config/config.json") -> None:
        load_config()
        self.config_path = Path(config_path)
        self.system_config_path = Path("config/system_config.json")
        self.audio_config_path = Path("config/audio_config.json")
        self._logger = logging.getLogger("bot_sdr_ai")
        self.required_dirs: List[str] = [
            "interface",
            "speech",
            "ai",
            "crm",
            "telefonia",
            "logic",
            "pitch",
            "logs",
            "audio",
            "config",
            "data",
        ]

    def ensure_project_structure(self) -> None:
        for folder in self.required_dirs:
            Path(folder).mkdir(parents=True, exist_ok=True)
        if not Path("main.py").exists():
            Path("main.py").write_text("", encoding="utf-8")
        if not Path("requirements.txt").exists():
            Path("requirements.txt").write_text("", encoding="utf-8")
        if not self.audio_config_path.exists():
            self._save_json(
                self.audio_config_path,
                {
                    "noise_reduction_enabled": True,
                    "noise_reduction_strength": 0.8,
                    "vad_enabled": True,
                },
            )
        if not self.system_config_path.exists():
            self.save_config({})
        cadence_path = Path("config/cadence_config.json")
        if not cadence_path.exists():
            self._save_json(
                cadence_path,
                {
                    "cad_1": 0,
                    "cad_2": 3600,
                    "cad_3": 86400,
                    "cad_4": 172800,
                    "cad_5": 259200,
                    "cad_6": 432000,
                },
            )

    def save_config(self, data: Dict[str, Any]) -> None:
        current = self.load_config()
        now = datetime.utcnow().isoformat()

        payload = {
            "API4COM_TOKEN": data.get("api4com_token", current.get("api4com_token", "")),
            "PIPEDRIVE_TOKEN": data.get("pipedrive_token", current.get("pipedrive_token", "")),
            "GEMINI_API_KEY": data.get("gemini_api_key", current.get("gemini_api_key", "")),
            "GEMINI_API_KEY_PRIMARY": data.get(
                "gemini_api_key_primary",
                current.get("gemini_api_key_primary", current.get("gemini_api_key", "")),
            ),
            "GEMINI_API_KEY_FALLBACK": data.get(
                "gemini_api_key_fallback",
                current.get("gemini_api_key_fallback", ""),
            ),
            "CASA_DOS_DADOS_API_KEY": data.get(
                "casa_dos_dados_api_key",
                current.get("casa_dos_dados_api_key", ""),
            ),
            "MASTER_SHEET_URL": data.get("master_sheet_url", current.get("master_sheet_url", "")),
            "voice_provider": data.get("voice_provider", current.get("voice_provider", "openvoice")),
            "pitch_text": data.get("pitch", current.get("pitch", "")),
            "knowledge_base": data.get("knowledge_base", current.get("knowledge_base", "")),
            "max_calls": int(data.get("max_calls", current.get("max_calls", 20))),
            "interval_between_calls": int(
                data.get("interval_between_calls", current.get("interval_between_calls", 5))
            ),
            "noise_reduction_enabled": self._to_bool(
                data.get("noise_reduction_enabled", current.get("noise_reduction_enabled", True))
            ),
            "vad_enabled": self._to_bool(data.get("vad_enabled", current.get("vad_enabled", True))),
            "timestamp_last_save": now,
        }
        self._save_json(self.system_config_path, payload)

        # Mantem audio pipeline sincronizado.
        audio_cfg = self._load_json(
            self.audio_config_path,
            {"noise_reduction_enabled": True, "noise_reduction_strength": 0.8, "vad_enabled": True},
        )
        audio_cfg["noise_reduction_enabled"] = payload["noise_reduction_enabled"]
        audio_cfg["vad_enabled"] = payload["vad_enabled"]
        self._save_json(self.audio_config_path, audio_cfg)

        self._logger.info("Configuration saved successfully")

    def load_config(self) -> Dict[str, Any]:
        if not self.system_config_path.exists():
            self.save_config({})
        raw = self._load_json(self.system_config_path, {})
        normalized = {
            "api4com_token": self._env_or_legacy("api4com_token", raw.get("API4COM_TOKEN", "")),
            "pipedrive_token": self._env_or_legacy("pipedrive_token", raw.get("PIPEDRIVE_TOKEN", "")),
            "gemini_api_key": self._env_or_legacy("gemini_api_key", raw.get("GEMINI_API_KEY", "")),
            "gemini_api_key_primary": self._env_or_legacy(
                "gemini_api_key_primary",
                raw.get("GEMINI_API_KEY_PRIMARY", raw.get("GEMINI_API_KEY", "")),
            ),
            "gemini_api_key_fallback": self._env_or_legacy(
                "gemini_api_key_fallback",
                raw.get("GEMINI_API_KEY_FALLBACK", ""),
            ),
            "casa_dos_dados_api_key": self._env_or_legacy(
                "casa_dos_dados_api_key",
                raw.get("CASA_DOS_DADOS_API_KEY", ""),
            ),
            "master_sheet_url": raw.get("MASTER_SHEET_URL", ""),
            "voice_provider": raw.get("voice_provider", "openvoice"),
            "pitch": raw.get("pitch_text", ""),
            "knowledge_base": raw.get("knowledge_base", ""),
            "max_calls": int(raw.get("max_calls", 20)),
            "interval_between_calls": int(raw.get("interval_between_calls", 5)),
            "noise_reduction_enabled": self._to_bool(raw.get("noise_reduction_enabled", True)),
            "vad_enabled": self._to_bool(raw.get("vad_enabled", True)),
            "timestamp_last_save": raw.get("timestamp_last_save", ""),
        }
        return normalized

    def load(self) -> AppConfig:
        cfg = AppConfig()
        merged = load_config()
        if self.config_path.exists():
            data = self._load_json(self.config_path, {})
            if "limite_chamadas" in data and "max_calls" not in data:
                data["max_calls"] = data["limite_chamadas"]
            if "intervalo_chamadas" in data and "interval_between_calls" not in data:
                data["interval_between_calls"] = data["intervalo_chamadas"]
            if "base_conhecimento" in data and "knowledge_base" not in data:
                data["knowledge_base"] = data["base_conhecimento"]
            for key, value in data.items():
                if key in SECRET_FIELDS:
                    setattr(cfg, key, self._env_or_legacy(key, value))
                    continue
                if hasattr(cfg, key):
                    setattr(cfg, key, value)
                else:
                    cfg.extra[key] = value

        system_data = self.load_config()
        cfg.api4com_token = system_data.get("api4com_token", cfg.api4com_token)
        cfg.pipedrive_token = system_data.get("pipedrive_token", cfg.pipedrive_token)
        cfg.gemini_api_key = system_data.get("gemini_api_key", cfg.gemini_api_key)
        cfg.gemini_api_key_primary = system_data.get(
            "gemini_api_key_primary",
            cfg.gemini_api_key_primary or cfg.gemini_api_key,
        )
        cfg.gemini_api_key_fallback = system_data.get("gemini_api_key_fallback", cfg.gemini_api_key_fallback)
        cfg.casa_dos_dados_api_key = system_data.get("casa_dos_dados_api_key", cfg.casa_dos_dados_api_key)
        cfg.master_sheet_url = system_data.get("master_sheet_url", cfg.master_sheet_url)
        cfg.voice_provider = system_data.get("voice_provider", cfg.voice_provider)
        cfg.pitch = system_data.get("pitch", cfg.pitch)
        cfg.knowledge_base = system_data.get("knowledge_base", cfg.knowledge_base)
        cfg.max_calls = int(system_data.get("max_calls", cfg.max_calls))
        cfg.interval_between_calls = int(system_data.get("interval_between_calls", cfg.interval_between_calls))
        cfg.noise_reduction_enabled = self._to_bool(
            system_data.get("noise_reduction_enabled", cfg.noise_reduction_enabled)
        )
        cfg.vad_enabled = self._to_bool(system_data.get("vad_enabled", cfg.vad_enabled))
        cfg.timestamp_last_save = system_data.get("timestamp_last_save", "")

        cfg.api4com_token = self._env_or_legacy("api4com_token", cfg.api4com_token)
        cfg.pipedrive_token = self._env_or_legacy("pipedrive_token", cfg.pipedrive_token)
        cfg.gemini_api_key = self._env_or_legacy("gemini_api_key", cfg.gemini_api_key)
        cfg.gemini_api_key_primary = self._env_or_legacy(
            "gemini_api_key_primary",
            cfg.gemini_api_key_primary or cfg.gemini_api_key,
        )
        cfg.gemini_api_key_fallback = self._env_or_legacy(
            "gemini_api_key_fallback",
            cfg.gemini_api_key_fallback,
        )
        cfg.casa_dos_dados_api_key = self._env_or_legacy(
            "casa_dos_dados_api_key",
            cfg.casa_dos_dados_api_key,
        )
        if not cfg.gemini_api_key:
            cfg.gemini_api_key = cfg.gemini_api_key_primary or cfg.gemini_api_key_fallback
        cfg.master_sheet_url = str(merged.get("master_sheet_url", cfg.master_sheet_url) or cfg.master_sheet_url)
        cfg.voice_provider = str(merged.get("voice_provider", cfg.voice_provider) or cfg.voice_provider)
        return cfg

    def save(self, config: AppConfig) -> None:
        self.config_path.parent.mkdir(parents=True, exist_ok=True)
        payload = asdict(config)
        for key in SECRET_FIELDS:
            if key in payload:
                payload[key] = ""
        safe_write_json(self.config_path, payload)

    @staticmethod
    def _env_or_legacy(field_name: str, legacy_value: Any = "") -> str:
        env_value = str(get_config_value(field_name, "") or "").strip()
        if env_value:
            return env_value
        return str(legacy_value or "").strip()

    @staticmethod
    def _to_bool(value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        return str(value).strip().lower() in {"1", "true", "yes", "on"}

    @staticmethod
    def _save_json(path: Path, payload: Dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        safe_write_json(path, payload)

    @staticmethod
    def _load_json(path: Path, default: Dict[str, Any]) -> Dict[str, Any]:
        try:
            payload = safe_read_json(path)
            return payload if isinstance(payload, dict) else dict(default)
        except Exception:
            return dict(default)
