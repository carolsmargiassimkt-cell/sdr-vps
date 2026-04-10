from __future__ import annotations

import copy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict

import pytest

import whatsapp_bot
from whatsapp_bot import WhatsAppBot


@pytest.fixture
def fake_json_store(monkeypatch: pytest.MonkeyPatch) -> Dict[str, Any]:
    store: Dict[str, Any] = {}

    def fake_read(path: Path) -> Any:
        return copy.deepcopy(store.get(str(path), {}))

    def fake_write(path: Path, data: Any) -> None:
        store[str(path)] = copy.deepcopy(data)

    monkeypatch.setattr(whatsapp_bot, "safe_read_json", fake_read)
    monkeypatch.setattr(whatsapp_bot, "safe_write_json", fake_write)
    return store


def build_bot(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    store: Dict[str, Any],
    initial: Dict[str, Any],
) -> tuple[WhatsAppBot, str, list[str]]:
    target_file = tmp_path / "wpp_enviados.json"
    target_file.write_text("{}")
    key = str(target_file)
    store[key] = copy.deepcopy(initial)
    monkeypatch.setattr(whatsapp_bot, "PROJECT_ROOT", tmp_path)
    log: list[str] = []
    bot = WhatsAppBot(real_send=False)
    bot._emit = lambda message: log.append(message)
    return bot, key, log


def test_load_initial_sent_today(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, fake_json_store: Dict[str, Any]) -> None:
    today = datetime.now(timezone.utc).date()
    initial = {today.isoformat(): ["5511999999999"]}
    bot, _, log = build_bot(monkeypatch, tmp_path, fake_json_store, initial)
    assert "5511999999999" in bot._sent_today
    assert any(message.startswith("[PERSISTENCIA_CARREGADA]") for message in log)


def test_duplication_logs(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, fake_json_store: Dict[str, Any]) -> None:
    today = datetime.now(timezone.utc).date()
    phone = "5511999999999"
    initial = {today.isoformat(): [phone]}
    bot, _, log = build_bot(monkeypatch, tmp_path, fake_json_store, initial)
    log.clear()
    assert bot._has_sent_today(phone)
    if not bot._is_test_mode_phone(phone) and bot._has_sent_today(phone):
        bot._emit(f"[DUPLICIDADE_BLOQUEADA] telefone={phone} motivo=enviado_dia")
    assert any("[DUPLICIDADE_BLOQUEADA]" in message for message in log)


def test_persistence_after_mark(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, fake_json_store: Dict[str, Any]) -> None:
    today = datetime.now(timezone.utc).date()
    initial: Dict[str, Any] = {today.isoformat(): []}
    bot, key, log = build_bot(monkeypatch, tmp_path, fake_json_store, initial)
    log.clear()
    phone = "5511999990000"
    bot._mark_sent_today(phone)
    stored = fake_json_store[key]
    assert stored[today.isoformat()] == [phone]
    assert any("[PERSISTENCIA_OK]" in message for message in log)


def test_prune_old_dates(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, fake_json_store: Dict[str, Any]) -> None:
    today = datetime.now(timezone.utc).date()
    days = [today - timedelta(days=i) for i in range(5)]
    initial: Dict[str, Any] = {day.isoformat(): [f"55{9000000000 + i}"] for i, day in enumerate(days)}
    bot, key, log = build_bot(monkeypatch, tmp_path, fake_json_store, initial)
    log.clear()
    bot._persist_sent_today()
    stored = fake_json_store[key]
    expected = {day.isoformat() for day in days[:3]}
    assert set(stored.keys()) == expected


def test_phone_normalization(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, fake_json_store: Dict[str, Any]) -> None:
    today = datetime.now(timezone.utc).date()
    initial: Dict[str, Any] = {today.isoformat(): []}
    bot, key, log = build_bot(monkeypatch, tmp_path, fake_json_store, initial)
    log.clear()
    raw = "(11) 99999-9999"
    bot._mark_sent_today(raw)
    stored = fake_json_store[key]
    normalized = "5511999999999"
    assert stored[today.isoformat()] == [normalized]
    assert any("[PERSISTENCIA_OK]" in message for message in log)
