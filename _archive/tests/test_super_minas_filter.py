import pytest

from whatsapp_bot import WhatsAppBot


def make_lead(*, tags=None, origin="", phone="", status_whatsapp=""):
    tags = tags or []
    return {
        "tags": list(tags),
        "origem_oficial": origin,
        "telefone": phone,
        "status_whatsapp": status_whatsapp,
    }


@pytest.fixture
def bot():
    return WhatsAppBot(real_send=False)


def test_filter_only_super_minas(bot):
    leads = [
        make_lead(tags=["SUPER_MINAS"], phone="5511999999999"),
        make_lead(tags=["OUTRA"], phone="5511999999998"),
        make_lead(origin="campanha_abertura", phone="5511999999997"),
        make_lead(origin="outro", phone="5511999999996"),
    ]
    filtered = bot._filter_super_minas_leads(leads, limit=10)
    assert len(filtered) == 2


def test_ignore_status_enviado(bot):
    leads = [
        make_lead(tags=["SUPER_MINAS"], phone="5511999999999", status_whatsapp="ENVIADO"),
        make_lead(tags=["SUPER_MINAS"], phone="5511999999998", status_whatsapp="PENDENTE"),
    ]
    filtered = bot._filter_super_minas_leads(leads, limit=10)
    assert len(filtered) == 1
    assert filtered[0]["status_whatsapp"] == "PENDENTE"


def test_phone_validity(bot):
    leads = [
        make_lead(tags=["SUPER_MINAS"], phone="551199999999"),
        make_lead(tags=["SUPER_MINAS"], phone="119999999"),
        make_lead(tags=["SUPER_MINAS"], phone=""),
    ]
    filtered = bot._filter_super_minas_leads(leads, limit=10)
    assert len(filtered) == 1
    assert filtered[0]["telefone"] == "551199999999"


def test_normalize_phone_variants(bot):
    assert bot._normalize_phone("(11) 99999-9999") == "5511999999999"
    assert bot._normalize_phone("11 99999-9999") == "5511999999999"


def test_filter_logs(capsys, bot):
    leads = [make_lead(tags=["SUPER_MINAS"], phone="5511999999999")]
    bot.logger = None
    bot._filter_super_minas_leads(leads, limit=2)
    captured = capsys.readouterr()
    assert "[SUPER_MINAS_FILTRADOS]" in captured.out


def test_no_csv_fallback(monkeypatch, bot):
    monkeypatch.setattr(bot, "_fetch_targets_from_local_base", lambda limit, only_super_minas: [])
    # CSV fallback removido, lambda *args, **kwargs: pytest.fail("should not call CSV fallback"))
    result = bot._fetch_super_minas_targets(limit=5)
    assert result == []


def test_final_count_122(bot):
    leads = []
    for i in range(200):
        if i < 122:
            tags = ["SUPER_MINAS"]
            phone = f"55119{10000000 + i}"
        else:
            tags = ["OUTRO"]
            phone = f"55119{20000000 + i}"
        leads.append(make_lead(tags=tags, phone=phone))
    filtered = bot._filter_super_minas_leads(leads, limit=200)
    assert len(filtered) == 122
