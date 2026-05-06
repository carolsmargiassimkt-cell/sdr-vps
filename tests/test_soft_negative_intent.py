from logic.whatsapp_pitch_engine import WhatsAppPitchEngine


def test_pause_not_now_does_not_generate_insistent_reply():
    text = "no momento não dá estou sem tempo"
    engine = WhatsAppPitchEngine()

    assert engine.detect_pause_not_now_intent(text) is True
    assert engine.build_reply(
        {"nome": "Lead", "empresa": "Empresa", "conversation_state": {"wa1_sent": True, "replied": True}},
        [text],
        current_step=2,
        allow_opening=False,
    ) == ""


def test_negative_stop_has_priority_over_pause_not_now():
    text = "sem interesse, pode remover"
    engine = WhatsAppPitchEngine()

    assert engine._is_opt_out_message(text) is True
    assert engine.build_reply(
        {"nome": "Lead", "empresa": "Empresa"},
        [text],
        current_step=2,
        allow_opening=False,
    ) != ""
