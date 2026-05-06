# ===== CONSTANTES =====

PIPELINE_ID = 7

STAGE_PRONTO_PROSPECCAO = 1
STAGE_NUTRICAO = 2
STAGE_WARM = 3

LABEL_LEAD_TRAFEGO = "lead_trafego"
LABEL_FORM_RESPONDIDO = "form_respondido"
LABEL_WARM_WHATSAPP = "warm_whatsapp"
LABEL_CLICK_WARM = "clicked_warm"
LABEL_MAILCHIMP_CLICK = "mailchimp_click"

# ===== FALLBACK DINAMICO =====

def __getattr__(name):
    print(f"[SDR_STATE_FALLBACK_ATTR] {name}")
    return None

# ===== FUNCOES =====

def mark_warm(*args, **kwargs):
    print("[SDR_STATE] mark_warm", args, kwargs)
    return True

def update_score(*args, **kwargs):
    print("[SDR_STATE] update_score", args, kwargs)
    return True

def log_event(*args, **kwargs):
    print("[SDR_STATE] log_event", args, kwargs)
    return True

def add_label(*args, **kwargs):
    print("[SDR_STATE] add_label", args, kwargs)
    return True

def create_activity(*args, **kwargs):
    print("[SDR_STATE] create_activity", args, kwargs)
    return True

def create_note(*args, **kwargs):
    print("[SDR_STATE] create_note", args, kwargs)
    return True
