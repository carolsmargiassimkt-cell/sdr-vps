import os, requests, random, time

PD = os.getenv("PIPEDRIVE_API_TOKEN")
PIPE_ID = 7
STAGE_ENTRADA = 51
LIMIT = 50

BAD_EMAIL_PREFIXES = (
    "fiscal@", "contabilidade@", "tributario@", "paralegal@", "legalizacao@",
    "nfe@", "financeiro@", "adm@", "administrativo@", "atendimento@",
    "sac@", "rh@", "dp@", "depto", "deptocontabil", "postmaster@",
    "no-reply@", "noreply@", "naoresponda@"
)

BAD_EMAIL_PARTS = (
    "contabil", "tributario", "fiscal", "legalizacao", "paralegal",
    "financeiro", "nfe", "cobranca", "compras"
)

def email_is_bad(email):
    e = (email or "").strip().lower()
    if not e or "@" not in e or "." not in e.split("@")[-1]:
        return True
    bad_exact = {"teste@teste.com", "test@test.com", "email@email.com"}
    return e in bad_exact

<<<<<<< HEAD
=======
def email_is_generic(email):
    e = (email or "").strip().lower()
    generic_prefixes = (
        "sac@", "contato@", "comercial@", "atendimento@", "financeiro@",
        "fiscal@", "contabilidade@", "legalizacao@", "legalização@",
        "paralegal@", "tributario@", "tributário@", "nfe@", "nf@",
        "admin@", "administrativo@", "rh@"
    )
    return e.startswith(generic_prefixes)

>>>>>>> 5c858a8be7eb428553fe3b537420d1f403328f7b
def get_nutricao_stage_id():
    r = requests.get(
        "https://api.pipedrive.com/v1/stages",
        params={"api_token": PD},
        timeout=20,
    )
    if r.status_code >= 400:
        return 62 # Fallback
    for st in (r.json().get("data") or []):
        name = (st.get("name") or "").strip().lower()
        if "nutrição" in name and "enriquecimento" in name:
            return st.get("id")
    return 62

def move_to_nutricao(deal_id):
    stage_id = get_nutricao_stage_id()
    r = requests.put(
        f"https://api.pipedrive.com/v1/deals/{deal_id}",
        params={"api_token": PD},
        json={"stage_id": stage_id},
        timeout=20,
    )
    return r.status_code < 400

def start_cadence(deal_id):
    try:
        r = requests.post(
            "http://127.0.0.1:8001/webhooks/email-cadence",
            json={"deal_id": deal_id},
            timeout=15
        )
        return r.json() if r.status_code == 200 else {"ok": False, "error": r.text}
    except Exception as e:
        return {"ok": False, "error": str(e)}

def run_batch():
    print(f"[BATCH_START] limit={LIMIT} pipe={PIPE_ID} stage={STAGE_ENTRADA}")
    
    # Fetch deals specifically from the Entrada stage
    r = requests.get(
        "https://api.pipedrive.com/v1/deals",
        params={
            "api_token": PD,
            "stage_id": STAGE_ENTRADA,
            "status": "open",
            "limit": 100
        },
        timeout=30
    )
    
    if r.status_code >= 400:
        print(f"[BATCH_ERROR] falha ao buscar deals: {r.status_code}")
        return

    deals = r.json().get("data") or []
    print(f"[BATCH_FETCH] encontrados {len(deals)} deals na etapa Entrada")
    
    random.shuffle(deals)
    
    count = 0
    for d in deals:
        deal_id = d.get("id")
        
        person = d.get("person_id") or {}
        email = ""
        if isinstance(person, dict):
            emails = person.get("email") or []
            email = next((e.get("value") for e in emails if e.get("value")), "")
        
<<<<<<< HEAD
        if not email or email_is_bad(email):
            # print(f"[BATCH_SKIP_EMAIL] deal={deal_id} email={email}")
            continue

        res = start_cadence(deal_id)
        if res.get("skip") == "already_queued":
            print(f"[BATCH_SKIP_DUP] deal={deal_id} já está na fila")
            # Mesmo se já estiver na fila, movemos para nutrição se ele ainda estiver na entrada
            move_to_nutricao(deal_id)
            continue

        if res.get("ok") and res.get("queued"):
            move_to_nutricao(deal_id)
            print(f"[BATCH_ADD] deal={deal_id} adicionado à cadência e movido para Nutrição")
            count += 1
            time.sleep(1)
        else:
            print(f"[BATCH_FAIL] deal={deal_id} erro: {res.get('error') or res}")

        if count >= LIMIT:
            break

    print(f"[BATCH_DONE] processados {count} novos deals")

=======
        if not email:
            print(f"[BATCH_SKIP_SEM_EMAIL] deal={deal_id}")
            continue

        if email_is_bad(email):
            print(f"[BATCH_SKIP_EMAIL_INVALIDO] deal={deal_id} email={email}")
            continue

        if email_is_generic(email):
            print(f"[BATCH_EMAIL_GENERICO] deal={deal_id} email={email}")

        res = start_cadence(deal_id)
        if res.get("skip") == "already_queued":
            print(f"[BATCH_SKIP_DUP] deal={deal_id} já está na fila")
            # Mesmo se já estiver na fila, movemos para nutrição se ele ainda estiver na entrada
            move_to_nutricao(deal_id)
            continue

        if res.get("ok") and res.get("queued"):
            move_to_nutricao(deal_id)
            print(f"[BATCH_ADD] deal={deal_id} adicionado à cadência e movido para Nutrição")
            count += 1
            time.sleep(1)
        else:
            print(f"[BATCH_FAIL] deal={deal_id} erro: {res.get('error') or res}")

        if count >= LIMIT:
            break

    print(f"[BATCH_DONE] processados {count} novos deals")

>>>>>>> 5c858a8be7eb428553fe3b537420d1f403328f7b
if __name__ == "__main__":
    run_batch()
