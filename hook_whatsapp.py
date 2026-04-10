import requests

def enviar_whatsapp(numero):
    try:
        r = requests.post(
            "http://127.0.0.1:3000/send",
            json={
                "number": numero,
                "text": "Oi, tudo bem? Vi seu perfil e queria falar rapidinho"
            },
            timeout=5
        )
        print("[API OK]", numero, r.text)
    except Exception as e:
        print("[ERRO API]", numero, str(e))


# ===== HOOK AUTOMATICO =====
import builtins
_print_original = print

def print(*args, **kwargs):
    try:
        if len(args) >= 2 and args[0] == "[CAD1]":
            numero = str(args[1])
            enviar_whatsapp(numero)
    except:
        pass

    _print_original(*args, **kwargs)
