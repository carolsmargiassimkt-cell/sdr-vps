import requests

def enviar_whatsapp(numero, mensagem):
    try:
        # 🔥 GARANTE QUE USA O TEXTO DO WORKER (PITCH)
        texto = str(mensagem)

        # bloqueia fallback genérico
        if "Vi seu perfil" in texto:
            print("[ERRO] CAIU EM MENSAGEM PADRAO - BLOQUEADO")
            return {"error": "mensagem padrão detectada"}

        r = requests.post(
            "http://127.0.0.1:3000/send",
            json={
                "number": str(numero),
                "text": texto
            },
            timeout=60
        )

        print("[ENVIO REAL]", numero)
        return r.json()

    except Exception as e:
        print("[ERRO WHATSAPP]", numero, str(e))
        return {"error": str(e)}


def send_whatsapp_message(numero, mensagem):
    return enviar_whatsapp(numero, mensagem)

def send_message(numero, mensagem):
    return enviar_whatsapp(numero, mensagem)

def enviar(numero, mensagem):
    return enviar_whatsapp(numero, mensagem)
