import requests

def enviar_whatsapp(numero):
    try:
        url = "http://127.0.0.1:3000/send"

        texto = "Oi, tudo bem? Vi seu perfil e queria falar rapidinho"

        payload = {
            "number": numero,
            "text": texto
        }

        r = requests.post(url, json=payload, timeout=5)

        print("[API OK]", numero, r.text)

    except Exception as e:
        print("[ERRO API]", numero, str(e))


# ===== EXEMPLO DE LOOP =====
if __name__ == "__main__":
    numeros = [
        "5511998804191"
    ]

    for numero in numeros:
        enviar_whatsapp(numero)
