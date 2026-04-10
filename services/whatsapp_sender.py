import requests

def send_whatsapp(numero, mensagem):
    url = "http://localhost:3000/message/send"

    payload = {
        "number": numero,
        "text": mensagem
    }

    try:
        res = requests.post(url, json=payload, timeout=20)

        if res.status_code == 200:
            print(f"[ENVIADO_OK_CENTRAL] {numero}")
            return True
        else:
            print(f"[ERRO_ENVIO_CENTRAL] {res.text}")
            return False

    except Exception as e:
        print(f"[ERRO_ENVIO_CENTRAL] {e}")
        return False
