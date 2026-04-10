from services.whatsapp_sender import send_whatsapp

# INJEÇÃO GLOBAL - NÃO ALTERAR FUNIL
def _send_whatsapp_wrapper(numero, mensagem):
    return send_whatsapp(numero, mensagem)
