import random

ABERTURA = [
"Oi, tudo bem? Aqui é a Carol da Mand Digital.",
"Olá, tudo bem? Carol da Mand aqui.",
"Fala, tudo certo? Carol da Mand Digital falando.",
"Oi! Aqui é a Carol, da Mand Digital."
]

DIRECIONAMENTO = [
"Queria falar com a pessoa responsável por marketing ou vendas aí na empresa.",
"Você pode me direcionar para quem cuida de marketing ou comercial?",
"Consegue me indicar quem é responsável pelo marketing ou vendas?"
]

COPA = [
"Com a Copa chegando, muitas empresas começam a rodar campanhas pra aproveitar o aumento de demanda.",
"Agora com a Copa, muita empresa está tentando aproveitar o aumento de movimento.",
"A gente está vendo um aumento forte de campanhas por causa da Copa.",
"A Copa acaba puxando bastante demanda e movimento nas empresas."
]

PROBLEMA = [
"O problema é que a maioria faz isso sem capturar dados e sem transformar em venda depois.",
"Muitas acabam investindo, mas sem conseguir medir ou reaproveitar depois.",
"O que mais vejo é investimento sem retorno estruturado.",
"E aí acaba virando custo, sem gerar dado nem venda depois."
]

GANCHO = [
"Faz sentido olhar isso por aí?",
"Isso acontece por aí também?",
"Vocês já passaram por isso aí?",
"Hoje vocês conseguem aproveitar esses dados depois?"
]

def gerar_msg():
    return f"{random.choice(ABERTURA)}\n\n{random.choice(DIRECIONAMENTO)}\n\n{random.choice(COPA)} {random.choice(PROBLEMA)}\n\n{random.choice(GANCHO)}"
