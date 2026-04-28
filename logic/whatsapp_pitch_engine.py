from __future__ import annotations

import os
import random
import re
import unicodedata
from typing import Any, Dict, List

import requests

try:
    from config.config_loader import get_config_value
except Exception:
    def get_config_value(_key, default=""):
        return default


class WhatsAppPitchEngine:
    GENERIC_FIRST_NAMES = {
        "administrativo",
        "atendimento",
        "atacadao",
        "comercial",
        "compras",
        "contabilidade",
        "contato",
        "diretoria",
        "drogaria",
        "farmacia",
        "financeiro",
        "fiscal",
        "juridico",
        "marketing",
        "nfe",
        "sac",
        "supermercado",
        "vendas",
        "rh",
        "recursos humanos",
        "recepcao",
        "portaria",
        "suporte",
        "ti",
        "tecnologia",
    }
    NONLEAD_INTENT_RULES = (
        (
            "mensagem_automatica",
            (
                "bem vindo ao nosso whatsapp",
                "bem vindo ao whatsapp",
                "mensagem automatica",
                "resposta automatica",
                "assistente virtual",
                "atendimento automatico",
                "atendimento automatizado",
                "robo de atendimento",
            ),
            (
                r"\bbem[\s-]?vindo(?:a)?\b.*\bwhatsapp\b",
                r"\bmensagem\b.*\bautomat",
                r"\batendimento\b.*\bautomat",
            ),
        ),
        (
            "menu_ura",
            (
                "nao entendi escolha uma das opcoes acima",
                "nao entendi, escolha uma das opcoes acima",
                "escolha uma das opcoes acima",
                "escolha uma opcao",
                "digite uma opcao",
                "menu principal",
                "para falar com",
                "opcao 1",
                "opcao 2",
                "opcao 3",
                "tecle 1",
                "tecle 2",
            ),
            (
                r"\bescolha\b.*\b(opc|op\u00e7)\w*\b",
                r"\bmenu\b",
                r"\btecle\b.*\b\d\b",
                r"\bop(?:c|ç)ao\b\s*\d",
            ),
        ),
        (
            "fila_espera",
            (
                "um momento",
                "um instante",
                "em instantes",
                "ja iremos atender",
                "seu atendimento sera realizado",
                "fila de atendimento",
                "seu atendimento sera transferido",
                "estamos transferindo",
                "ja vamos atender",
            ),
            (
                r"\b(aguarde|aguardar)\b",
                r"\bum (momento|instante)\b",
                r"\bfila\b.*\b(atendimento|espera)\b",
                r"\batendimento\b.*\b(instantes|breve|transferid|encaminhad|realizado)\b",
                r"\batendente\b.*\b(atender|retornar|realizar)\b",
            ),
        ),
        (
            "atendimento_humano",
            (
                "um de nossos atendentes",
                "nossos atendentes",
                "um atendente ira",
                "atendente humano",
                "vou encaminhar",
                "irei encaminhar",
                "vou transferir",
                "vou passar para",
                "setor comercial",
                "setor responsavel",
                "responsavel pelo atendimento",
            ),
            (
                r"\b(atendente|colaborador|responsavel)\b.*\b(falara|atendera|retornara)\b",
                r"\b(vou|iremos|irei)\b.*\b(encaminhar|transferir|passar)\b",
            ),
        ),
        (
            "mensagem_institucional",
            (
                "nosso horario",
                "horario de atendimento",
                "de segunda a sexta",
                "segunda a sexta",
                "segunda a sexta feira",
                "seg a sex",
                "horario comercial",
                "retornaremos no horario",
                "retornaremos em horario",
                "nosso atendimento e de",
                "recepcao",
                "secretaria",
                "clinica",
                "central de atendimento",
                "ura",
            ),
            (
                r"\b(horario|funcionamento)\b.*\b(atendimento|comercial)\b",
                r"\bsegunda\b.*\bsexta\b",
                r"\b(retornaremos|retomaremos)\b.*\b(dia util|horario)\b",
            ),
        ),
        (
            "nao_interessado",
            (
                "nao temos interesse",
                "não temos interesse",
                "nao tenho interesse",
                "não tenho interesse",
                "sem interesse",
                "nao queremos",
                "não queremos",
            ),
            (
                r"\bnao\b.*\binteresse\b",
                r"\bsem interesse\b",
            ),
        ),
        (
            "empresa_incompativel",
            (
                "somos uma contabilidade",
                "aqui e uma contabilidade",
                "aqui é uma contabilidade",
                "escritorio de contabilidade",
                "escritório de contabilidade",
                "empresa de contabilidade",
            ),
            (
                r"\bcontabil\w+\b",
                r"\bcontabilidade\b",
            ),
        ),
        (
            "numero_errado",
            (
                "houve um equivoco",
                "houve um equívoco",
                "numero errado",
                "número errado",
                "telefone errado",
                "contato errado",
            ),
            (
                r"\bequivoc\w+\b",
                r"\bnumero\b.*\berrado\b",
                r"\btelefone\b.*\berrado\b",
            ),
        ),
        (
            "setor_errado",
            (
                "nao e o setor responsavel",
                "não é o setor responsável",
                "nao somos o setor responsavel",
                "não somos o setor responsável",
                "setor errado",
                "nao e aqui",
                "não é aqui",
            ),
            (
                r"\bsetor\b.*\brespons\w+\b",
                r"\bsetor errado\b",
                r"\bnao\b.*\baqui\b",
            ),
        ),
    )
    WHATSAPP_OPENINGS = [
        "{Oi|Olá}, {tudo bem|tudo joia}? {Aqui é a Carol da Mand Digital|Carol da Mand Digital aqui}. Nesse número falo com {a pessoa que cuida do marketing|a pessoa que cuida de marketing|a pessoa que cuida do marketing ou vendas|a pessoa que cuida de marketing ou vendas} aí na __EMPRESA__?",
        "{Oi|Olá}, {tudo bem|tudo joia}? {Aqui é a Carol da Mand Digital|Carol da Mand Digital aqui}. Nesse número eu falo com {a pessoa que cuida do marketing|a pessoa que cuida de marketing|a pessoa que cuida do marketing ou vendas|a pessoa que cuida de marketing ou vendas} aí na __EMPRESA__?",
        "{Oi|Olá}, {tudo bem|tudo joia}? {Aqui é a Carol da Mand Digital|Carol da Mand Digital aqui}. Por esse número consigo falar com {a pessoa que cuida do marketing|a pessoa que cuida de marketing|a pessoa que cuida do marketing ou vendas|a pessoa que cuida de marketing ou vendas} aí na __EMPRESA__?",
        "{Oi|Olá}, {tudo bem|tudo joia}? {Aqui é a Carol da Mand Digital|Carol da Mand Digital aqui}. Queria confirmar se nesse número eu falo com {a pessoa que cuida do marketing|a pessoa que cuida de marketing|a pessoa que cuida do marketing ou vendas|a pessoa que cuida de marketing ou vendas} aí na __EMPRESA__?",
    ]
    WHATSAPP_OPENINGS_GENERIC = [
        "{Oi|Olá}, {tudo bem|tudo joia}? {Aqui é a Carol da Mand Digital|Carol da Mand Digital aqui}. Nesse número falo com {a pessoa que cuida do marketing|a pessoa que cuida de marketing|a pessoa que cuida do marketing ou vendas|a pessoa que cuida de marketing ou vendas}?",
        "{Oi|Olá}, {tudo bem|tudo joia}? {Aqui é a Carol da Mand Digital|Carol da Mand Digital aqui}. Por esse número consigo falar com {a pessoa que cuida do marketing|a pessoa que cuida de marketing|a pessoa que cuida do marketing ou vendas|a pessoa que cuida de marketing ou vendas}?",
        "{Oi|Olá}, {tudo bem|tudo joia}? {Aqui é a Carol da Mand Digital|Carol da Mand Digital aqui}. Queria confirmar se nesse número eu falo com {a pessoa que cuida do marketing|a pessoa que cuida de marketing|a pessoa que cuida do marketing ou vendas|a pessoa que cuida de marketing ou vendas}?",
    ]
    EMAIL_TEMPLATES = [
        {
            "subject": "Copa do Mundo e Campanhas - Mand Digital",
            "body": "Olá {nome},\n\nVi que a {empresa} está crescendo e com a Copa chegando, as campanhas podem ser um grande diferencial.\n\nPodemos falar sobre como estruturar isso?\n\nAbs, Carol"
        },
        {
            "subject": "Dados e Performance na {empresa}",
            "body": "Oi {nome},\n\nNotei que muitas empresas perdem dados valiosos em campanhas sazonais.\n\nNa Mand, ajudamos a transformar isso em vendas reais. Topa uma conversa rápida?\n\nCarol"
        },
        {
            "subject": "Parceria Mand Digital + {empresa}",
            "body": "Olá {nome},\n\nEstou acompanhando a {empresa} e acredito que nossas campanhas gamificadas fariam sentido para vocês agora na Copa.\n\nQual sua disponibilidade para um call de 5 min?\n\nCarol"
        },
        {
            "subject": "Sugestão para o marketing da {empresa}",
            "body": "Oi {nome},\n\nTive uma ideia de como a {empresa} pode aproveitar o engajamento da Copa para capturar dados reais de clientes.\n\nConseguimos falar essa semana?\n\nCarol"
        },
        {
            "subject": "Convite: Demonstração Prática",
            "body": "Olá {nome},\n\nGostaria de te mostrar como grandes marcas estão usando gamificação para vender mais.\n\nHorário livre na quinta?\n\nAbs, Carol"
        },
        {
            "subject": "Encerrando meu contato com a {empresa}",
            "body": "Olá {nome},\n\nComo não tive retorno, vou encerrar meu contato por aqui para não ser insistente.\n\nSe fizer sentido falar sobre campanhas da Copa, captação de dados e geração de vendas, fico à disposição.\n\nAbs, Carol"
        }
    ]
    FLUID_REPLY_RULES = (
        (
            ("oi", "olá", "ola", "bom dia", "boa tarde", "boa noite"),
            [
                "{Oi|Olá}! Tudo bem por aí?\n\nSou a Carol, da Mand. Queria te explicar rapidinho como a gente usa campanhas gamificadas (tipo roleta ou raspadinha) para gerar venda e captar dados reais.\n\nFaz sentido eu te resumir em 1 minuto?",
                "{Oi|Olá}! Tudo certo?\n\nAqui é a Carol, da Mand. Posso te explicar como transformamos campanhas em experiências interativas que vendem de verdade?\n\nSe fizer sentido, eu já te mostro o próximo passo.",
            ],
        ),
        (
            ("quero entender", "como funciona", "me explica", "entender melhor", "como seria", "não entendi", "nao entendi"),
            [
                "Claro.\n\nA gente transforma campanhas tradicionais em experiências gamificadas, como roletas ou jornadas interativas. Isso gera muito mais engajamento e dados reais dos clientes.\n\nHoje vocês já fazem algo nessa linha ou seria a primeira vez?",
                "Vou resumir: a Mand cria a mecânica (raspadinhas, roletas, etc), coleta os dados e acompanha a conversão em venda. Com a Copa chegando, é uma ação bem forte.\n\nVocês já têm alguma ação parecida planejada?",
            ],
        ),
        (
            ("sim", "tem interesse", "interesse", "faz sentido", "legal", "curti", "gostei"),
            [
                "Legal. A ideia é transformar campanhas promocionais em experiências simples, tipo roleta ou raspadinha, que ajudam a gerar engajamento, venda e ainda capturam dados reais durante a jornada.\n\nFaz sentido eu te mostrar em 10 minutos como funcionaria para vocês?",
                "Perfeito. Com a Copa chegando, essas ações gamificadas ajudam a identificar quem participou e transformar o movimento em venda real.\n\nTopa um papo de 10 minutos para eu te mostrar alguns exemplos?",
            ],
        ),
        (
            ("preço", "preco", "valor", "quanto custa", "investimento"),
            [
                "O investimento depende da mecânica (roleta, raspadinha, etc) e do volume. Para te passar algo coerente, preciso entender qual o objetivo principal hoje.\n\nA prioridade de vocês seria gerar novos leads ou vender para a base atual?",
                "Varia conforme a complexidade da jornada interativa. Se você quiser, eu te mostro os modelos mais comuns e já te dou uma estimativa de valores.\n\nQual horário fica melhor para a gente falar rapidinho?",
            ],
        ),
        (
            ("reunião", "reuniao", "call", "agenda", "horário", "horario"),
            [
                "Perfeito. Consigo te mostrar como as experiências gamificadas funcionam na prática em 10 minutos.\n\nQual horário fica melhor para você?",
                "Ótimo. Vou te mostrar como marcas estão usando isso para capturar dados e vender mais na Copa.\n\nQual horário te ajuda mais?",
            ],
        ),
    )
    DEFAULT_DAY_OVERRIDES = {
        1: (
            "__ABERTURA__\n\n"
            "Vi a __EMPRESA__ e queria te trazer um contexto rápido:\n\n"
            "Com a Copa chegando, muitas empresas começam a rodar campanhas promocionais pra aproveitar o aumento de demanda,\n"
            "mas a maioria ainda faz isso sem conseguir medir resultado ou capturar dados dos clientes.\n\n"
            "Queria entender uma coisa rapidinho:\n\n"
            "Vocês pensam em fazer alguma ação nesse período ou não costumam trabalhar com campanhas?"
        ),
        2: (
            "Perfeito, __NOME__.\n\n"
            "Vou direto ao ponto contigo então.\n\n"
            "Hoje o que mais vemos são empresas investindo nesse tipo de campanha agora com a Copa,\n"
            "mas sem conseguir identificar quem participou ou transformar isso em venda depois.\n\n"
            "Vocês hoje conseguem acompanhar quem compra ou participa dessas ações?"
        ),
        3: (
            "Entendi.\n\n"
            "Isso é mais comum do que parece.\n\n"
            "Muitas empresas acabam tendo pouca visibilidade do resultado,\n"
            "dificuldade de medir retorno\n"
            "e perda de dados valiosos.\n\n"
            "E no fim, a campanha até gera movimento, mas não gera crescimento previsível."
        ),
        4: (
            "Principalmente agora com a Copa, onde o volume aumenta muito,\n"
            "se não tiver estrutura, acaba sendo uma oportunidade desperdiçada.\n\n"
            "Porque entra gente, mas não vira base, não vira recorrência."
        ),
        5: (
            "Foi exatamente por isso que criamos aqui na Mand as campanhas gamificadas.\n\n"
            "A gente estrutura toda a campanha:\n"
            "com segurança jurídica,\n"
            "captação de dados reais,\n"
            "mecânica de engajamento\n"
            "e acompanhamento em tempo real.\n\n"
            "Ou seja, não é só rodar campanha:\n"
            "ela vira venda + base de dados + inteligência."
        ),
        6: "Faz sentido esse tipo de estrutura pra vocês ou hoje não é prioridade?",
        7: (
            "Perfeito.\n\n"
            "Acho que faz sentido te mostrar isso na prática, porque muda bastante quando você vê aplicado."
        ),
        8: (
            "Perfeito.\n\n"
            "Acho que faz sentido te mostrar isso na prática.\n\n"
            "Qual horário fica melhor pra você?"
        ),
        9: (
            "Ah, um minutinho que eu vou ver aqui.\n\n"
            "Consegui sim.\n\n"
            "Qual o melhor e-mail pra eu te enviar o convite?"
        ),
        10: (
            "Perfeito! Já vou te enviar o convite da reunião nesse e-mail.\n\n"
            "No dia eu te mando um lembrete por aqui também."
        ),
    }
    DEFAULT_CLOSING_OVERRIDES = {
        "wrong_number": "Desculpa pelo engano. Vou retirar este contato da lista. Obrigada por avisar.",
        "no_interest": "Perfeito, obrigada por avisar. Vou encerrar por aqui para não incomodar.",
        "unknown_person": "Entendi. Obrigada por avisar. Vou remover este contato para evitar novos envios.",
        "referral": "Perfeito, obrigada pela ajuda. Você poderia me informar o melhor contato da pessoa responsável?",
    }
    DAY_PATTERN = re.compile(
        r"(DIA\s+([1-9]|10)\s+[^\n]*\n)(.*?)(?=\n\s*DIA\s+([1-9]|10)\s+[^\n]*\n|\n\s*=+\n|\Z)",
        re.IGNORECASE | re.DOTALL,
    )
    SCHEDULE_DAY_PATTERN = re.compile(
        r"\b("
        r"hoje|amanha|amanhã|"
        r"segunda(?:-feira)?|"
        r"terca(?:-feira)?|terça(?:-feira)?|"
        r"quarta(?:-feira)?|"
        r"quinta(?:-feira)?|"
        r"sexta(?:-feira)?|"
        r"sabado|sábado|"
        r"domingo"
        r")\b",
        re.IGNORECASE,
    )
    SCHEDULE_PERIOD_PATTERN = re.compile(
        r"\b("
        r"de manha|de manhã|na manha|na manhã|pela manha|pela manhã|cedo|"
        r"a tarde|à tarde|de tarde|na tarde|no fim da tarde|"
        r"a noite|à noite|de noite|na noite"
        r")\b",
        re.IGNORECASE,
    )
    GEMINI_GENERATE_URL_TEMPLATE = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"

    def __init__(self, config: Any, logger=None) -> None:
        self.config = config
        self.logger = logger
        self._pitch_text = str(getattr(config, "pitch", "") or "")
        self._days = self._parse_day_messages(self._pitch_text)
        self._closings = self._parse_closing_messages(self._pitch_text)
        self.gemini_model = str(
            os.getenv("GEMINI_MODEL")
            or get_config_value("gemini_model", "gemini-1.5-flash")
            or "gemini-1.5-flash"
        ).strip() or "gemini-1.5-flash"

    @staticmethod
    def _clean_block(text: str) -> str:
        cleaned = str(text or "").strip()
        cleaned = cleaned.replace("\r\n", "\n").replace("\r", "\n")
        cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
        return cleaned.strip()

    @classmethod
    def _parse_day_messages(cls, pitch_text: str) -> Dict[int, str]:
        days: Dict[int, str] = {}
        for match in cls.DAY_PATTERN.finditer(str(pitch_text or "")):
            day = int(match.group(2) or 0)
            body = cls._clean_block(match.group(3) or "")
            if day and body:
                days[day] = body
        return days

    @classmethod
    def _parse_closing_messages(cls, pitch_text: str) -> Dict[str, str]:
        text = str(pitch_text or "")
        marker = "ENCERRAMENTOS AUTOMATICOS"
        start_idx = text.find(marker)
        if start_idx < 0:
            return {}
        section = text[start_idx:]
        keys = {
            "wrong_number": "NUMERO ERRADO",
            "no_interest": "SEM INTERESSE",
            "unknown_person": "NAO CONHECE A PESSOA",
            "referral": "INDICACAO DE CONTATO",
        }
        output: Dict[str, str] = {}
        for key, label in keys.items():
            pattern = re.compile(
                rf"{re.escape(label)}\s*\n(.*?)(?=\n[A-Z0-9][^\n]*\n|\Z)",
                re.IGNORECASE | re.DOTALL,
            )
            match = pattern.search(section)
            if match:
                body = cls._clean_block(match.group(1) or "")
                if body:
                    output[key] = body
        return output

    def opening_script(self, lead: Dict[str, Any]) -> str:
        return self.get_day_message(1, lead=lead)

    @staticmethod
    def _render_spintax(text: str) -> str:
        rendered = str(text or "")
        pattern = re.compile(r"\{([^{}]+)\}")
        while True:
            match = pattern.search(rendered)
            if not match:
                break
            options = [item.strip() for item in str(match.group(1) or "").split("|") if item.strip()]
            replacement = random.choice(options) if options else ""
            rendered = f"{rendered[:match.start()]}{replacement}{rendered[match.end():]}"
        return rendered

    @staticmethod
    def _clean_company_name(value: str) -> str:
        cleaned = str(value or "").strip()
        if not cleaned:
            return "a empresa"
        
        # Remove suffixes and corporate structures
        cleaned = re.sub(r"\b(via|loja|shopping|sorteio|promo[cç][aã]o|filial|matriz)\b.*$", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\b(ltda|me|eireli|s\/a|sa|epp|mei|inc|corp|cia|company|unidade|centro)\b\.?", "", cleaned, flags=re.IGNORECASE)
        
        # Remove business types
        business_types = r"\b(comercio|comÃ©rcio|servicos|serviÃ§os|industria|indÃºstria|holding|grupo|distribuidora|atacadista|varejista|transportes|logistica|consultoria|assessoramento)\b"
        cleaned = re.sub(business_types, "", cleaned, flags=re.IGNORECASE)
        
        # Final cleanup
        cleaned = re.sub(r"\s{2,}", " ", cleaned).strip(" -|,:;/\\")
        
        # Se sobrar apenas 1 palavra curta (ex: "O"), ou ficar vazio
        if len(cleaned) <= 1 or not cleaned:
            return "a empresa"
            
        return cleaned

    @classmethod
    def _resolve_short_company_name(cls, lead: Dict[str, Any] | None = None, *, fallback: str = "a empresa") -> str:
        payload = dict(lead or {})
        candidates = [
            payload.get("nome_fantasia_curto"),
            payload.get("nome_fantasia"),
            payload.get("trade_name"),
            payload.get("company_name"),
            payload.get("empresa"),
            payload.get("company"),
        ]
        for candidate in candidates:
            raw_candidate = str(candidate or "").strip()
            cleaned = cls._clean_company_name(raw_candidate)
            if cleaned and cleaned.lower() not in {"a empresa", "empresa", "supermercado", "supermercados"}:
                if raw_candidate and cleaned != raw_candidate:
                    print(f"[NOME_NORMALIZADO] original={raw_candidate} final={cleaned}")
                return cleaned
        return str(fallback or "").strip()

    @staticmethod
    def _safe_first_name(value: Any) -> str:
        raw = str(value or "").strip()
        if not raw:
            return ""
        first = raw.split()[0].strip(" -_,.;:/\\|()[]{}")
        if not first:
            return ""
        lowered = first.lower()
        lowered_ascii = (
            lowered.replace("á", "a")
            .replace("à", "a")
            .replace("ã", "a")
            .replace("â", "a")
            .replace("é", "e")
            .replace("ê", "e")
            .replace("í", "i")
            .replace("ó", "o")
            .replace("ô", "o")
            .replace("õ", "o")
            .replace("ú", "u")
            .replace("ç", "c")
        )
        if any(ch.isdigit() for ch in first) or "@" in first:
            return ""
        if lowered_ascii in WhatsAppPitchEngine.GENERIC_FIRST_NAMES:
            return ""
        if any(token in lowered_ascii for token in ("drog", "farma", "supermerc", "atacadao", "juridic", "fiscal", "financeir")):
            return ""
        if len(first) > 14 and " " not in raw:
            return ""
        if len(first) <= 2:
            return ""
        return first[:1].upper() + first[1:].lower()

    @staticmethod
    def _render_placeholders(text: str, lead: Dict[str, Any] | None = None) -> str:
        payload = dict(lead or {})
        nome = WhatsAppPitchEngine._safe_first_name(payload.get("nome") or payload.get("first_name") or "")
        empresa = WhatsAppPitchEngine._resolve_short_company_name(payload, fallback="")
        has_specific_company = bool(empresa)
        segmento = str(payload.get("segmento") or payload.get("segment") or payload.get("industry") or "").strip()
        if not segmento:
            segmento = "marketing"
        rendered = str(text or "")

        if "__ABERTURA__" in rendered:
            opening_options = (
                WhatsAppPitchEngine.WHATSAPP_OPENINGS
                if has_specific_company
                else WhatsAppPitchEngine.WHATSAPP_OPENINGS_GENERIC
            )
            abertura = random.choice(opening_options)
            rendered = rendered.replace("__ABERTURA__", abertura)

        if nome:
            rendered = rendered.replace("__NOME__,", f"{nome},")
            rendered = rendered.replace("__NOME__?", f"{nome}?")
            rendered = rendered.replace("__NOME__", nome)
        else:
            rendered = rendered.replace("__NOME__,", "")
            rendered = rendered.replace("__NOME__?", "")
            rendered = rendered.replace("__NOME__", "")

        rendered = rendered.replace("__NOME__,", f"{nome}," if nome else "")
        rendered = rendered.replace("__EMPRESA__", empresa or "empresa de voces")
        rendered = rendered.replace("__SEGMENTO__", segmento)
        rendered = re.sub(r"\s{2,}", " ", rendered)
        rendered = re.sub(r"(^|\n)\s+,", r"\1", rendered)
        rendered = re.sub(r"\(\s*\)", "", rendered)
        return rendered.strip()

    def _build_short_opening(self, lead: Dict[str, Any] | None = None) -> str:
        payload = dict(lead or {})
        options = [
            "Oi, tudo bem? Vi que vocês são da área de __SEGMENTO__ e queria te fazer uma pergunta rápida.",
            "Fala, tudo certo? Posso te fazer uma pergunta bem direta sobre campanhas?",
            "Oi, rapidinho: vocês costumam rodar campanhas promocionais ou não é prioridade aí?",
            "Oi __NOME__, tudo bem? Posso te fazer uma pergunta rápida sobre campanhas promocionais?",
        ]
        if self._resolve_short_company_name(payload, fallback=""):
            options.append("Oi, tudo certo? Na __EMPRESA__, vocês costumam fazer campanhas promocionais?")
        chosen = self._render_placeholders(random.choice(options), lead=payload)
        return self._clean_block(self._render_spintax(chosen))

    @staticmethod
    def _cleanup_placeholders(text: str) -> str:
        cleaned = str(text or "")
        cleaned = re.sub(r"__[^_]+__", "", cleaned)
        cleaned = re.sub(r"\s{2,}", " ", cleaned)
        cleaned = re.sub(r" *\n *", "\n", cleaned)
        return cleaned.strip()

    def _quality_check_message(self, text: str, *, first_touch: bool = False) -> str:
        cleaned = self._cleanup_placeholders(self._clean_block(text))
        if first_touch:
            cleaned = cleaned.replace("...", ".")
        return cleaned

    def get_closing_message(self, key: str) -> str:
        normalized = str(key or "").strip().lower()
        return str(self.DEFAULT_CLOSING_OVERRIDES.get(normalized) or self._closings.get(normalized, "")).strip()

    def get_day_message(self, day: int, *, lead: Dict[str, Any] | None = None) -> str:
        day = int(day or 0)
        if day == 1:
            opening = self._build_short_opening(lead=lead)
            question = "Com a Copa chegando, isso entra no radar de vocês ou não é prioridade agora?"
            rendered = self._clean_block(f"{opening}\n{question}")
            return self._quality_check_message(rendered, first_touch=True)
        base = str(self.DEFAULT_DAY_OVERRIDES.get(day) or self._days.get(day, "")).strip()
        base = self._render_placeholders(base, lead=lead)
        rendered = self._render_spintax(base).strip()
        return self._quality_check_message(rendered, first_touch=False)

    def get_cadence_message(self, cadence_step: int, *, lead: Dict[str, Any] | None = None) -> str:
        step = max(1, min(6, int(cadence_step or 1)))
        has_specific_company = bool(self._resolve_short_company_name(lead, fallback=""))
        
        # Define variations for each step
        variations = {
            1: [
                "__ABERTURA__\n\nEstava olhando algumas empresas do setor de voces e notei um padrao curioso nas estrategias de vendas.\n\nPosso te perguntar algo rapido? Hoje voces usam campanhas promocionais para gerar clientes ou o crescimento vem mais de indicacao?",
                "__ABERTURA__\n\nEstava analisando algumas empresas parecidas com a __EMPRESA__ e notei um padrao curioso no crescimento de vendas.\n\nPosso te perguntar algo rapido? Voces ja usam campanhas promocionais para gerar novos clientes?",
                "__ABERTURA__\n\nVi o perfil da __EMPRESA__ e fiquei com uma curiosidade rapida sobre a parte comercial.\n\nHoje voces utilizam campanhas promocionais para atrair clientes ou o foco esta mais em vendas diretas?"
            ],
            2: [
                "__ABERTURA__\n\nAnalisando algumas empresas do mercado de voces percebi algo curioso na forma como muitas estao gerando vendas ultimamente.\n\nQueria te perguntar algo rapido: voces ja usam campanhas promocionais para gerar demanda?",
                "__ABERTURA__\n\nNotei que muitas empresas do seu setor estao escalando rapido usando campanhas gamificadas.\n\nVoces ja chegaram a testar algo nesse sentido ou focam mais no tradicional?",
            ],
            3: [
                "__ABERTURA__\n\nPassando para saber se conseguiu ver minha mensagem anterior.\n\nComo a Copa esta chegando, acredito que a __EMPRESA__ poderia aproveitar bastante as acoes gamificadas que comentei.\n\nFaz sentido a gente falar 2 minutinhos?",
                "__ABERTURA__\n\nOi, tudo bem? Queria muito te mostrar como a Mand Digital esta ajudando empresas como a __EMPRESA__ a captar dados reais e converter em vendas.\n\nConseguimos falar essa semana?",
            ],
            4: [
                "__ABERTURA__\n\nSei que a rotina deve estar corrida, mas nao queria deixar passar essa oportunidade para a __EMPRESA__ agora na Copa.\n\nPodemos agendar um papo rapido de 5 minutos?",
                "__ABERTURA__\n\nAcredito que a estrutura que comentei pode ser um divisor de aguas para o marketing de voces.\n\nTopa uma conversa rapida para eu te mostrar como funciona?",
            ],
            5: [
                "__ABERTURA__\n\nImagino que nao tenha sido o melhor momento para conversar.\n\nVou encerrar por aqui para nao ficar incomodando. Mas se em algum momento fizer sentido falar sobre campanhas promocionais para gerar vendas e engajamento, fico a disposicao.",
            ],
            6: [
                "__ABERTURA__\n\nComo nao tive retorno, vou encerrar meu contato por aqui para nao virar insistencia.\n\nSe fizer sentido retomar depois sobre campanhas da Copa, captacao de dados e aumento de vendas, fico a disposicao.",
            ]
        }
        
        # Variations for generic case (no company name)
        variations_generic = {
            1: variations[1],
            2: variations[2],
            3: [
                "__ABERTURA__\n\nFiquei com uma curiosidade rapida sobre a parte comercial de voces.\n\nHoje voces utilizam campanhas promocionais para atrair clientes ou o foco esta mais em vendas diretas?",
                "__ABERTURA__\n\nQueria te perguntar algo rapido: voces ja usam campanhas promocionais para gerar demanda ou estao focados em outros canais?",
            ],
            4: [
                "__ABERTURA__\n\nAnalisando empresas desse setor, notei um padrao curioso no crescimento de vendas.\n\nPosso te perguntar algo rapido? Voces ja usam campanhas promocionais para gerar novos clientes?",
            ],
            5: variations[5],
            6: variations[6]
        }
        
        target_dict = variations if has_specific_company else variations_generic
        options = target_dict.get(step) or variations[1]
        
        # Use deal_id for deterministic choice if available, else random
        deal_id = int((lead or {}).get("id") or 0)
        if deal_id > 0:
            chosen = options[deal_id % len(options)]
        else:
            chosen = random.choice(options)
            
        base = self._render_placeholders(chosen, lead=lead)
        rendered = self._render_spintax(base).strip()
        return self._quality_check_message(rendered, first_touch=True)

    def next_reply_from_stage(self, current_step: int, *, lead: Dict[str, Any] | None = None) -> str:
        next_step = max(1, min(10, int(current_step or 1)))
        return self.get_day_message(next_step, lead=lead)

    @staticmethod
    def _normalize_message(text: str) -> str:
        normalized = str(text or "").strip().lower()
        normalized = re.sub(r"\s+", " ", normalized)
        return normalized

    @staticmethod
    def _normalize_opt_out_message(text: str) -> str:
        normalized = unicodedata.normalize("NFKD", str(text or ""))
        normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
        normalized = normalized.lower()
        return re.sub(r"\s+", " ", normalized).strip()

    @classmethod
    def _is_opt_out_message(cls, text: str) -> bool:
        normalized = cls._normalize_opt_out_message(text)
        if not normalized:
            return False

        direct_tokens = (
            "nao tenho interesse",
            "sem interesse",
            "nao quero",
            "nao quero receber",
            "nao me chama",
            "nao me manda",
            "nao manda mais",
            "nao me envie",
            "nao insist",
            "remove meu numero",
            "remova meu numero",
            "retira meu numero",
            "retire meu numero",
            "tira meu numero",
            "me tira da lista",
            "tire meu numero",
            "descadastra",
            "descadastre",
            "para de mandar",
            "pare de mandar",
            "para de me mandar",
            "pare de me mandar",
            "para com isso",
            "me deixa em paz",
        )
        if any(token in normalized for token in direct_tokens):
            return True

        opt_out_patterns = (
            r"\bpara de (me )?(mandar|enviar|disparar|chamar|falar)\b",
            r"\bpare de (me )?(mandar|enviar|disparar|chamar|falar)\b",
            r"\bnao (quero|aceito) (mais )?(receber|mensagem|mensagens|contato)\b",
            r"\bnao (me )?(manda|mande|envia|envie|chama|chame) mais\b",
            r"\b(remova|remove|retira|retire|tira|tire) (meu )?(numero|contato)\b",
            r"\b(descadastra|descadastre|descadastrar)\b",
        )
        return any(re.search(pattern, normalized) for pattern in opt_out_patterns)

    @classmethod
    def detect_nonlead_intent(cls, text: str) -> str:
        normalized = cls._normalize_opt_out_message(text)
        if not normalized:
            return ""
        for intent_name, direct_tokens, regex_patterns in cls.NONLEAD_INTENT_RULES:
            if any(token in normalized for token in direct_tokens):
                return intent_name
            if any(re.search(pattern, normalized) for pattern in regex_patterns):
                return intent_name
        return ""

    @classmethod
    def _normalize_schedule_phrase(cls, text: str) -> str:
        normalized = cls._normalize_message(text)
        replacements = (
            (r"\bamanha\b", "amanhã"),
            (r"\bterca\b", "terça"),
            (r"\bsabado\b", "sábado"),
            (r"\bde manha\b", "de manhã"),
            (r"\bna manha\b", "na manhã"),
            (r"\bpela manha\b", "pela manhã"),
            (r"\ba tarde\b", "à tarde"),
            (r"\ba noite\b", "à noite"),
        )
        for pattern, replacement in replacements:
            normalized = re.sub(pattern, replacement, normalized)
        return normalized.strip(" ,.;:-")

    @classmethod
    def _extract_schedule_phrase(cls, text: str) -> str:
        normalized = cls._normalize_schedule_phrase(text)
        if not normalized:
            return ""

        day_match = cls.SCHEDULE_DAY_PATTERN.search(normalized)
        period_match = cls.SCHEDULE_PERIOD_PATTERN.search(normalized)

        if day_match and period_match:
            if day_match.start() <= period_match.start():
                return normalized[day_match.start():period_match.end()].strip(" ,.;:-")
            return normalized[period_match.start():day_match.end()].strip(" ,.;:-")
        if day_match:
            return day_match.group(0).strip()
        if period_match:
            return period_match.group(0).strip()
        return ""

    @classmethod
    def _schedule_hour_examples(cls, text: str) -> str:
        normalized = cls._normalize_schedule_phrase(text)
        if re.search(r"\b(de manhã|na manhã|pela manhã|cedo)\b", normalized):
            return "9h, 10h"
        if re.search(r"\b(à tarde|de tarde|na tarde|no fim da tarde)\b", normalized):
            return "14h, 15h"
        if re.search(r"\b(à noite|de noite|na noite)\b", normalized):
            return "18h, 19h"
        return "9h, 10h"

    def _should_use_ai_fallback(self, inbound_messages: List[str], candidate_reply: str) -> bool:
        if not str(candidate_reply or "").strip():
            return True
        if not inbound_messages:
            return False
        latest = self._normalize_message(inbound_messages[-1])
        if not latest:
            return False
        if len(latest) <= 2:
            return False

        scripted_patterns = (
            "sim",
            "não",
            "nao",
            "temos",
            "tem campanha",
            "fazemos campanha",
            "quero",
            "interesse",
            "manda",
            "envia",
            "pode",
            "horario",
            "horário",
            "email",
            "reuni",
            "call",
            "agenda",
        )
        if any(token in latest for token in scripted_patterns):
            return False
        if "?" in latest:
            return True
        if len(latest.split()) >= 6:
            return True
        return False

    def _rule_based_fluid_reply(self, lead: Dict[str, Any], inbound_messages: List[str]) -> str:
        if not inbound_messages:
            return ""
        raw_latest = str(inbound_messages[-1] or "").strip()
        if self._is_opt_out_message(raw_latest):
            return str(self.get_closing_message("no_interest") or "").strip()
        if self.detect_nonlead_intent(raw_latest):
            return ""
        latest = self._normalize_message(inbound_messages[-1])
        if not latest:
            return ""

        for keywords, templates in self.FLUID_REPLY_RULES:
            if any(keyword in latest for keyword in keywords):
                chosen = random.choice(templates)
                rendered = self._render_placeholders(chosen, lead=lead)
                rendered = self._render_spintax(rendered)
                return self._clean_block(rendered)
        return ""

    def _soften_scripted_reply(self, text: str) -> str:
        softened = self._clean_block(text)
        softened = softened.replace("Vou direto ao ponto contigo então.", "Vou te falar de forma bem direta.")
        softened = softened.replace("Queria entender uma coisa rapidinho:", "Queria te perguntar uma coisa:")
        softened = softened.replace("Foi exatamente por isso que criamos aqui na Mand as campanhas gamificadas.", "Foi por isso que a Mand estruturou esse modelo de campanha.")
        softened = re.sub(r"\n{3,}", "\n\n", softened)
        return softened.strip()

    def _gemini_api_keys(self) -> List[str]:
        keys: List[str] = []
        for candidate in (
            os.getenv("GEMINI_API_KEY_1", ""),
            os.getenv("GEMINI_API_KEY_2", ""),
            os.getenv("GEMINI_API_KEY", ""),
            get_config_value("gemini_api_key_primary", ""),
            get_config_value("gemini_api_key_fallback", ""),
            get_config_value("gemini_api_key", ""),
        ):
            token = str(candidate or "").strip()
            if token and token not in keys:
                keys.append(token)
        return keys

    def _gemini_generate_text(self, prompt: str, *, max_output_tokens: int = 180, temperature: float = 0.7) -> str:
        prompt_text = str(prompt or "").strip()
        if not prompt_text:
            return ""
        payload = {
            "contents": [{"role": "user", "parts": [{"text": prompt_text}]}],
            "generationConfig": {
                "temperature": float(temperature or 0),
                "maxOutputTokens": max(1, int(max_output_tokens or 180)),
            },
        }
        for api_key in self._gemini_api_keys():
            try:
                url = self.GEMINI_GENERATE_URL_TEMPLATE.format(model=self.gemini_model)
                response = requests.post(url, params={"key": api_key}, json=payload, timeout=12)
                if int(response.status_code or 0) >= 400:
                    continue
                body = response.json() if response.content else {}
                candidates = list((body or {}).get("candidates") or [])
                if not candidates:
                    continue
                parts = (((candidates[0] or {}).get("content") or {}).get("parts") or [])
                for part in parts:
                    maybe_text = str((part or {}).get("text") or "").strip()
                    if maybe_text:
                        return maybe_text
            except Exception:
                continue
        return ""

    def detect_stateful_intent(self, text: str) -> str:
        if self._is_opt_out_message(text):
            return ""
        if self.detect_nonlead_intent(text):
            return ""
        normalized = self._normalize_message(text)
        if re.search(r"\b\d{1,2}(:\d{2})?\s*h\b", normalized) or re.search(r"\b\d{1,2}:\d{2}\b", normalized):
            return "agendamento_horario_definido"
        if any(token in normalized for token in ("depois das", "apos as", "após as")) and re.search(r"\b\d{1,2}\b", normalized):
            return "agendamento_horario_definido"
        availability_tokens = (
            "amanha", "amanhã", "hoje",
            "segunda", "terça", "terca", "quarta", "quinta", "sexta", "sábado", "sabado", "domingo",
            "tarde", "manha", "manhã", "noite", "cedo",
        )
        if any(token in normalized for token in availability_tokens):
            return "agendamento_disponivel"
        return ""

    def infer_question_id(self, text: str) -> str:
        normalized = self._normalize_message(text)
        if "qual horario" in normalized or "qual horário" in normalized or "horario fica melhor" in normalized or "horário fica melhor" in normalized:
            return "horario"
        if "melhor e-mail" in normalized or "melhor email" in normalized:
            return "email"
        return ""

    def build_stateful_reply(
        self,
        lead: Dict[str, Any],
        inbound_messages: List[str],
        *,
        current_step: int = 1,
        conversation_state: Dict[str, Any] | None = None,
        detected_intent: str = "",
    ) -> Dict[str, str]:
        state = dict(conversation_state or {})
        etapa_atual = str(state.get("etapa") or "inicio").strip() or "inicio"
        last_question_id = str(state.get("last_question_id") or "").strip()
        last_question = str(state.get("last_question") or "").strip()
        latest = str((inbound_messages or [""])[-1] or "").strip()

        if self._is_opt_out_message(latest):
            return {
                "reply": str(self.get_closing_message("no_interest") or "").strip(),
                "estado_atual": etapa_atual,
                "estado_novo": "encerrado",
                "question_id": "",
                "last_question": last_question,
                "last_lead_reply": latest,
                "intent": "sem_interesse",
            }

        if detected_intent == "agendamento_disponivel":
            print("[INTENT_DETECTADA] agendamento_disponivel")
            print(f"[ESTADO_ATUAL] {etapa_atual}")
            novo_estado = "confirmando_agendamento"
            print(f"[ESTADO_NOVO] {novo_estado}")
            if etapa_atual == "perguntou_horario" or last_question_id == "horario":
                schedule_phrase = self._extract_schedule_phrase(latest)
                examples = self._schedule_hour_examples(schedule_phrase or latest)
                reply = (
                    f"Perfeito, {schedule_phrase or 'esse período'} funciona sim 🙌\n\n"
                    f"Qual horário fica melhor pra você? (ex: {examples}...)"
                )
            else:
                reply = (
                    "Perfeito 🙌\n\n"
                    "Qual horário fica melhor pra você? (ex: 14h, 15h...)"
                )
            return {
                "reply": reply,
                "estado_atual": etapa_atual,
                "estado_novo": novo_estado,
                "question_id": "horario_detalhe",
                "last_question": last_question,
                "last_lead_reply": latest,
                "intent": detected_intent,
            }

        if detected_intent == "agendamento_horario_definido":
            print("[INTENT_DETECTADA] agendamento_horario_definido")
            print(f"[ESTADO_ATUAL] {etapa_atual}")
            novo_estado = "coletando_email_convite"
            print(f"[ESTADO_NOVO] {novo_estado}")
            email = str((lead or {}).get("email") or "").strip().lower()
            horario = latest
            if email:
                reply = (
                    f"Perfeito 🙌 {horario} funciona.\n\n"
                    f"Posso te enviar o convite da reunião nesse e-mail: {email}?"
                )
                question_id = "confirmar_email"
            else:
                reply = (
                    f"Perfeito 🙌 {horario} funciona.\n\n"
                    "Qual o melhor e-mail pra eu te enviar o convite da reunião?"
                )
                question_id = "email"
            return {
                "reply": reply,
                "estado_atual": etapa_atual,
                "estado_novo": novo_estado,
                "question_id": question_id,
                "last_question": last_question,
                "last_lead_reply": latest,
                "intent": detected_intent,
            }

        reply = self.build_reply(lead, inbound_messages, current_step=current_step)
        question_id = self.infer_question_id(reply)
        novo_estado = etapa_atual
        if question_id == "horario":
            novo_estado = "perguntou_horario"
        elif question_id == "email":
            novo_estado = "pedindo_email"
        if question_id and question_id == last_question_id:
            if question_id == "horario":
                reply = (
                    "Perfeito 🙌\n\n"
                    "Se amanhã à tarde te ajuda, me diz só um horário melhor pra você. "
                    "(ex: 14h, 15h...)"
                )
                novo_estado = "confirmando_agendamento"
                question_id = "horario_detalhe"
        return {
            "reply": reply,
            "estado_atual": etapa_atual,
            "estado_novo": novo_estado,
            "question_id": question_id,
            "last_question": reply,
            "last_lead_reply": latest,
            "intent": detected_intent,
        }

    def build_reply(self, lead: Dict[str, Any], inbound_messages: List[str], *, current_step: int = 1) -> str:
        step = max(1, min(10, int(current_step or 1)))
        fluid_reply = self._rule_based_fluid_reply(lead, inbound_messages)
        if fluid_reply:
            return fluid_reply
        reply = self.next_reply_from_stage(step, lead=lead)
        reply = self._soften_scripted_reply(reply)
        if self._should_use_ai_fallback(inbound_messages, reply):
            ai_builder = getattr(self, "_gemini_reply", None)
            if callable(ai_builder):
                ai_reply = ai_builder(lead, inbound_messages)
                if ai_reply:
                    print("[IA_FALLBACK_USADA]")
                    return ai_reply
        return reply

    def _gemini_reply(self, lead: Dict[str, Any], inbound_messages: List[str]) -> str:
        if not inbound_messages:
            return ""
        nome = str((lead or {}).get("nome") or (lead or {}).get("name") or "Cliente").strip() or "Cliente"
        empresa = self._resolve_short_company_name(lead)
        historico = "\n".join(f"- {str(item or '').strip()}" for item in inbound_messages[-5:] if str(item or "").strip())
        prompt = (
            "Voce e a Carol, SDR da Mand Digital. Responda em portugues do Brasil, de forma curta, natural e objetiva. "
            "Use o pitch da Copa do Mundo apenas quando ajudar a conversa comercial.\n"
            "Regras:\n"
            "- Nao invente desconto, prazo ou funcionalidade.\n"
            "- Nao repita a mensagem anterior do bot.\n"
            "- Priorize conduzir para call/agendamento quando houver interesse.\n"
            "- Se a mensagem indicar desinteresse, nao escreva follow-up comercial.\n\n"
            f"Lead: {nome}\n"
            f"Empresa: {empresa}\n"
            f"Historico recente:\n{historico}\n\n"
            "Responda apenas com a mensagem que a Carol deve enviar agora."
        )
        reply = self._gemini_generate_text(prompt, max_output_tokens=180, temperature=0.7)
        reply = self._clean_block(str(reply or "").replace("Carol:", "").replace("Carol SDR:", "").strip())
        return reply

    def find_lead_context(self, phone: str) -> Dict[str, Any]:
        return {"telefone": str(phone or "").strip()}

    def identify_stage(self, lead: Dict[str, Any]) -> str:
        raw = (lead or {}).get("cadence_step") or (lead or {}).get("sheet_day") or 1
        digits = re.sub(r"\D+", "", str(raw or "1"))
        return f"cad{max(1, min(10, int(digits or '1')))}"
