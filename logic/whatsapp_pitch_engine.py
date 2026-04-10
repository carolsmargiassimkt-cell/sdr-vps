from __future__ import annotations

import random
import re
from typing import Any, Dict, List

import requests


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
    }
    WHATSAPP_OPENINGS = [
        "{Oi|Olá}, tudo bem? Aqui é a Carol da Mand Digital. {Nesse contato|Por aqui} consigo falar com {a pessoa responsável|quem cuida} por marketing e vendas {aí na __EMPRESA__|na empresa}?",
        "{Oi|Olá}, tudo bem? Carol da Mand Digital aqui. {Consigo falar por aqui|Falo com você por aqui} com {quem responde|a pessoa responsável} por marketing e vendas {da __EMPRESA__|aí na empresa}?",
        "{Oi|Olá}, tudo bem? Aqui é a Carol da Mand Digital. {Você fala com marketing e vendas|Você é a pessoa que cuida de marketing e vendas} {aí na __EMPRESA__|na empresa}?",
        "{Oi|Olá}, tudo bem? Carol da Mand Digital falando. {Queria confirmar uma coisa rapidinho|Me confirma uma coisa rapidinho}: por esse contato eu consigo falar com {o responsável|a pessoa responsável} por marketing e vendas {aí na __EMPRESA__|da empresa}?",
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
        }
    ]
    FLUID_REPLY_RULES = (
        (
            ("oi", "olá", "ola", "bom dia", "boa tarde", "boa noite"),
            [
                "{Oi|Olá}! Tudo bem por aí?\n\nSou a Carol, da Mand. Queria te explicar rapidinho como a gente usa campanhas para gerar venda e captar dados reais.\n\nFaz sentido eu te resumir em 1 minuto?",
                "{Oi|Olá}! Tudo certo?\n\nAqui é a Carol, da Mand. Posso te explicar de forma bem direta como essa estrutura funciona na prática?\n\nSe fizer sentido, eu já te mostro o próximo passo.",
            ],
        ),
        (
            ("quero entender", "como funciona", "me explica", "entender melhor", "como seria", "não entendi", "nao entendi"),
            [
                "Claro.\n\nA gente monta campanhas promocionais e gamificadas para gerar engajamento, captar dados reais e acompanhar resultado de ponta a ponta.\n\nHoje vocês já fazem algo nessa linha ou seria a primeira vez?",
                "Claro, vou resumir.\n\nA Mand estrutura a campanha, coleta os dados dos participantes e acompanha o resultado para isso virar venda, não só movimento.\n\nVocês já têm alguma ação parecida hoje?",
            ],
        ),
        (
            ("sim", "tem interesse", "interesse", "faz sentido", "legal", "curti", "gostei"),
            [
                "Boa.\n\nO ponto central é fazer a campanha gerar resultado mensurável, não só alcance.\n\nHoje vocês conseguem identificar quem participou e depois transformar isso em venda?",
                "Perfeito.\n\nÉ justamente aí que a Mand entra: campanha com dado, rastreio e retorno claro.\n\nVocês já medem esse tipo de ação hoje ou ainda fica mais no feeling?",
            ],
        ),
        (
            ("preço", "preco", "valor", "quanto custa", "investimento"),
            [
                "Depende um pouco do formato da campanha e do objetivo.\n\nAntes de te falar valor solto, faz mais sentido eu entender o cenário de vocês para te passar algo coerente.\n\nHoje a prioridade é captação, recorrência ou ativação?",
                "O investimento varia conforme a mecânica e o volume da ação.\n\nSe você quiser, eu te explico rapidamente os formatos mais comuns e já te digo em qual faixa vocês provavelmente entrariam.",
            ],
        ),
        (
            ("reunião", "reuniao", "call", "agenda", "horário", "horario", "pode ser"),
            [
                "Perfeito.\n\nConsigo te mostrar isso de forma bem objetiva.\n\nQual horário fica melhor para você?",
                "Ótimo.\n\nFaz sentido a gente falar rapidinho para eu te mostrar como isso funcionaria no caso de vocês.\n\nQual horário te ajuda mais?",
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
    CADENCE_OPENINGS = {
        1: (
            "__ABERTURA__\n\n"
            "Estava olhando algumas empresas do setor de voces e notei um padrao curioso nas estrategias de vendas.\n\n"
            "Posso te perguntar algo rapido? Hoje voces usam campanhas promocionais para gerar clientes ou o crescimento vem mais de indicacao?"
        ),
        2: (
            "__ABERTURA__\n\n"
            "Analisando algumas empresas do mercado de voces percebi algo curioso na forma como muitas estao gerando vendas ultimamente.\n\n"
            "Queria te perguntar algo rapido: voces ja usam campanhas promocionais para gerar demanda?"
        ),
        3: (
            "__ABERTURA__\n\n"
            "Vi o perfil da __EMPRESA__ e fiquei com uma curiosidade rapida sobre a parte comercial.\n\n"
            "Hoje voces utilizam campanhas promocionais para atrair clientes ou o foco esta mais em vendas diretas?"
        ),
        4: (
            "__ABERTURA__\n\n"
            "Estava analisando algumas empresas parecidas com a __EMPRESA__ e notei um padrao curioso no crescimento de vendas.\n\n"
            "Posso te perguntar algo rapido? Voces ja usam campanhas promocionais para gerar novos clientes?"
        ),
        5: (
            "__ABERTURA__\n\n"
            "Imagino que a rotina esteja corrida por ai e talvez nao tenha sido o melhor momento para conversar.\n\n"
            "Vou encerrar por aqui para nao ficar incomodando. Mas se em algum momento fizer sentido falar sobre campanhas promocionais para gerar vendas e engajamento, fico a disposicao."
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
    OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
    OPENROUTER_TOKEN = "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"

    def __init__(self, config: Any, logger=None) -> None:
        self.config = config
        self.logger = logger
        self._pitch_text = str(getattr(config, "pitch", "") or "")
        self._days = self._parse_day_messages(self._pitch_text)
        self._closings = self._parse_closing_messages(self._pitch_text)

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

    def cad1_script(self, lead: Dict[str, Any]) -> str:
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
        cleaned = re.sub(r"\b(via|loja|shopping|sorteio|promo[cç][aã]o)\b.*$", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\b(ltda|me|eireli|s\/a|sa|epp|mei)\b\.?", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\b(comercio|comÃ©rcio|servicos|serviÃ§os|industria|indÃºstria|holding|grupo)\b", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\s{2,}", " ", cleaned).strip(" -|,:;")
        return cleaned or "a empresa"

    @classmethod
    def _resolve_short_company_name(cls, lead: Dict[str, Any] | None = None) -> str:
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
        return "a empresa"

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
        empresa = WhatsAppPitchEngine._resolve_short_company_name(payload)
        segmento = str(payload.get("segmento") or payload.get("segment") or payload.get("industry") or "").strip()
        if not segmento:
            segmento = "marketing"
        rendered = str(text or "")

        if "__ABERTURA__" in rendered:
            abertura = random.choice(WhatsAppPitchEngine.WHATSAPP_OPENINGS)
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
        rendered = rendered.replace("__EMPRESA__", empresa)
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
            "Oi, tudo certo? Na __EMPRESA__, vocês costumam fazer campanhas promocionais?",
        ]
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

    def get_closing_message(self, key: str) -> str:
        normalized = str(key or "").strip().lower()
        return str(self.DEFAULT_CLOSING_OVERRIDES.get(normalized) or self._closings.get(normalized, "")).strip()

    def get_day_message(self, day: int, *, lead: Dict[str, Any] | None = None) -> str:
        day = int(day or 0)
        base = str(self.DEFAULT_DAY_OVERRIDES.get(day) or self._days.get(day, "")).strip()
        base = self._render_placeholders(base, lead=lead)
        rendered = self._render_spintax(base).strip()
        return self._quality_check_message(rendered, first_touch=(day == 1))

    def get_cadence_message(self, cadence_step: int, *, lead: Dict[str, Any] | None = None) -> str:
        step = max(1, min(5, int(cadence_step or 1)))
        base = str(self.CADENCE_OPENINGS.get(step) or self.CADENCE_OPENINGS[1]).strip()
        base = self._render_placeholders(base, lead=lead)
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

    def _openrouter_reply(self, lead: Dict[str, Any], inbound_messages: List[str]) -> str:
        latest = str((inbound_messages or [""])[-1] or "").strip()
        if not latest:
            return ""
        nome = str((lead or {}).get("nome") or "").strip()
        context = latest
        if nome:
            context = f"Lead: {nome}\nMensagem: {latest}"
        payload = {
            "model": "openrouter/auto",
            "messages": [
                {
                    "role": "system",
                    "content": "Você é um SDR consultivo seguindo o pitch da Copa. Seja direto, natural e leve. Responda em no máximo 3 linhas. Não mencione IA. Conduza a conversa para o próximo passo comercial.",
                },
                {
                    "role": "user",
                    "content": context,
                },
            ],
        }
        headers = {
            "Authorization": f"Bearer {self.OPENROUTER_TOKEN}",
            "Content-Type": "application/json",
        }
        try:
            response = requests.post(self.OPENROUTER_URL, headers=headers, json=payload, timeout=8)
            if response.status_code != 200:
                return ""
            body = response.json()
            choices = body.get("choices") or []
            if not choices:
                return ""
            content = (((choices[0] or {}).get("message") or {}).get("content") or "").strip()
            content = self._clean_block(content)
            content = "\n".join(content.splitlines()[:3]).strip()
            return content
        except Exception:
            return ""

    def build_reply(self, lead: Dict[str, Any], inbound_messages: List[str], *, current_step: int = 1) -> str:
        step = max(1, min(10, int(current_step or 1)))
        fluid_reply = self._rule_based_fluid_reply(lead, inbound_messages)
        if fluid_reply:
            return fluid_reply
        reply = self.next_reply_from_stage(step, lead=lead)
        reply = self._soften_scripted_reply(reply)
        if self._should_use_ai_fallback(inbound_messages, reply):
            ai_reply = self._openrouter_reply(lead, inbound_messages)
            if ai_reply:
                print("[IA_FALLBACK_USADA]")
                return ai_reply
        return reply

    def find_lead_context(self, phone: str) -> Dict[str, Any]:
        return {"telefone": str(phone or "").strip()}

    def identify_stage(self, lead: Dict[str, Any]) -> str:
        raw = (lead or {}).get("cadence_step") or (lead or {}).get("sheet_day") or 1
        digits = re.sub(r"\D+", "", str(raw or "1"))
        return f"cad{max(1, min(10, int(digits or '1')))}"
