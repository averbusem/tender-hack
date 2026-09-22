"""Классификатор тематики обращений и маршрутизации по линиям поддержки."""

import asyncio
import re
import time
from dataclasses import dataclass
from typing import Any, ClassVar, Literal, Protocol, runtime_checkable

from src.rag.schemas import EntityItem, QueryRouterOutput


@runtime_checkable
class LlmRouterClientProtocol(Protocol):
    """Протокол взаимодействия роутера с клиентом языковой модели."""

    async def generate_structured(
        self,
        schema: type[QueryRouterOutput],
        prompt: str,
        system_prompt: str,
        timeout: float,
    ) -> QueryRouterOutput:
        """Генерирует структурированный ответ модели по заданной Pydantic-схеме."""
        ...


class MockLlmClient:
    """Тестовый клиент для локальной разработки, изоляции модулей и юнит-тестов."""

    def __init__(
        self,
        default_response: QueryRouterOutput | None = None,
        delay: float = 0.0,
        fail_times: int = 0,
    ) -> None:
        """Инициализирует тестовый мок-клиент с заданным поведением."""
        self.default_response = default_response
        self.delay = delay
        self.fail_times = fail_times
        self.call_count = 0

    async def generate_structured(
        self,
        schema: type[QueryRouterOutput],
        prompt: str,
        system_prompt: str,
        timeout: float,
    ) -> QueryRouterOutput:
        """Эмулирует структурированный ответ языковой модели."""
        self.call_count += 1
        if self.delay > 0:
            await asyncio.sleep(self.delay)
        if self.call_count <= self.fail_times:
            raise ValueError("LLM generation error (mock failure)")

        if self.default_response is not None:
            return self.default_response.model_copy(deep=True)

        query_text = prompt.strip()
        marker = "ТЕКУЩЕЕ СООБЩЕНИЕ ПОЛЬЗОВАТЕЛЯ:\n"
        if marker in prompt:
            parts = prompt.split(marker, 1)[1]
            query_text = parts.split("\n\nВерни строго валидный JSON")[
                0
            ].strip()

        return QueryRouterOutput(
            intent="qa",
            regime_hint="MOS_PORTAL",
            topic="general_faq",
            priority="P2",
            support_line="L1",
            sentiment="neutral",
            follow_up_type="none",
            error_codes=[],
            escalation_requested=False,
            entities=[],
            standalone_query=query_text[:500],
            sub_queries=[],
        )


@dataclass
class PrePassResult:
    """Результат детерминированного предварительного анализа запроса регулярками."""

    error_codes: list[str]
    entities: list[EntityItem]
    regime_hint: Literal["44-FZ", "223-FZ", "MOS_PORTAL"] | None
    caps_p0_triggered: bool
    threat_p0_triggered: bool
    financial_dispute_p0_triggered: bool
    deadline_p1_triggered: bool
    escalation_requested: bool


class DeterministicPrePass:
    """Детерминированный анализатор ключевых сущностей и критических маркеров."""

    ERROR_CODE_REGEX = re.compile(r"\b0x[0-9A-Fa-f]{8}\b")

    LAW_44_REGEX = re.compile(
        r"\b44\s*[-‑–—]?\s*фз\b|\bфз\s*[-‑–—]?\s*44\b", re.IGNORECASE
    )
    LAW_223_REGEX = re.compile(
        r"\b223\s*[-‑–—]?\s*фз\b|\bфз\s*[-‑–—]?\s*223\b", re.IGNORECASE
    )
    PORTAL_REGEX = re.compile(
        r"\b(?:портал(?:е|а)?\s+поставщиков|котировочн(?:ой|ую|ые|ых)?\s+сесси(?:и|й|я)|"
        r"оферт(?:а|ы|у|е)|регламент(?:а|е)?\s+портала|сте\b|еруз\b)",
        re.IGNORECASE,
    )

    ARTICLE_REGEX = re.compile(
        r"\b(?:стать[еяию]|ст\.)\s*(\d+(?:\.\d+)?)\b", re.IGNORECASE
    )
    PART_REGEX = re.compile(r"\b(?:част[ьиею]|ч\.)\s*(\d+)\b", re.IGNORECASE)

    THREAT_REGEX = re.compile(
        r"\b(?:суд[аеуом]?|судебн(?:ый|ого|ому|ым|ое|ую)|прокуратур[аеуы]|"
        r"фас|жалоб[аеуомы]|исков(?:ое|ый|ого|ую)|иск)\b",
        re.IGNORECASE,
    )

    DISPUTE_FINANCE_KEYWORD_REGEX = re.compile(
        r"\b(?:списан[а-я]*|деньг[а-я]*|денег|денежн[а-я]*|"
        r"средств[а-я]*|блокировк[а-я]*|заблокир[а-я]*|"
        r"удержан[а-я]*|обеспечен[а-я]*)\b",
        re.IGNORECASE,
    )
    DISPUTE_CONFLICT_KEYWORD_REGEX = re.compile(
        r"\b(?:не\s+соглас[а-я]*|верните|вернуть|незаконн[а-я]*|"
        r"ошибочн[а-я]*|разблокируйт[а-я]*|неправомерн[а-я]*|украли)\b",
        re.IGNORECASE,
    )

    DEADLINE_REGEX = re.compile(
        r"\b(?:истекает|заканчивается|дедлайн|до\s+конца\s+дня|меньше\s+суток|"
        r"осталось\s+(?:менее|меньше)?\s*(?:\d+|суток|дня|часов)|"
        r"через\s+\d+\s+час(?:а|ов)?)\b",
        re.IGNORECASE,
    )

    ESCALATION_REGEX = re.compile(
        r"\b(?:оператор[аеу]?|человек[а]?|специалист[а]?|живо[йг]|"
        r"переведи(?:те)?|соедини(?:те)?)\b",
        re.IGNORECASE,
    )

    @staticmethod
    def is_caps_message(text: str) -> bool:
        """Проверяет, является ли сообщение написанным в верхнем регистре (КАПС).

        Критерий: не менее 10 буквенных символов и доля заглавных букв не менее 70%.
        """
        letters = [c for c in text if c.isalpha()]
        if len(letters) < 10:
            return False
        upper_count = sum(1 for c in letters if c.isupper())
        return (upper_count / len(letters)) >= 0.70

    @classmethod
    def check_consecutive_caps(
        cls,
        current_query: str,
        conversation_history: list[dict[str, Any]] | None,
    ) -> bool:
        """Определяет наличие >= 3 сообщений подряд в верхнем регистре от клиента."""
        if not cls.is_caps_message(current_query):
            return False

        if not conversation_history:
            return False

        client_messages: list[str] = []
        for msg in conversation_history:
            sender = str(msg.get("sender") or msg.get("role") or "").lower()
            if sender in ("client", "user"):
                text = str(msg.get("text") or "")
                client_messages.append(text)

        if len(client_messages) < 2:
            return False

        last_two = client_messages[-2:]
        return cls.is_caps_message(last_two[0]) and cls.is_caps_message(
            last_two[1]
        )

    @classmethod
    def analyze(
        cls,
        query: str,
        conversation_history: list[dict[str, Any]] | None = None,
    ) -> PrePassResult:
        """Выполняет детерминированное извлечение сущностей и триггеров."""
        # 1. Поиск шестнадцатеричных кодов ошибок
        error_codes = list(dict.fromkeys(cls.ERROR_CODE_REGEX.findall(query)))

        # 2. Определение правового режима
        has_44 = bool(cls.LAW_44_REGEX.search(query))
        has_223 = bool(cls.LAW_223_REGEX.search(query))
        has_portal = bool(cls.PORTAL_REGEX.search(query))

        regime_hint: Literal["44-FZ", "223-FZ", "MOS_PORTAL"] | None = None
        if has_44 and not has_223:
            regime_hint = "44-FZ"
        elif has_223 and not has_44:
            regime_hint = "223-FZ"
        elif has_portal:
            regime_hint = "MOS_PORTAL"

        # 3. Извлечение сущностей
        entities: list[EntityItem] = []
        for code in error_codes:
            entities.append(EntityItem(type="error_code", number=code))

        if has_44:
            entities.append(EntityItem(type="law", doc="44-FZ"))
        if has_223:
            entities.append(EntityItem(type="law", doc="223-FZ"))

        art_match = cls.ARTICLE_REGEX.search(query)
        if art_match:
            art_no = art_match.group(1)
            part_no: str | None = None
            part_match = cls.PART_REGEX.search(query)
            if part_match:
                part_no = part_match.group(1)
            entities.append(
                EntityItem(
                    type="article",
                    doc=regime_hint,
                    number=art_no,
                    part=part_no,
                )
            )

        # 4. Проверка триггеров P0
        caps_p0 = cls.check_consecutive_caps(query, conversation_history)
        threat_p0 = bool(cls.THREAT_REGEX.search(query))

        has_fin_word = bool(cls.DISPUTE_FINANCE_KEYWORD_REGEX.search(query))
        has_conflict_word = bool(
            cls.DISPUTE_CONFLICT_KEYWORD_REGEX.search(query)
        )
        financial_dispute_p0 = has_fin_word and has_conflict_word

        # 5. Проверка триггеров P1
        deadline_p1 = bool(cls.DEADLINE_REGEX.search(query))

        # 6. Проверка запроса эскалации
        escalation = bool(cls.ESCALATION_REGEX.search(query))

        return PrePassResult(
            error_codes=error_codes,
            entities=entities,
            regime_hint=regime_hint,
            caps_p0_triggered=caps_p0,
            threat_p0_triggered=threat_p0,
            financial_dispute_p0_triggered=financial_dispute_p0,
            deadline_p1_triggered=deadline_p1,
            escalation_requested=escalation,
        )


class QueryRouter:
    """Маршрутизатор входящих обращений и классификатор намерений."""

    SYSTEM_PROMPT = (
        "Ты — высокоточный модуль классификации и маршрутизации обращений Портала поставщиков Москвы.\n"
        "Твоя задача — классифицировать запрос пользователя, определить коммуникативное намерение,\n"
        "тему обращения, приоритет, целевую линию поддержки, тональность и сформировать нормализованный\n"
        "поисковый запрос (standalone_query) с разрешением анафоры и местоимений по контексту диалога.\n\n"
        "ТАКСОНОМИЯ ТЕМ (topic):\n"
        "- L2 (Техническая): digital_signature_plugin (ЭЦП, КриптоПро), technical_errors (системные сбои/коды ошибок),\n"
        "  api_integration (API, интеграции), browser_compatibility (браузеры, плагины).\n"
        "- L3 (Экспертная/Правовая): payment_delays (задержка выплат/возврата средств), account_blocking (блокировка ЛК),\n"
        "  complaints_fas (жалобы в ФАС/суд), contract_disputes (споры по контракту).\n"
        "- L1 (Первая линия): registration_portal (регистрация/ЕРУЗ), catalog_navigation (каталог СТЕ),\n"
        "  quote_sessions_rules (правила котировочных сессий), general_faq (общие вопросы/FAQ),\n"
        "  contract_conclusion (заключение контракта/протокол разногласий), closing_documents (акты/УПД),\n"
        "  tender_cancellation (отмена котировочной сессии), bid_security (обеспечение заявки),\n"
        "  contract_security (обеспечение контракта), delivery_acceptance (приемка товаров/работ),\n"
        "  electronic_store (закупки малого объема/оферты), other (прочее).\n\n"
        "ПРАВИЛА ИНТЕНТОВ (intent):\n"
        "- qa: стандартный информационный вопрос по процедурам или регламентам.\n"
        "- article_lookup: запрос прямого текста статьи закона или пункта регламента (содержит 'что написано в ст...', 'текст статьи').\n"
        "- procedural: пошаговые инструкции 'как сделать' (куда нажать, как прикрепить).\n"
        "- chitchat: приветствия, благодарности, этикет ('здравствуйте', 'спасибо').\n"
        "- out_of_domain: вопросы вне тематики закупок и портала.\n\n"
        "ПРАВИЛА НОРМАЛИЗАЦИИ ЗАПРОСА:\n"
        "- standalone_query: должен быть полным, автономным запросом, понятным без истории чата. Если пользователь пишет 'А в какие сроки?', раскрой его в 'Сроки заключения контракта на Портале поставщиков'. При смене темы (new_topic) прошлый контекст не добавляй.\n"
        "- sub_queries: список 1-3 поисковых подзапросов ТОЛЬКО для составных/многоаспектных вопросов. Для простых вопросов, chitchat и out_of_domain возвращай пустой список [].\n"
    )

    L2_TOPICS: ClassVar[set[str]] = {
        "digital_signature_plugin",
        "technical_errors",
        "api_integration",
        "browser_compatibility",
    }

    L3_TOPICS: ClassVar[set[str]] = {
        "payment_delays",
        "account_blocking",
        "complaints_fas",
        "contract_disputes",
    }

    def __init__(
        self,
        llm_client: LlmRouterClientProtocol | None = None,
        timeout: float = 1.5,
    ) -> None:
        """Инициализирует маршрутизатор клиентом языковой модели и лимитом времени."""
        self.llm_client = llm_client or MockLlmClient()
        self.timeout = timeout

    def _build_user_prompt(
        self,
        query: str,
        conversation_history: list[dict[str, Any]] | None,
        pre_pass: PrePassResult,
    ) -> str:
        """Формирует текстовый промпт для языковой модели."""
        history_lines: list[str] = []
        if conversation_history:
            recent = conversation_history[-6:]
            for item in recent:
                sender = str(
                    item.get("sender") or item.get("role") or "unknown"
                )
                text = str(item.get("text") or "").strip()
                history_lines.append(f"{sender}: {text}")

        history_block = (
            "\n".join(history_lines) if history_lines else "История пуста."
        )

        hints: list[str] = []
        if pre_pass.error_codes:
            hints.append(f"Коды ошибок: {', '.join(pre_pass.error_codes)}")
        if pre_pass.regime_hint:
            hints.append(f"Правовой режим: {pre_pass.regime_hint}")
        if pre_pass.deadline_p1_triggered:
            hints.append("Обнаружен маркер срочного дедлайна (<24ч)")
        if pre_pass.threat_p0_triggered:
            hints.append("Обнаружены маркеры претензии/ФАС/суда")
        if pre_pass.escalation_requested:
            hints.append("Пользователь запросил соединение с человеком")

        hints_str = (
            "; ".join(hints) if hints else "Нет специфических маркеров."
        )

        return (
            f"ИСТОРИЯ ДИАЛОГА (до 6 последних реплик):\n{history_block}\n\n"
            f"ПРЕДВАРИТЕЛЬНЫЕ МАРКЕРЫ: {hints_str}\n\n"
            f"ТЕКУЩЕЕ СООБЩЕНИЕ ПОЛЬЗОВАТЕЛЯ:\n{query}\n\n"
            "Верни строго валидный JSON по контракту QueryRouterOutput."
        )

    def _create_fallback_output(
        self,
        query: str,
        pre_pass: PrePassResult,
    ) -> QueryRouterOutput:
        """Формирует детерминированный обогащенный результат при таймауте или сбое LLM."""
        # 1. Определение приоритета
        if (
            pre_pass.caps_p0_triggered
            or pre_pass.threat_p0_triggered
            or pre_pass.financial_dispute_p0_triggered
        ):
            priority: Literal["P0", "P1", "P2"] = "P0"
        elif pre_pass.error_codes or pre_pass.deadline_p1_triggered:
            priority = "P1"
        else:
            priority = "P2"

        # 2. Определение линии и темы
        if pre_pass.threat_p0_triggered:
            support_line: Literal["L1", "L2", "L3"] = "L3"
            topic = "complaints_fas"
        elif pre_pass.financial_dispute_p0_triggered:
            support_line = "L3"
            topic = "payment_delays"
        elif pre_pass.error_codes:
            support_line = "L2"
            topic = "technical_errors"
        else:
            support_line = "L1"
            topic = "general_faq"

        # 3. Тональность
        sentiment: Literal["neutral", "frustrated", "aggressive"] = (
            "aggressive"
            if (pre_pass.caps_p0_triggered or pre_pass.threat_p0_triggered)
            else "neutral"
        )

        # 4. Интент
        has_article = any(e.type == "article" for e in pre_pass.entities)
        intent: Literal[
            "qa", "article_lookup", "procedural", "chitchat", "out_of_domain"
        ] = "qa"
        if has_article and re.search(
            r"\b(?:что\s+написано|текст|содержание|процитируй|покажи)\b",
            query,
            re.IGNORECASE,
        ):
            intent = "article_lookup"

        return QueryRouterOutput(
            intent=intent,
            regime_hint=pre_pass.regime_hint or "MOS_PORTAL",
            topic=topic,
            priority=priority,
            support_line=support_line,
            sentiment=sentiment,
            follow_up_type="none",
            error_codes=pre_pass.error_codes,
            escalation_requested=pre_pass.escalation_requested,
            entities=pre_pass.entities,
            standalone_query=query.strip(),
            sub_queries=[],
        )

    def _apply_post_arbitration(
        self,
        output: QueryRouterOutput,
        pre_pass: PrePassResult,
    ) -> QueryRouterOutput:
        """Детерминированно применяет жесткие инварианты поверх результата модели."""
        # 1. Объединение и инварианты кодов ошибок
        merged_codes = list(
            dict.fromkeys(pre_pass.error_codes + output.error_codes)
        )
        if merged_codes:
            output.error_codes = merged_codes
            if output.support_line == "L1":
                output.support_line = "L2"
            if output.priority == "P2":
                output.priority = "P1"

        # 2. Инварианты тем по матрице поддержки
        if output.topic in self.L2_TOPICS:
            if output.support_line == "L1":
                output.support_line = "L2"
        elif output.topic in self.L3_TOPICS:
            output.support_line = "L3"

        # 3. Инварианты приоритета P0
        if (
            pre_pass.caps_p0_triggered
            or pre_pass.threat_p0_triggered
            or pre_pass.financial_dispute_p0_triggered
            or output.sentiment == "aggressive"
        ):
            output.priority = "P0"
            if (
                pre_pass.threat_p0_triggered
                or pre_pass.financial_dispute_p0_triggered
            ):
                output.support_line = "L3"

        # 4. Инварианты приоритета P1 (если не P0)
        if output.priority != "P0" and (
            pre_pass.error_codes or pre_pass.deadline_p1_triggered
        ):
            output.priority = "P1"

        # 5. Запрос эскалации
        if pre_pass.escalation_requested:
            output.escalation_requested = True

        # 6. Слияние сущностей
        existing_entities = [
            (e.type, e.number, e.part, e.doc) for e in output.entities
        ]
        for pre_ent in pre_pass.entities:
            key = (pre_ent.type, pre_ent.number, pre_ent.part, pre_ent.doc)
            if key not in existing_entities:
                output.entities.append(pre_ent)
                existing_entities.append(key)

        # 7. Контроль подзапросов
        if output.intent in ("chitchat", "out_of_domain"):
            output.sub_queries = []
        elif len(output.sub_queries) > 3:
            output.sub_queries = output.sub_queries[:3]

        return output

    async def route(
        self,
        query: str,
        conversation_history: list[dict[str, Any]] | None = None,
    ) -> QueryRouterOutput:
        """Выполняет гибридный анализ и маршрутизацию входящего сообщения."""
        start_time = time.monotonic()

        # 1. Быстрый детерминированный Pre-pass
        pre_pass = DeterministicPrePass.analyze(query, conversation_history)

        # 2. Вызов LLM с контролем тайм-бюджета
        elapsed = time.monotonic() - start_time
        remaining = self.timeout - elapsed
        if remaining <= 0:
            return self._create_fallback_output(query, pre_pass)

        prompt = self._build_user_prompt(query, conversation_history, pre_pass)

        llm_result: QueryRouterOutput | None = None
        try:
            llm_result = await asyncio.wait_for(
                self.llm_client.generate_structured(
                    schema=QueryRouterOutput,
                    prompt=prompt,
                    system_prompt=self.SYSTEM_PROMPT,
                    timeout=remaining,
                ),
                timeout=remaining,
            )
        except TimeoutError:
            return self._create_fallback_output(query, pre_pass)
        except Exception:
            # Ошибка генерации или валидации: повторный запрос только если осталось > 0.6 с
            remaining_retry = self.timeout - (time.monotonic() - start_time)
            if remaining_retry > 0.6:
                try:
                    llm_result = await asyncio.wait_for(
                        self.llm_client.generate_structured(
                            schema=QueryRouterOutput,
                            prompt=prompt,
                            system_prompt=self.SYSTEM_PROMPT,
                            timeout=remaining_retry,
                        ),
                        timeout=remaining_retry,
                    )
                except Exception:
                    return self._create_fallback_output(query, pre_pass)
            else:
                return self._create_fallback_output(query, pre_pass)

        if llm_result is None:
            return self._create_fallback_output(query, pre_pass)

        # 3. Детерминированный Post-арбитраж инвариантов
        return self._apply_post_arbitration(llm_result, pre_pass)
