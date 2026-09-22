"""Генерация сводки диалога и подсказок оператору (Copilot)."""

import asyncio
import logging
from typing import Any, ClassVar, Literal, Protocol, runtime_checkable
from uuid import UUID

from qdrant_client import AsyncQdrantClient

from src.core.config import settings
from src.core.qdrant_client import get_qdrant_client
from src.operators.schemas import (
    CopilotSummaryResponseSchema,
    SimilarTicketItemSchema,
)
from src.rag.qdrant_tickets import search_similar_resolved_tickets
from src.rag.schemas import CopilotLlmOutputSchema

logger = logging.getLogger(__name__)

COPILOT_SYSTEM_PROMPT = (
    "Ты — экспертный аналитический ассистент AI Copilot для операторов службы поддержки "
    "Портала поставщиков Москвы (ЕАИСТ) и госзакупок по 44-ФЗ и 223-ФЗ.\n\n"
    "Твоя задача — проанализировать историю диалога клиента и вернуть строго валидный JSON:\n"
    "{\n"
    '  "summary": "Краткая суть проблемы клиента (1-2 предложения, строго факты)",\n'
    '  "suggested_line_code": "L1", // одно из: "L1" (регламенты), "L2" (сбои ЭЦП/КриптоПро), "L3" (ФАС/споры/блокировки)\n'
    '  "suggested_response": "Готовый вежливый черновик ответа для оператора с пошаговой инструкцией",\n'
    '  "recommended_chunk_ids": [] // список ID статей регламентов или пустой список\n'
    "}\n\n"
    "Соблюдай деловой тон и точность ссылок на процедуры госзакупок."
)


@runtime_checkable
class CopilotLlmClientProtocol(Protocol):
    """Протокол вызова языковой модели для генерации сводки Copilot."""

    async def generate_copilot_summary(
        self,
        prompt: str,
        system_prompt: str,
        timeout: float = 30.0,
    ) -> CopilotLlmOutputSchema:
        """Генерирует структурированную аналитическую выжимку и черновик решения."""
        ...


class MockCopilotLlmClient:
    """Детерминированный тестовый клиент генерации подсказок Copilot."""

    TECH_MARKERS: ClassVar[set[str]] = {
        "0x",
        "криптопро",
        "эцп",
        "cades",
        "рутокен",
        "плагин",
        "сертификат",
        "cadesplugin",
        "csp",
    }

    DISPUTE_MARKERS: ClassVar[set[str]] = {
        "фас",
        "суд",
        "судебн",
        "исков",
        "жалоб",
        "претензи",
        "расторг",
        "блокировк",
        "рнп",
        "уклон",
    }

    def __init__(
        self,
        default_output: CopilotLlmOutputSchema | None = None,
        delay: float = 0.0,
        fail_times: int = 0,
    ) -> None:
        """Инициализирует тестовый генератор подсказок."""
        self.default_output = default_output
        self.delay = delay
        self.fail_times = fail_times
        self.call_count = 0

    async def generate_copilot_summary(
        self,
        prompt: str,
        system_prompt: str,
        timeout: float = 30.0,
    ) -> CopilotLlmOutputSchema:
        """Эмулирует структурированный ответ языковой модели с контекстной эвристикой."""
        self.call_count += 1
        if self.delay > 0:
            await asyncio.sleep(self.delay)

        if self.call_count <= self.fail_times:
            raise TimeoutError("LLM copilot generation timeout (mock failure)")

        if self.default_output is not None:
            return self.default_output.model_copy(deep=True)

        lowered = prompt.lower()

        # 1. Проверка технических маркеров ЭЦП и плагинов -> Линия L2
        if any(marker in lowered for marker in self.TECH_MARKERS):
            return CopilotLlmOutputSchema(
                summary=(
                    "Пользователь столкнулся со сбоем плагина ЭЦП / КриптоПро CSP при попытке "
                    "подписания документа на Портале поставщиков."
                ),
                suggested_line_code="L2",
                suggested_response=(
                    "Здравствуйте! По вашей проблеме с электронной подписью рекомендуем: "
                    "1) Проверить видимость личного сертификата в панели КриптоПро CSP; "
                    "2) Переустановить КриптоПро ЭЦП Browser plug-in; "
                    "3) Добавить сайт https://zakupki.mos.ru в список доверенных узлов браузера."
                ),
                recommended_chunk_ids=[
                    "chunk_portal_zakupki_reglament_sec4_p1"
                ],
            )

        # 2. Проверка маркеров споров, ФАС и блокировок -> Линия L3
        if any(marker in lowered for marker in self.DISPUTE_MARKERS):
            return CopilotLlmOutputSchema(
                summary=(
                    "Обращение связано с претензионной работой, угрозой жалобы в ФАС "
                    "или блокировкой учетной записи поставщика."
                ),
                suggested_line_code="L3",
                suggested_response=(
                    "Здравствуйте! Ваше обращение принято в работу старшим специалистом. "
                    "Рекомендуем зафиксировать скриншоты системного времени и направить официальное "
                    "уведомление заказчику о возникших технических обстоятельствах."
                ),
                recommended_chunk_ids=[],
            )

        # 3. Регламентные вопросы (котировочные сессии, протоколы разногласий) -> Линия L1
        return CopilotLlmOutputSchema(
            summary=(
                "Вопрос клиента: регламентные сроки, протокол разногласий "
                "по котировочной сессии на Портале поставщиков."
            ),
            suggested_line_code="L1",
            suggested_response=(
                "Добрый день! В соответствии с разделом 4 Регламента Портала поставщиков Москвы, "
                "вы вправе сформировать и направить протокол разногласий в личном кабинете "
                "в течение 3 рабочих дней с момента размещения проекта контракта заказчиком."
            ),
            recommended_chunk_ids=["chunk_portal_zakupki_reglament_sec4_p1"],
        )


class OllamaCopilotLlmClient:
    """Боевой клиент генерации подсказок Copilot через Ollama со страховочным резервом."""

    DEFAULT_TIMEOUT: float = 60.0

    def __init__(
        self,
        llm_client: Any | None = None,
        fallback_client: CopilotLlmClientProtocol | None = None,
        timeout: float | None = None,
    ) -> None:
        """Инициализирует клиент инференса Ollama и резервную заглушку."""
        from src.core.llm_client import get_llm_stream_client

        self.llm_client = llm_client or get_llm_stream_client()
        self.fallback_client = fallback_client or MockCopilotLlmClient()
        self.timeout = (
            timeout
            if timeout is not None
            else getattr(
                settings, "OLLAMA_TIMEOUT_SECONDS", self.DEFAULT_TIMEOUT
            )
        )

    @staticmethod
    def _normalize_line_code(value: Any) -> Literal["L1", "L2", "L3"]:
        """Приводит код линии поддержки к допустимому значению L1/L2/L3."""
        val_str = str(value or "").strip().upper()
        if val_str in ("L1", "L2", "L3"):
            return val_str  # type: ignore[return-value]
        if "2" in val_str:
            return "L2"
        if "3" in val_str:
            return "L3"
        return "L1"

    async def generate_copilot_summary(
        self,
        prompt: str,
        system_prompt: str = COPILOT_SYSTEM_PROMPT,
        timeout: float | None = None,
    ) -> CopilotLlmOutputSchema:
        """Генерирует сводку диалога и черновик через Ollama с автопереходом на заглушку."""
        request_timeout = timeout if timeout is not None else self.timeout
        try:
            raw_dict = await self.llm_client.generate_json(
                prompt=prompt,
                system_prompt=system_prompt,
                timeout=request_timeout,
            )

            summary = str(raw_dict.get("summary") or "").strip()
            if not summary:
                summary = "Клиент обратился за консультацией по работе на Портале поставщиков."

            line_code = self._normalize_line_code(
                raw_dict.get("suggested_line_code")
            )

            suggested_response = str(
                raw_dict.get("suggested_response") or ""
            ).strip()
            if not suggested_response:
                suggested_response = (
                    "Здравствуйте! По вашему вопросу рекомендуем проверить данные в личном кабинете "
                    "и следовать установленным регламентам Портала поставщиков."
                )

            chunk_ids_raw = raw_dict.get("recommended_chunk_ids")
            recommended_chunk_ids: list[str] = []
            if isinstance(chunk_ids_raw, list):
                recommended_chunk_ids = [
                    str(item) for item in chunk_ids_raw if item
                ]

            return CopilotLlmOutputSchema(
                summary=summary,
                suggested_line_code=line_code,
                suggested_response=suggested_response,
                recommended_chunk_ids=recommended_chunk_ids,
            )
        except Exception as exc:
            logger.warning(
                "Сбой вызова Ollama при генерации подсказки Copilot (%s). Активирован страховочный резерв",
                exc,
            )
            return await self.fallback_client.generate_copilot_summary(
                prompt=prompt,
                system_prompt=system_prompt,
                timeout=request_timeout,
            )


class CopilotService:
    """Сервис формирования аналитической подсказки оператора и подбора прецедентов."""

    DEFAULT_TIMEOUT: float = 60.0

    def __init__(
        self,
        llm_client: CopilotLlmClientProtocol | None = None,
        qdrant_client: AsyncQdrantClient | None = None,
        timeout: float | None = None,
    ) -> None:
        """Инициализирует сервис клиентом LLM и клиентом Qdrant."""
        self.timeout = (
            timeout
            if timeout is not None
            else getattr(
                settings, "OLLAMA_TIMEOUT_SECONDS", self.DEFAULT_TIMEOUT
            )
        )
        self.fallback_client = MockCopilotLlmClient()
        self.llm_client = llm_client or OllamaCopilotLlmClient(
            timeout=self.timeout
        )
        self.qdrant_client = qdrant_client

    def _format_conversation(self, messages: list[dict[str, Any]]) -> str:
        """Форматирует переписку в компактный диалоговый контекст для промпта."""
        lines: list[str] = []
        for msg in messages:
            sender = str(
                msg.get("sender") or msg.get("sender_type") or "client"
            )
            text = str(msg.get("text") or "").strip()
            if text:
                lines.append(f"{sender}: {text}")

        return "\n".join(lines) if lines else "История диалога пуста."

    def _extract_last_user_query(self, messages: list[dict[str, Any]]) -> str:
        """Извлекает последнюю реплику клиента для семантического поиска в базе прецедентов."""
        for msg in reversed(messages):
            sender = str(msg.get("sender") or msg.get("sender_type") or "")
            if sender in ("client", "user"):
                text = str(msg.get("text") or "").strip()
                if text:
                    return text

        # Фолбэк: если реплик клиента не найдено, берем последнее непустое сообщение
        for msg in reversed(messages):
            text = str(msg.get("text") or "").strip()
            if text:
                return text

        return ""

    async def build_copilot_summary(
        self,
        ticket_id: UUID | str,
        messages: list[dict[str, Any]],
        qdrant_client: AsyncQdrantClient | None = None,
    ) -> CopilotSummaryResponseSchema:
        """Формирует выжимку диалога, черновик ответа и находит похожие прецеденты в Qdrant."""
        target_qdrant = (
            qdrant_client or self.qdrant_client or get_qdrant_client()
        )
        conversation_context = self._format_conversation(messages)
        user_query = self._extract_last_user_query(messages)

        # 1. Поиск похожих закрытых кейсов в Qdrant
        similar_tickets: list[SimilarTicketItemSchema] = []
        if target_qdrant is not None and user_query:
            similar_tickets = await search_similar_resolved_tickets(
                client=target_qdrant,
                query_text=user_query,
                limit=3,
            )

        # 2. Генерация аналитической выжимки через LLM с таймаутом до 4.0с
        prompt = (
            f"ИСТОРИЯ ОБРАЩЕНИЯ (ТИКЕТ {ticket_id}):\n"
            f"{conversation_context}\n\n"
            "Сформируй краткую суть, выбери линию поддержки и составь черновик ответа."
        )

        try:
            llm_output = await asyncio.wait_for(
                self.llm_client.generate_copilot_summary(
                    prompt=prompt,
                    system_prompt=COPILOT_SYSTEM_PROMPT,
                    timeout=self.timeout,
                ),
                timeout=self.timeout,
            )
        except Exception as exc:
            logger.warning(
                "Copilot LLM execution error/timeout: %s. Using heuristic fallback.",
                exc,
            )
            llm_output = await self.fallback_client.generate_copilot_summary(
                prompt=prompt,
                system_prompt=COPILOT_SYSTEM_PROMPT,
                timeout=self.timeout,
            )

        # 3. Агрегация в публичную контрактную схему
        return CopilotSummaryResponseSchema(
            summary=llm_output.summary,
            suggested_line_code=llm_output.suggested_line_code,
            suggested_response=llm_output.suggested_response,
            recommended_chunk_ids=llm_output.recommended_chunk_ids,
            similar_resolved_tickets=similar_tickets,
        )
