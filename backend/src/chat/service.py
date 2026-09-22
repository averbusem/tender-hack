import json
import logging
from collections.abc import AsyncGenerator
from datetime import datetime
from typing import Any
from uuid import UUID

import uuid6
from fastapi import HTTPException, Request, status
from sqlalchemy.ext.asyncio import AsyncSession

from src.auth.models import UserModel
from src.chat.models import (
    TERMINAL_TICKET_STATUSES,
    ChatModel,
    MessageModel,
    MessageModerationStatus,
    MessageSenderType,
    MessageSourceModel,
    TicketModel,
    TicketPriority,
    TicketStatus,
)
from src.chat.moderation import (
    ProfanityModerator,
    get_moderator,
)
from src.chat.repository import ChatRepository, TicketRepository
from src.chat.router import EscalationRouter
from src.chat.schemas import (
    ActiveTicketSummarySchema,
    CancelTicketResponseSchema,
    ChatStateResponseSchema,
    ClientResolveTicketResponseSchema,
    ClientSendMessageRequestSchema,
    EscalateRequestSchema,
    MessageResponseSchema,
)
from src.core.config import settings
from src.core.redis_client import (
    RedisChatContext,
    RedisLineQueue,
    RedisTicketEvents,
)
from src.rag.router import QueryRouter
from src.rag.schemas import (
    RagDegradedModeEventSchema,
    RagDoneEventSchema,
    RagQueryRequestSchema,
    RagSentenceEventSchema,
    RagSourceChunkSchema,
    RagSourcesEventSchema,
)
from src.rag.service import RagService

logger = logging.getLogger(__name__)


class ChatService:
    """Сервис управления перепиской клиентов и оркестрации диалога."""

    def __init__(
        self,
        repo: ChatRepository,
        session: AsyncSession,
        rag_service: RagService,
        ticket_repo: TicketRepository | None = None,
        redis_context: RedisChatContext | None = None,
        moderator: ProfanityModerator | None = None,
        ticket_events: RedisTicketEvents | None = None,
        line_queue: RedisLineQueue | None = None,
        query_router: QueryRouter | None = None,
        escalation_router: EscalationRouter | None = None,
    ) -> None:
        """Инициализирует сервис диалогов репозиториями, сессией БД, поисковым ядром и Redis."""
        self.repo = repo
        self.session = session
        self.rag_service = rag_service
        self.ticket_repo = ticket_repo or TicketRepository(session)
        self.redis_context = redis_context
        self.moderator = moderator or get_moderator()
        self.ticket_events = ticket_events
        self.line_queue = line_queue
        self.query_router = query_router or QueryRouter()
        self.escalation_router = escalation_router or EscalationRouter()

    async def get_chat_state(self, user: UserModel) -> ChatStateResponseSchema:
        """Возвращает текущее состояние переписки и историю сообщений клиента.

        Метод находит постоянный чат клиента (или создает его при первом обращении),
        загружает активное обращение с назначенным оператором и линией поддержки,
        извлекает до 50 последних реплик ленты в хронологическом порядке и вычисляет
        флаги доступных клиенту действий.
        """
        chat = await self.repo.get_by_client_id(user.id)
        if chat is None:
            chat = ChatModel(client_id=user.id)
            await self.repo.create(chat)

        active_ticket = await self.ticket_repo.get_active_by_chat_id(chat.id)

        active_summary: ActiveTicketSummarySchema | None = None
        if active_ticket is not None:
            operator_name: str | None = None
            if active_ticket.assigned_operator is not None:
                operator_name = (
                    active_ticket.assigned_operator.full_name
                    or active_ticket.assigned_operator.email
                )
            line_code = (
                active_ticket.line.code
                if active_ticket.line is not None
                else None
            )
            active_summary = ActiveTicketSummarySchema(
                id=active_ticket.id,
                priority=active_ticket.priority,
                status=active_ticket.status,
                line_code=line_code,
                assigned_operator_name=operator_name,
                created_at=active_ticket.created_at,
            )

        recent_messages = await self.repo.get_recent_messages(
            chat.id, limit=50
        )
        messages_dto = [
            MessageResponseSchema.model_validate(msg)
            for msg in recent_messages
        ]

        can_escalate = bool(
            active_ticket
            and active_ticket.status == TicketStatus.BOT_PROCESSING
        )
        can_cancel = bool(
            active_ticket and active_ticket.status == TicketStatus.QUEUED
        )

        can_feedback = False
        feedback_ticket_id: UUID | None = None
        if active_ticket is None:
            last_closed = await self.ticket_repo.get_last_closed_by_chat_id(
                chat.id
            )
            if (
                last_closed is not None
                and last_closed.status == TicketStatus.RESOLVED
            ):
                can_feedback = True
                feedback_ticket_id = last_closed.id

        return ChatStateResponseSchema(
            chat_id=chat.id,
            active_ticket=active_summary,
            messages=messages_dto,
            can_escalate=can_escalate,
            can_cancel=can_cancel,
            can_feedback=can_feedback,
            feedback_ticket_id=feedback_ticket_id,
        )

    async def process_client_message(
        self,
        payload: ClientSendMessageRequestSchema,
        user: UserModel | None = None,
        client_id: UUID | None = None,
        accept_header: str | None = None,
    ) -> AsyncGenerator[str, None]:
        """Обрабатывает входящую реплику клиента и возвращает поток Server-Sent Events.

        1. Находит или создает постоянный чат клиента.
        2. Проверяет входящую реплику модератором ненормативной лексики.
        3. При обнаружении мата переводит обращение в closed_by_moderation, сохраняет реплику со статусом blocked,
           очищает оперативный контекст Redis и отдает SSE session_terminated либо HTTP 400.
        4. Если проверка пройдена, сохраняет реплику со статусом passed в messages и оперативный контекст Redis.
        5. Запускает генератор RagService.generate_answer.
        6. Отдает поток SSE со статусами, источниками, предложениями и финальным событием done.
        7. По завершении генерации фиксирует ответ бота и источники в PostgreSQL и Redis.
        """
        user_id = user.id if user is not None else client_id
        if user_id is None:
            raise ValueError(
                "Для обработки сообщения требуется передать пользователя"
            )

        chat = await self.repo.get_by_client_id(user_id)
        if chat is None:
            chat = ChatModel(client_id=user_id)
            await self.repo.create(chat)

        if payload.new_ticket:
            prev_active = await self.ticket_repo.get_active_by_chat_id(chat.id)
            if (
                prev_active
                and prev_active.status == TicketStatus.BOT_PROCESSING
            ):
                prev_active.status = TicketStatus.RESOLVED
                prev_active.closed_at = datetime.now(settings.TIMEZONE)
                await self.ticket_repo.update(prev_active)
            active_ticket = None
        elif payload.ticket_id:
            active_ticket = await self.ticket_repo.get_by_id(payload.ticket_id)
            if active_ticket and active_ticket.chat_id != chat.id:
                active_ticket = None
        else:
            active_ticket = await self.ticket_repo.get_active_by_chat_id(
                chat.id
            )

        # Если обращение уже завершено (например, закрыто модерацией), запрещаем продолжать переписку
        if (
            active_ticket is not None
            and active_ticket.status in TERMINAL_TICKET_STATUSES
        ):
            if active_ticket.status == TicketStatus.CLOSED_BY_MODERATION:
                detail_msg = (
                    "Ваше обращение закрыто в связи с нарушением правил общения "
                    "(использование нецензурной лексики). Пожалуйста, сформируйте "
                    "новое обращение в корректной форме."
                )
                reason = "profanity"
            else:
                detail_msg = "Обращение уже завершено. Пожалуйста, начните новый диалог."
                reason = "closed"

            if accept_header and "text/event-stream" in accept_header:
                event_data = json.dumps(
                    {
                        "reason": reason,
                        "message": detail_msg,
                    },
                    ensure_ascii=False,
                )
                yield f"event: session_terminated\ndata: {event_data}\n\n"
                return

            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=detail_msg,
            )

        # 1. Проверка модератором обсценной лексики
        moderation_result = self.moderator.check_profanity(payload.text)
        if moderation_result.is_profane:
            if active_ticket is None:
                active_ticket = TicketModel(
                    chat_id=chat.id,
                    status=TicketStatus.CLOSED_BY_MODERATION,
                    priority=TicketPriority.P2,
                    closed_at=datetime.now(settings.TIMEZONE),
                )
                await self.ticket_repo.create(active_ticket)
            else:
                active_ticket.status = TicketStatus.CLOSED_BY_MODERATION
                active_ticket.closed_at = datetime.now(settings.TIMEZONE)
                await self.ticket_repo.update(active_ticket)

            client_message = MessageModel(
                id=uuid6.uuid7(),
                ticket_id=active_ticket.id,
                sender_type=MessageSenderType.CLIENT,
                sender_id=user_id,
                text=payload.text,
                moderation_status=MessageModerationStatus.BLOCKED,
                moderation_reason=moderation_result.reason or "profanity",
                created_at=datetime.now(settings.TIMEZONE),
            )
            await self.repo.save_message(client_message)
            await self.session.commit()

            # Очищаем оперативный контекст диалога в Redis
            if self.redis_context is not None:
                await self.redis_context.clear_context(active_ticket.id)

            termination_msg = (
                "Ваше обращение завершено в связи с нарушением правил общения "
                "(использование нецензурной лексики). Пожалуйста, сформируйте "
                "новое обращение в корректной форме."
            )

            if self.ticket_events is not None:
                await self.ticket_events.publish_session_terminated(
                    ticket_id=active_ticket.id,
                    reason="profanity",
                    message=termination_msg,
                )
                await self.ticket_events.publish_ticket_resolved(
                    ticket_id=active_ticket.id,
                    operator_id=active_ticket.assigned_operator_id,
                    closed_at=active_ticket.closed_at,
                )

            if (
                active_ticket.line is not None
                and active_ticket.assigned_operator_id is not None
            ):
                await self._safe_dispatch_task(
                    active_ticket.line.code, "slot_freed"
                )

            await self._safe_enqueue_audit(
                active_ticket.id, "closed_by_moderation"
            )

            if accept_header and "text/event-stream" in accept_header:
                event_data = json.dumps(
                    {
                        "reason": "profanity",
                        "message": termination_msg,
                    },
                    ensure_ascii=False,
                )
                yield f"event: session_terminated\ndata: {event_data}\n\n"
                return

            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=termination_msg,
            )

        if active_ticket is None:
            active_ticket = TicketModel(
                chat_id=chat.id,
                status=TicketStatus.BOT_PROCESSING,
                priority=TicketPriority.P2,
            )
            await self.ticket_repo.create(active_ticket)

        client_message = MessageModel(
            id=uuid6.uuid7(),
            ticket_id=active_ticket.id,
            sender_type=MessageSenderType.CLIENT,
            sender_id=user_id,
            text=payload.text,
            moderation_status=MessageModerationStatus.PASSED,
            created_at=datetime.now(settings.TIMEZONE),
        )
        await self.repo.save_message(client_message)
        await self.session.commit()

        if self.redis_context:
            await self.redis_context.add_message(
                ticket_id=active_ticket.id,
                sender=MessageSenderType.CLIENT.value,
                text=payload.text,
                timestamp=client_message.created_at,
            )

        # 1. Сбор контекста предыдущих реплик диалога (до 6 последних реплик)
        conversation_history: list[dict[str, str]] = []
        if self.redis_context:
            try:
                cached_msgs = await self.redis_context.get_messages(
                    active_ticket.id
                )
                if cached_msgs:
                    for m in cached_msgs[:-1][-6:]:
                        sender_role = (
                            "user"
                            if m.get("sender")
                            == MessageSenderType.CLIENT.value
                            else "assistant"
                        )
                        conversation_history.append(
                            {"role": sender_role, "text": m.get("text", "")}
                        )
            except Exception as exc:
                logger.warning(
                    "Не удалось получить контекст диалога из Redis: %s", exc
                )

        if not conversation_history:
            ticket_db_msgs = await self.repo.get_messages_by_ticket_id(
                active_ticket.id
            )
            for m in ticket_db_msgs:
                if m.id == client_message.id:
                    continue
                sender_role = (
                    "user"
                    if m.sender_type == MessageSenderType.CLIENT
                    else "assistant"
                )
                conversation_history.append(
                    {"role": sender_role, "text": m.text}
                )
            conversation_history = conversation_history[-6:]

        # 2. Анализ и классификация сообщения через QueryRouter
        route_output = await self.query_router.route(
            payload.text, conversation_history=conversation_history
        )

        # 3. Обновление приоритета и линии тикета в БД
        priority_map = {
            "P0": TicketPriority.P0,
            "P1": TicketPriority.P1,
            "P2": TicketPriority.P2,
        }
        if route_output.priority in priority_map:
            active_ticket.priority = priority_map[route_output.priority]

        # При наличии кодов ошибок (0x...) повышаем до P0 по регламенту отказоустойчивости
        if route_output.error_codes:
            active_ticket.priority = TicketPriority.P0

        await self.ticket_repo.update(active_ticket)
        await self.session.commit()

        # 4. Перехват non-RAG намерений (escalation_requested, chitchat, out_of_domain)
        if route_output.escalation_requested:
            escalation_text = (
                "Переключаю диалог на специалиста службы поддержки. "
                "Пожалуйста, оставайтесь на связи, первый освободившийся оператор сейчас подключится к чату."
            )
            bot_message_id = uuid6.uuid7()
            bot_message = MessageModel(
                id=bot_message_id,
                ticket_id=active_ticket.id,
                sender_type=MessageSenderType.BOT,
                sender_id=None,
                text=escalation_text,
                moderation_status=MessageModerationStatus.PASSED,
                created_at=datetime.now(settings.TIMEZONE),
            )
            await self.repo.save_message(bot_message)

            active_ticket.status = TicketStatus.QUEUED.value
            active_ticket.escalation_reason = "client_requested"
            active_ticket.opened_at = datetime.now(settings.TIMEZONE)
            await self.ticket_repo.update(active_ticket)
            await self.session.commit()

            line_code = "L1"
            if active_ticket.line:
                line_code = active_ticket.line.code
            elif route_output.support_line:
                line_code = route_output.support_line

            if self.line_queue is not None:
                await self.line_queue.enqueue_ticket(
                    line_code=line_code,
                    ticket_id=active_ticket.id,
                    priority=active_ticket.priority,
                )

            await self._safe_dispatch_task(line_code, "ticket_escalated")
            await self._safe_copilot_task(active_ticket.id)

            if self.redis_context:
                await self.redis_context.add_message(
                    ticket_id=active_ticket.id,
                    sender=MessageSenderType.BOT.value,
                    text=escalation_text,
                    timestamp=bot_message.created_at,
                )

            sent_event = RagSentenceEventSchema(
                sentence_idx=0,
                text=escalation_text,
                verified=True,
            )
            yield f"event: {sent_event.event}\ndata: {sent_event.model_dump_json(exclude={'event'})}\n\n"

            done_event = RagDoneEventSchema(
                message_id=bot_message_id,
                text=escalation_text,
                all_verified=True,
                ticket_id=active_ticket.id,
            )
            yield f"event: {done_event.event}\ndata: {done_event.model_dump_json(exclude={'event'})}\n\n"
            return

        if route_output.intent == "chitchat":
            chitchat_text = (
                "Здравствуйте! Я виртуальный ассистент службы поддержки Портала поставщиков Москвы. "
                "Чем я могу помочь вам по регламенту, офертам или котировочным сессиям?"
            )
            bot_message_id = uuid6.uuid7()
            bot_message = MessageModel(
                id=bot_message_id,
                ticket_id=active_ticket.id,
                sender_type=MessageSenderType.BOT,
                sender_id=None,
                text=chitchat_text,
                moderation_status=MessageModerationStatus.PASSED,
                created_at=datetime.now(settings.TIMEZONE),
            )

            await self.repo.save_message(bot_message)
            await self.session.commit()

            if self.redis_context:
                await self.redis_context.add_message(
                    ticket_id=active_ticket.id,
                    sender=MessageSenderType.BOT.value,
                    text=chitchat_text,
                    timestamp=bot_message.created_at,
                )

            sent_event = RagSentenceEventSchema(
                sentence_idx=0,
                text=chitchat_text,
                verified=True,
            )
            yield f"event: {sent_event.event}\ndata: {sent_event.model_dump_json(exclude={'event'})}\n\n"

            done_event = RagDoneEventSchema(
                message_id=bot_message_id,
                text=chitchat_text,
                all_verified=True,
                ticket_id=active_ticket.id,
            )
            yield f"event: {done_event.event}\ndata: {done_event.model_dump_json(exclude={'event'})}\n\n"
            return

        if route_output.intent == "out_of_domain":
            refusal_text = (
                "Я специализированный консультант по закупкам и регламентам Портала поставщиков Москвы. "
                "Я не могу отвечать на вопросы на отвлеченные темы. "
                "Пожалуйста, задайте вопрос по регламенту, участию в закупках или работе личного кабинета."
            )
            bot_message_id = uuid6.uuid7()
            bot_message = MessageModel(
                id=bot_message_id,
                ticket_id=active_ticket.id,
                sender_type=MessageSenderType.BOT,
                sender_id=None,
                text=refusal_text,
                moderation_status=MessageModerationStatus.PASSED,
                created_at=datetime.now(settings.TIMEZONE),
            )
            await self.repo.save_message(bot_message)
            await self.session.commit()

            if self.redis_context:
                await self.redis_context.add_message(
                    ticket_id=active_ticket.id,
                    sender=MessageSenderType.BOT.value,
                    text=refusal_text,
                    timestamp=bot_message.created_at,
                )

            sent_event = RagSentenceEventSchema(
                sentence_idx=0,
                text=refusal_text,
                verified=True,
            )
            yield f"event: {sent_event.event}\ndata: {sent_event.model_dump_json(exclude={'event'})}\n\n"

            done_event = RagDoneEventSchema(
                message_id=bot_message_id,
                text=refusal_text,
                all_verified=True,
                ticket_id=active_ticket.id,
            )
            yield f"event: {done_event.event}\ndata: {done_event.model_dump_json(exclude={'event'})}\n\n"
            return

        # 5. Полноценный запуск RAG с контекстом диалога
        effective_query = payload.text
        if (
            getattr(route_output, "standalone_query", None)
            and route_output.standalone_query.strip()
            and not route_output.standalone_query.startswith("ИСТОРИЯ ДИАЛОГА")
        ):
            effective_query = route_output.standalone_query.strip()

        if len(effective_query) > 4000:
            effective_query = effective_query[:4000]

        bot_message_id = uuid6.uuid7()
        collected_sources: list[RagSourceChunkSchema] = []

        try:
            rag_request = RagQueryRequestSchema(
                query=effective_query,
                message_id=bot_message_id,
                conversation_history=conversation_history,
            )
            async for event in self.rag_service.generate_answer(rag_request):
                if isinstance(event, RagSourcesEventSchema):
                    collected_sources.extend(event.sources)
                elif isinstance(event, RagDoneEventSchema):
                    target_bot_id = event.message_id or bot_message_id
                    bot_message = MessageModel(
                        id=target_bot_id,
                        ticket_id=active_ticket.id,
                        sender_type=MessageSenderType.BOT,
                        sender_id=None,
                        text=event.text,
                        moderation_status=MessageModerationStatus.PASSED,
                        created_at=datetime.now(settings.TIMEZONE),
                    )
                    source_models = [
                        MessageSourceModel(
                            id=uuid6.uuid7(),
                            message_id=target_bot_id,
                            chunk_id=src.chunk_id,
                            doc_id=src.doc_id,
                            quote_text=src.quote_text,
                            created_at=datetime.now(settings.TIMEZONE),
                        )
                        for src in collected_sources
                    ]
                    await self.repo.save_message(
                        bot_message, sources=source_models
                    )
                    await self.session.commit()

                    if self.redis_context:
                        await self.redis_context.add_message(
                            ticket_id=active_ticket.id,
                            sender=MessageSenderType.BOT.value,
                            text=event.text,
                            timestamp=bot_message.created_at,
                        )
                elif isinstance(event, RagDegradedModeEventSchema):
                    target_bot_id = bot_message_id
                    bot_message = MessageModel(
                        id=target_bot_id,
                        ticket_id=active_ticket.id,
                        sender_type=MessageSenderType.BOT,
                        sender_id=None,
                        text=event.message,
                        moderation_status=MessageModerationStatus.PASSED,
                        created_at=datetime.now(settings.TIMEZONE),
                    )
                    source_models = [
                        MessageSourceModel(
                            id=uuid6.uuid7(),
                            message_id=target_bot_id,
                            chunk_id=src.chunk_id,
                            doc_id=src.doc_id,
                            quote_text=src.quote_text,
                            created_at=datetime.now(settings.TIMEZONE),
                        )
                        for src in (event.sources or collected_sources)
                    ]
                    await self.repo.save_message(
                        bot_message, sources=source_models
                    )
                    await self.session.commit()

                    if self.redis_context:
                        await self.redis_context.add_message(
                            ticket_id=active_ticket.id,
                            sender=MessageSenderType.BOT.value,
                            text=event.message,
                            timestamp=bot_message.created_at,
                        )
                    event.ticket_id = active_ticket.id
                    event.message_id = target_bot_id

                if isinstance(event, RagDoneEventSchema):
                    event.ticket_id = active_ticket.id
                data_json = event.model_dump_json(exclude={"event"})
                yield f"event: {event.event}\ndata: {data_json}\n\n"
        except Exception as exc:
            logger.warning(
                "Критический сбой конвейера RAG, переход в режим деградации: %s",
                exc,
            )
            fallback_text = (
                "Генеративная модель временно недоступна. "
                "Ниже представлены найденные нормативные регламенты."
            )
            degraded_event = RagDegradedModeEventSchema(
                message=fallback_text,
                sources=collected_sources,
                ticket_id=active_ticket.id,
                message_id=bot_message_id,
            )
            bot_message = MessageModel(
                id=bot_message_id,
                ticket_id=active_ticket.id,
                sender_type=MessageSenderType.BOT,
                sender_id=None,
                text=fallback_text,
                moderation_status=MessageModerationStatus.PASSED,
                created_at=datetime.now(settings.TIMEZONE),
            )
            source_models = [
                MessageSourceModel(
                    id=uuid6.uuid7(),
                    message_id=bot_message_id,
                    chunk_id=src.chunk_id,
                    doc_id=src.doc_id,
                    quote_text=src.quote_text,
                    created_at=datetime.now(settings.TIMEZONE),
                )
                for src in collected_sources
            ]
            await self.repo.save_message(bot_message, sources=source_models)
            await self.session.commit()

            if self.redis_context:
                await self.redis_context.add_message(
                    ticket_id=active_ticket.id,
                    sender=MessageSenderType.BOT.value,
                    text=fallback_text,
                    timestamp=bot_message.created_at,
                )

            data_json = degraded_event.model_dump_json(exclude={"event"})
            yield f"event: {degraded_event.event}\ndata: {data_json}\n\n"

    async def send_operator_message(
        self,
        ticket_id: UUID,
        operator: UserModel,
        text: str,
    ) -> MessageModel:
        """Отправляет ответ оператора в чат с валидацией на ненормативную лексику.

        Raises:
            ProfanityValidationError: если обнаружен мат (преобразуется в HTTP 422).
            HTTPException: если тикет не найден (HTTP 404).
        """
        self.moderator.validate_operator_message(text)

        ticket = await self.ticket_repo.get_by_id(ticket_id)
        if ticket is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Обращение не найдено",
            )
        if ticket.status in TERMINAL_TICKET_STATUSES:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Обращение уже завершено",
            )

        operator_message = MessageModel(
            id=uuid6.uuid7(),
            ticket_id=ticket.id,
            sender_type=MessageSenderType.OPERATOR,
            sender_id=operator.id,
            text=text,
            moderation_status=MessageModerationStatus.PASSED,
            created_at=datetime.now(settings.TIMEZONE),
        )
        operator_message.sender = operator
        await self.repo.save_message(operator_message)
        await self.session.commit()

        await self.redis_context.add_message(
            ticket_id=ticket.id,
            sender=MessageSenderType.OPERATOR.value,
            text=text,
            timestamp=operator_message.created_at,
        )
        return operator_message

    async def stream_chat_events(
        self,
        user: UserModel,
        ticket_id: UUID | None,
        request: Request,
        last_event_id: UUID | str | None = None,
    ) -> AsyncGenerator[str, None]:
        """Оркестрирует поток Server-Sent Events для клиента по каналу обращения с поддержкой Replay."""
        if self.ticket_events is None:
            raise RuntimeError(
                "RedisTicketEvents не инициализирован в ChatService"
            )

        chat = await self.repo.get_by_client_id(user.id)
        if chat is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={
                    "code": "no_active_ticket",
                    "message": "Активное обращение не найдено",
                },
            )

        target_ticket_id: UUID
        if ticket_id is not None:
            ticket = await self.ticket_repo.get_by_id(ticket_id)
            if ticket is None:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Обращение не найдено",
                )
            if ticket.chat_id != chat.id:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Доступ к данному обращению запрещен",
                )
            target_ticket_id = ticket.id
        else:
            active_ticket = await self.ticket_repo.get_active_by_chat_id(
                chat.id
            )
            if active_ticket is None:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail={
                        "code": "no_active_ticket",
                        "message": "Активное обращение не найдено",
                    },
                )
            target_ticket_id = active_ticket.id

        seen_message_ids: set[str] = set()

        # Replay пропущенных сообщений по Last-Event-ID
        if last_event_id:
            parsed_event_id: UUID | None = None
            if isinstance(last_event_id, UUID):
                parsed_event_id = last_event_id
            elif isinstance(last_event_id, str):
                try:
                    parsed_event_id = UUID(last_event_id)
                except ValueError:
                    parsed_event_id = None

            if parsed_event_id:
                seen_message_ids.add(str(parsed_event_id))
                missed_messages = await self.repo.get_messages_since_id(
                    ticket_id=target_ticket_id,
                    last_event_id=parsed_event_id,
                )
                for msg in missed_messages:
                    seen_message_ids.add(str(msg.id))
                    msg_dto = MessageResponseSchema.model_validate(msg)
                    data_str = json.dumps(
                        msg_dto.model_dump(mode="json"),
                        ensure_ascii=False,
                        default=str,
                    )
                    yield f"id: {msg.id}\nevent: new_message\ndata: {data_str}\n\n"

        # Все выборки из БД завершены, дальше идет только Redis Pub/Sub.
        # Освобождаем сессию в пул соединений, чтобы не удерживать соединение с PostgreSQL.
        await self.session.close()

        async for chunk in self.ticket_events.subscribe_ticket_events(
            ticket_id=target_ticket_id, request=request
        ):
            if seen_message_ids:
                lines = chunk.splitlines()
                skip_chunk = False
                for line in lines:
                    if line.startswith("id: "):
                        ev_id = line[4:].strip()
                        if ev_id in seen_message_ids:
                            skip_chunk = True
                            break
                        seen_message_ids.add(ev_id)
                if skip_chunk:
                    continue

            yield chunk

    async def _safe_dispatch_task(
        self, line_code: str, trigger_reason: str
    ) -> None:
        """Безопасно ставит задачу dispatch_line_queue в очередь Taskiq."""
        try:
            from src.operators.schemas import DispatchPayloadSchema
            from src.operators.tasks import dispatch_line_queue

            await dispatch_line_queue.kiq(
                DispatchPayloadSchema(
                    line_code=line_code,
                    trigger_reason=trigger_reason,
                )
            )
        except Exception:
            logger.exception(
                "Не удалось поставить задачу dispatch_line_queue для линии %s (причина: %s)",
                line_code,
                trigger_reason,
            )

    async def _safe_copilot_task(self, ticket_id: UUID) -> None:
        """Безопасно ставит задачу generate_copilot_summary в очередь Taskiq."""
        try:
            from src.rag.schemas import CopilotPayloadSchema
            from src.rag.tasks import generate_copilot_summary

            await generate_copilot_summary.kiq(
                CopilotPayloadSchema(ticket_id=ticket_id)
            )
        except Exception:
            logger.exception(
                "Не удалось поставить задачу generate_copilot_summary для тикета %s",
                ticket_id,
            )

    async def _safe_enqueue_audit(
        self, ticket_id: UUID, trigger_reason: str
    ) -> None:
        """Безопасно ставит задачу audit_ticket_quality в очередь Taskiq с синхронным фолбэком."""
        try:
            from src.analytics.tasks import audit_ticket_quality

            await audit_ticket_quality.kiq(
                {
                    "ticket_id": str(ticket_id),
                    "trigger_reason": trigger_reason,
                }
            )
            logger.info(
                "Задача audit_ticket_quality для тикета %s поставлена в очередь (%s)",
                ticket_id,
                trigger_reason,
            )
        except Exception as exc:
            logger.warning(
                "Брокер Taskiq недоступен (%s), запуск синхронного аудита для тикета %s (фолбэк)",
                exc,
                ticket_id,
            )
            try:
                import redis.asyncio as aioredis

                from src.analytics.service import AnalyticsService

                redis_client = getattr(self.redis_context, "redis", None)
                if redis_client is None:
                    redis_client = aioredis.from_url(
                        settings.REDIS_URL, decode_responses=True
                    )

                analytics_service = AnalyticsService(
                    session=self.session, redis=redis_client
                )
                await analytics_service.audit_ticket_quality(
                    ticket_id=ticket_id,
                    trigger_reason=trigger_reason,
                )
                logger.info(
                    "Синхронный аудит для тикета %s успешно выполнен (фолбэк)",
                    ticket_id,
                )
            except Exception as fallback_exc:
                logger.error(
                    "Сбой синхронного аудита для тикета %s: %s",
                    ticket_id,
                    fallback_exc,
                )

    async def escalate_ticket(
        self,
        user: UserModel,
        payload: EscalateRequestSchema | None = None,
    ) -> ActiveTicketSummarySchema:
        """Переводит обращение клиента из bot_processing в queued к операторам."""
        chat = await self.repo.get_by_client_id(user.id)
        if chat is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Чат клиента не найден",
            )

        active_ticket = await self.ticket_repo.get_active_by_chat_id(chat.id)
        if active_ticket is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Активное обращение не найдено",
            )

        if active_ticket.status in (
            TicketStatus.QUEUED.value,
            TicketStatus.ASSIGNED.value,
            TicketStatus.IN_PROGRESS.value,
        ):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "code": "ticket_already_escalated",
                    "message": "Обращение уже находится в очереди или назначено оператору",
                },
            )

        # 1. Извлечение истории сообщений для контекста роутера
        conversation_history: list[dict[str, Any]] = []
        if self.redis_context is not None:
            try:
                recent_redis_msgs = await self.redis_context.get_messages(
                    active_ticket.id
                )
                if recent_redis_msgs:
                    for m in recent_redis_msgs[-8:]:
                        sender_role = (
                            "user"
                            if m.get("sender")
                            == MessageSenderType.CLIENT.value
                            else "assistant"
                        )
                        conversation_history.append(
                            {"role": sender_role, "text": m.get("text", "")}
                        )
            except Exception as exc:
                logger.warning(
                    "Не удалось получить контекст диалога из Redis для эскалации: %s",
                    exc,
                )

        if not conversation_history:
            ticket_db_msgs = await self.repo.get_messages_by_ticket_id(
                active_ticket.id
            )
            for m in ticket_db_msgs[-8:]:
                sender_role = (
                    "user"
                    if m.sender_type == MessageSenderType.CLIENT
                    else "assistant"
                )
                conversation_history.append(
                    {"role": sender_role, "text": m.text}
                )

        client_reason = (
            payload.reason.strip()
            if payload and payload.reason and payload.reason.strip()
            else None
        )

        # 2. Интеллектуальная классификация линии поддержки через EscalationRouter
        route_result = await self.escalation_router.route(
            conversation_history=conversation_history,
            client_reason=client_reason,
        )
        line_code = route_result.line

        from src.operators.repository import SupportLineRepository

        line_repo = SupportLineRepository(self.session)
        target_line = await line_repo.get_by_code(line_code)
        if target_line is not None:
            active_ticket.line_id = target_line.id
        elif active_ticket.line is not None:
            line_code = active_ticket.line.code
        else:
            l1_line = await line_repo.get_by_code("L1")
            if l1_line is not None:
                active_ticket.line_id = l1_line.id
            line_code = "L1"

        now = datetime.now(settings.TIMEZONE)
        active_ticket.status = TicketStatus.QUEUED.value
        active_ticket.opened_at = now
        active_ticket.escalation_reason = (
            client_reason if client_reason else route_result.reason
        )
        await self.ticket_repo.update(active_ticket)
        await self.session.commit()

        if self.line_queue is not None:
            await self.line_queue.enqueue_ticket(
                line_code=line_code,
                ticket_id=active_ticket.id,
                priority=active_ticket.priority,
            )

        await self._safe_dispatch_task(line_code, "ticket_escalated")
        await self._safe_copilot_task(active_ticket.id)

        operator_name: str | None = None
        if active_ticket.assigned_operator is not None:
            operator_name = (
                active_ticket.assigned_operator.full_name
                or active_ticket.assigned_operator.email
            )

        return ActiveTicketSummarySchema(
            id=active_ticket.id,
            priority=active_ticket.priority,
            status=active_ticket.status,
            line_code=line_code,
            assigned_operator_name=operator_name,
            created_at=active_ticket.created_at,
        )

    async def resolve_ticket_by_client(
        self,
        user: UserModel,
        ticket_id: UUID,
    ) -> ClientResolveTicketResponseSchema:
        """Подтверждает успешное решение вопроса клиентом."""
        chat = await self.repo.get_by_client_id(user.id)
        if chat is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Чат клиента не найден",
            )

        ticket = await self.ticket_repo.get_by_id(ticket_id)
        if ticket is None or ticket.chat_id != chat.id:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Обращение не найдено",
            )

        from src.chat.models import TERMINAL_TICKET_STATUSES

        if ticket.status in TERMINAL_TICKET_STATUSES:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Обращение уже завершено",
            )

        now = datetime.now(settings.TIMEZONE)
        ticket.status = TicketStatus.RESOLVED.value
        ticket.closed_at = now
        await self.ticket_repo.update(ticket)
        await self.session.commit()

        if self.redis_context is not None:
            await self.redis_context.clear_context(ticket.id)

        if self.ticket_events is not None:
            await self.ticket_events.publish_ticket_resolved(
                ticket_id=ticket.id,
                operator_id=ticket.assigned_operator_id,
                closed_at=now,
            )

        if ticket.line is not None and ticket.assigned_operator_id is not None:
            await self._safe_dispatch_task(ticket.line.code, "slot_freed")

        await self._safe_enqueue_audit(ticket.id, "client_resolved")

        return ClientResolveTicketResponseSchema(
            status="resolved",
            ticket_id=ticket.id,
            closed_at=now,
        )

    async def cancel_ticket_by_client(
        self,
        user: UserModel,
        ticket_id: UUID,
    ) -> CancelTicketResponseSchema:
        """Отменяет обращение клиентом до начала диалога с оператором."""
        chat = await self.repo.get_by_client_id(user.id)
        if chat is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Чат клиента не найден",
            )

        ticket = await self.ticket_repo.get_by_id(ticket_id)
        if ticket is None or ticket.chat_id != chat.id:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Обращение не найдено",
            )

        if ticket.status == TicketStatus.IN_PROGRESS.value:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Нельзя отменить обращение, диалог по которому уже начат специалистом",
            )

        now = datetime.now(settings.TIMEZONE)
        ticket.status = TicketStatus.CANCELED.value
        ticket.closed_at = now
        assigned_op_id = ticket.assigned_operator_id
        ticket.assigned_operator_id = None
        await self.ticket_repo.update(ticket)
        await self.session.commit()

        if self.redis_context is not None:
            await self.redis_context.clear_context(ticket.id)

        if self.ticket_events is not None:
            await self.ticket_events.publish_ticket_resolved(
                ticket_id=ticket.id,
                operator_id=assigned_op_id,
                closed_at=now,
            )

        if ticket.line is not None and assigned_op_id is not None:
            await self._safe_dispatch_task(ticket.line.code, "slot_freed")

        return CancelTicketResponseSchema(
            status="canceled",
            ticket_id=ticket.id,
        )
