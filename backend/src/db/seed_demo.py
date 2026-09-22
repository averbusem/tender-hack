"""Скрипт сидирования демонстрационных пользователей, ролей, линий поддержки и обращений."""

import asyncio
import logging
from datetime import datetime, timedelta
from pathlib import Path

import uuid6
from sqlalchemy import func, select, text

from src.analytics.models import (
    IncidentStatus,
    IncidentType,
    RootCauseType,
    SystemIncidentModel,
    TicketAuditModel,
    TicketFeedbackModel,
)
from src.auth.models import ClientProfileModel, RoleModel, UserModel, UserRole
from src.chat.models import (
    ChatModel,
    MessageModel,
    MessageModerationStatus,
    MessageSenderType,
    TicketModel,
    TicketPriority,
    TicketStatus,
)
from src.core.config import settings
from src.core.security import hash_password
from src.db.database import async_session_maker
from src.kb.models import KbDocumentModel
from src.operators.models import (
    OperatorProfileModel,
    OperatorShiftStatus,
    SupportLineModel,
    TicketCopilotSummaryModel,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("seed_demo")


async def seed() -> None:
    """Идемпотентно наполняет базу данных демонстрационными записями."""
    async with async_session_maker() as session:
        roles_data = [
            (
                UserRole.CLIENT,
                "Клиент (Поставщик)",
                "Пользователь портала поставщиков",
            ),
            (
                UserRole.OPERATOR,
                "Оператор поддержки",
                "Специалист линии поддержки",
            ),
            (
                UserRole.SUPERVISOR,
                "Руководитель поддержки",
                "Контроль качества и супервизия",
            ),
            (
                UserRole.ADMIN,
                "Системный администратор",
                "Полный доступ к системе",
            ),
        ]

        roles: dict[str, RoleModel] = {}
        for code, name, desc in roles_data:
            stmt = select(RoleModel).where(RoleModel.code == code)
            role = (await session.scalars(stmt)).first()
            if not role:
                role = RoleModel(code=code, name=name, description=desc)
                session.add(role)
                await session.flush()
                logger.info("Создана роль: %s", code)
            roles[code] = role

        lines_data = [
            (
                "L1",
                "Первая линия — регламенты и каталог",
                "Консультации по регламентам закупок, каталогу и навигации",
            ),
            (
                "L2",
                "Вторая линия — ЭЦП и технические сбои",
                "Техническая поддержка плагина КриптоПро, ошибок ЭДО и УПД",
            ),
            (
                "L3",
                "Третья линия — разработчики и инфраструктура",
                "Эскалация системных аварий, недоступности серверов и багов ПО",
            ),
        ]
        lines: dict[str, SupportLineModel] = {}
        for code, name, desc in lines_data:
            stmt_line = select(SupportLineModel).where(
                SupportLineModel.code == code
            )
            line_obj = (await session.scalars(stmt_line)).first()
            if not line_obj:
                line_obj = SupportLineModel(
                    code=code,
                    name=name,
                    description=desc,
                    is_active=True,
                )
                session.add(line_obj)
                await session.flush()
                logger.info("Создана линия поддержки: %s", code)
            lines[code] = line_obj

        # Удаление устаревшей линии general, если она осталась от ранних версий
        stmt_old_line = select(SupportLineModel).where(
            SupportLineModel.code == "general"
        )
        old_general_line = (await session.scalars(stmt_old_line)).first()
        if old_general_line:
            # Перепривязываем операторов и тикеты с general на L1
            stmt_update_profiles = select(OperatorProfileModel).where(
                OperatorProfileModel.line_id == old_general_line.id
            )
            profiles_to_reassign = (
                await session.scalars(stmt_update_profiles)
            ).all()
            for p in profiles_to_reassign:
                p.line_id = lines["L1"].id

            stmt_update_tickets = select(TicketModel).where(
                TicketModel.line_id == old_general_line.id
            )
            tickets_to_reassign = (
                await session.scalars(stmt_update_tickets)
            ).all()
            for t in tickets_to_reassign:
                t.line_id = lines["L1"].id

            await session.flush()
            await session.delete(old_general_line)
            await session.flush()
            logger.info(
                "Устаревшая линия general удалена, связанные профили переведены на L1"
            )

        # Сидирование демонстрационных пользователей (пароль: password123)
        default_pwd_hash = hash_password("password123")

        # Клиент-поставщик
        supplier_email = "supplier@example.com"
        stmt_user = select(UserModel).where(UserModel.email == supplier_email)
        supplier = (await session.scalars(stmt_user)).first()
        if not supplier:
            supplier = UserModel(
                role_id=roles[UserRole.CLIENT].id,
                email=supplier_email,
                password_hash=default_pwd_hash,
                full_name="Иванов Иван Иванович",
                is_active=True,
            )
            session.add(supplier)
            await session.flush()

            profile = ClientProfileModel(
                user_id=supplier.id,
                company_name="ООО «ТехноСнаб Поставка»",
                inn="7701234567",
                kpp="770101001",
                phone="+7 (495) 123-45-67",
            )
            session.add(profile)

            chat = ChatModel(client_id=supplier.id)
            session.add(chat)
            await session.flush()
            logger.info("Создан тестовый поставщик: %s", supplier_email)
        else:
            stmt_chat = select(ChatModel).where(
                ChatModel.client_id == supplier.id
            )
            chat = (await session.scalars(stmt_chat)).first()
            if not chat:
                chat = ChatModel(client_id=supplier.id)
                session.add(chat)
                await session.flush()

        # Операторы по линиям
        operators_data = [
            (
                "operator1@example.com",
                "Смирнова Анна Сергеевна",
                "L1",
                5,
            ),
            (
                "operator2@example.com",
                "Кузнецов Петр Васильевич",
                "L2",
                5,
            ),
            (
                "operator3@example.com",
                "Соколова Елена Дмитриевна",
                "L3",
                5,
            ),
        ]

        operator: UserModel | None = None
        operators_by_line: dict[str, UserModel] = {}
        for op_email, op_name, line_code, slots in operators_data:
            stmt_op = select(UserModel).where(UserModel.email == op_email)
            op_user = (await session.scalars(stmt_op)).first()
            if not op_user:
                op_user = UserModel(
                    role_id=roles[UserRole.OPERATOR].id,
                    email=op_email,
                    password_hash=default_pwd_hash,
                    full_name=op_name,
                    is_active=True,
                )
                session.add(op_user)
                await session.flush()

                op_profile = OperatorProfileModel(
                    user_id=op_user.id,
                    line_id=lines[line_code].id,
                    shift_status=OperatorShiftStatus.ACTIVE,
                    max_slots=slots,
                )
                session.add(op_profile)
                logger.info(
                    "Создан тестовый оператор %s (%s): %s",
                    line_code,
                    op_name,
                    op_email,
                )
            else:
                stmt_prof = select(OperatorProfileModel).where(
                    OperatorProfileModel.user_id == op_user.id
                )
                op_profile = (await session.scalars(stmt_prof)).first()
                if op_profile:
                    op_profile.line_id = lines[line_code].id
                    op_profile.shift_status = OperatorShiftStatus.ACTIVE

            operators_by_line[line_code] = op_user
            if op_email == "operator1@example.com":
                operator = op_user

        # Руководитель (супервизор)
        admin_email = "admin@example.com"
        stmt_admin = select(UserModel).where(UserModel.email == admin_email)
        admin_user = (await session.scalars(stmt_admin)).first()
        if not admin_user:
            admin_user = UserModel(
                role_id=roles[UserRole.SUPERVISOR].id,
                email=admin_email,
                password_hash=default_pwd_hash,
                full_name="Ковалев Михаил Петрович",
                is_active=True,
            )
            session.add(admin_user)
            await session.flush()

            admin_profile = OperatorProfileModel(
                user_id=admin_user.id,
                line_id=lines["L1"].id,
                shift_status=OperatorShiftStatus.OFFLINE,
                max_slots=10,
            )
            session.add(admin_profile)
            logger.info("Создан тестовый супервизор: %s", admin_email)
        else:
            stmt_adm_prof = select(OperatorProfileModel).where(
                OperatorProfileModel.user_id == admin_user.id
            )
            admin_profile = (await session.scalars(stmt_adm_prof)).first()
            if admin_profile:
                admin_profile.line_id = lines["L1"].id
                admin_profile.shift_status = OperatorShiftStatus.OFFLINE

        # Сидирование 30 демонстрационных обращений поставщиков по методичкам
        stmt_t_count = select(func.count(TicketModel.id)).where(
            TicketModel.chat_id == chat.id
        )
        existing_tickets_count = (await session.scalar(stmt_t_count)) or 0

        if existing_tickets_count < 25:
            logger.info(
                "Генерация 30 реалистичных обращений поставщиков для аналитики системных проблем..."
            )
            now = datetime.now(settings.TIMEZONE)

            # Шаблоны 30 сценариев (8 yml, 8 crypto, 6 upd/signing, 4 nav, 4 positive)
            scenarios = [
                # 8 сценариев YML
                {
                    "theme": "yml_import",
                    "line": "L1",
                    "priority": TicketPriority.P1,
                    "operator": operator,
                    "query": "Ошибка импорта YML: тег <param name='Цвет'> не проходит валидацию на строке 48. Как загрузить оферты в каталог?",
                    "bot_reply": "Согласно регламенту ведения каталога СТЕ, тег <param> должен содержать обязательный атрибут unit или присутствовать в справочнике характеристик категории.",
                    "op_reply": "Здравствуйте! Проверил ваш файл: в категории 'Канцтовары' параметр 'Цвет' требует выбора из выпадающего списка в ЛК. Исправьте структуру YML.",
                    "score": 2,
                    "comment": "Прайс-лист не грузится уже третий день! Бот и оператор твердят про невалидный тег <param>, хотя файл сформирован по шаблону из инструкции!",
                    "root_cause": RootCauseType.SYSTEM_ISSUE,
                    "audit_summary": "Сбой загрузки YML-прайс-листа из-за строгой схемы валидации тега <param>. Клиент не может выгрузить каталог товаров.",
                    "is_system_issue": True,
                    "incident_type": IncidentType.API_ERROR,
                    "incident_desc": "Отказ парсера YML-каталога: непредусмотренный атрибут в теге <param>",
                },
                {
                    "theme": "yml_import",
                    "line": "L1",
                    "priority": TicketPriority.P2,
                    "operator": operator,
                    "query": "Не могу загрузить прайс в формате YML. Пишет 'Ошибка валидации XML схемы: незакрытый тег <offer>'.",
                    "bot_reply": "Проверьте кодировку файла. Файл должен быть сохранен в UTF-8 без BOM и содержать закрывающий тег </offer>.",
                    "op_reply": "В строке 112 у вас нарушена XML-разметка. Пожалуйста, валидируйте файл через Notepad++ перед отправкой.",
                    "score": 1,
                    "comment": "Постоянные сбои при загрузке прайса! Почему Портал не указывает точную строку с ошибкой, а просто блокирует весь импорт?",
                    "root_cause": RootCauseType.SYSTEM_ISSUE,
                    "audit_summary": "Неинформативная диагностика ошибок валидации структуры YML при импорте прайс-листа.",
                    "is_system_issue": True,
                    "incident_type": None,
                    "incident_desc": None,
                },
                {
                    "theme": "yml_import",
                    "line": "L1",
                    "priority": TicketPriority.P2,
                    "operator": None,
                    "query": "Загрузка прайс-листа зависла на 0% в статусе 'Идет проверка файла YML'. Что делать?",
                    "bot_reply": "Проверка объемных прайс-листов (более 5000 позиций) может занимать до 30 минут. Пожалуйста, ожидайте смены статуса.",
                    "op_reply": None,
                    "score": 2,
                    "comment": "Файл прайс-листа висит в обработке часами. Автоматическая проверка YML работает крайне нестабильно.",
                    "root_cause": RootCauseType.SYSTEM_ISSUE,
                    "audit_summary": "Задержка очереди асинхронной обработки YML-прайсов.",
                    "is_system_issue": True,
                    "incident_type": IncidentType.PORTAL_DOWNTIME,
                    "incident_desc": "Зависание очереди импорта каталога товаров",
                },
                {
                    "theme": "yml_import",
                    "line": "L1",
                    "priority": TicketPriority.P2,
                    "operator": operator,
                    "query": "В выгрузке YML пропали цены после обновления каталога. В файле цены указаны, в ЛК стоят нули.",
                    "bot_reply": "Убедитесь, что тег <price> содержит числовое значение с точкой в качестве разделителя копеек.",
                    "op_reply": "У вас в теге цены стояла запятая: 1500,50 вместо 1500.50. Парсер обнулил некорректное значение.",
                    "score": 3,
                    "comment": "Слишком строгие требования к прайс-листам. Неужели система не может автоматически заменить запятую на точку?",
                    "root_cause": RootCauseType.REGULATION_DISSATISFACTION,
                    "audit_summary": "Претензия к строгости формата десятичного разделителя в теге <price> YML.",
                    "is_system_issue": False,
                    "incident_type": None,
                    "incident_desc": None,
                },
                {
                    "theme": "yml_import",
                    "line": "L1",
                    "priority": TicketPriority.P1,
                    "operator": operator,
                    "query": "Ошибка: дубликат артикула поставщика в теге <vendorCode> файла YML. Хотя артикулы уникальны!",
                    "bot_reply": "Артикул vendorCode сопоставляется с уже зарегистрированными СТЕ вашей организации в реестре оферт.",
                    "op_reply": "Обнаружил, что аналогичный артикул уже привязан к архивной оферте №10432. Нужно изменить артикул или деактивировать старую позицию.",
                    "score": 2,
                    "comment": "Архивные позиции блокируют загрузку нового прайса. Ошибки импорта YML не позволяют быстро обновлять остатки.",
                    "root_cause": RootCauseType.OPERATOR_ERROR,
                    "audit_summary": "Конфликт валидации YML-артикулов с архивными записями каталога.",
                    "is_system_issue": False,
                    "incident_type": None,
                    "incident_desc": None,
                },
                {
                    "theme": "yml_import",
                    "line": "L1",
                    "priority": TicketPriority.P2,
                    "operator": None,
                    "query": "Где скачать актуальный XSD-шаблон для выгрузки прайса YML с учетом требований 2026 года?",
                    "bot_reply": "Актуальная спецификация формата YML и XSD-схема доступны в Базе знаний в разделе 'Интеграция каталога СТЕ': https://zakupki.mos.ru/help/yml-spec.",
                    "op_reply": None,
                    "score": 3,
                    "comment": "В скачанной инструкции ссылка на схему устарела. Пришлось тратить время на поиск правильного формата тегов.",
                    "root_cause": RootCauseType.REGULATION_DISSATISFACTION,
                    "audit_summary": "Неактуальная гиперссылка на спецификацию формата YML в регламенте.",
                    "is_system_issue": False,
                    "incident_type": None,
                    "incident_desc": None,
                },
                {
                    "theme": "yml_import",
                    "line": "L1",
                    "priority": TicketPriority.P2,
                    "operator": operator,
                    "query": "При импорте YML пишет 'Недопустимая категория товара'. ID категории скопирован из классификатора Портала.",
                    "bot_reply": "Проверьте, не была ли категория деактивирована в последнем релизе классификатора СПГЗ.",
                    "op_reply": "Категория 'Расходные материалы' была объединена со 'Спецоснасткой'. Используйте новый ID 44021.",
                    "score": 2,
                    "comment": "Классификатор обновили без предупреждения, весь YML-прайс перестал загружаться!",
                    "root_cause": RootCauseType.SYSTEM_ISSUE,
                    "audit_summary": "Рассинхронизация классификатора категорий при валидации YML-импорта.",
                    "is_system_issue": True,
                    "incident_type": None,
                    "incident_desc": None,
                },
                {
                    "theme": "yml_import",
                    "line": "L1",
                    "priority": TicketPriority.P1,
                    "operator": operator,
                    "query": "Ошибка кодировки YML: кракозябры вместо кириллицы в наименованиях оферт.",
                    "bot_reply": "Сохраняйте файл строго в кодировке UTF-8. Кодировка Windows-1251 не поддерживается парсером Портала.",
                    "op_reply": "Конвертировал ваш тестовый файл в UTF-8, загрузка прошла успешно. Обратите внимание на настройки вашей 1С.",
                    "score": 3,
                    "comment": "1С стандартно выгружает прайс в 1251. Приходится вручную перекодировать каждый прайс-лист.",
                    "root_cause": RootCauseType.REGULATION_DISSATISFACTION,
                    "audit_summary": "Трудности конвертации кодировки файла прайса поставщика.",
                    "is_system_issue": False,
                    "incident_type": None,
                    "incident_desc": None,
                },
                # 8 сценариев CryptoPro / ЭЦП
                {
                    "theme": "crypto_plugin",
                    "line": "L2",
                    "priority": TicketPriority.P0,
                    "operator": operator,
                    "query": "Срочно! Идет котировочная сессия, не могу подписать оферту! Ошибка плагина: 0x80090016 Набор ключей не существует.",
                    "bot_reply": "Ошибка 0x80090016 обычно возникает, если КриптоПро CSP не может прочитать закрытый ключ на токене. Переподключите токен.",
                    "op_reply": "Добрый день! Проверьте, установлен ли корневой сертификат УЦ ФНС в хранилище 'Доверенные корневые центры' и перезапустите браузер Chromium-Gost.",
                    "score": 1,
                    "comment": "Сбои плагина ЭЦП КриптоПро при подписании оферты! Ошибка 0x80090016 срывает подачу заявки, сессия закроется через 15 минут!",
                    "root_cause": RootCauseType.SYSTEM_ISSUE,
                    "audit_summary": "Критический технический сбой КриптоПро Browser Plug-in (код 0x80090016) в процессе подписания оферты поставщиком.",
                    "is_system_issue": True,
                    "incident_type": IncidentType.CRYPTO_PLUGIN,
                    "incident_desc": "Ошибка 0x80090016 КриптоПро при подписании оферты в браузере",
                },
                {
                    "theme": "crypto_plugin",
                    "line": "L2",
                    "priority": TicketPriority.P0,
                    "operator": operator,
                    "query": "Плагин КриптоПро ЭЦП Browser plug-in не загружается в браузере. Кнопка 'Подписать контракт' неактивна.",
                    "bot_reply": "Убедитесь, что расширение CryptoPro Extension включено в настройках браузера и служба cadesplugin запущена.",
                    "op_reply": "Выполните сброс кэша расширения и проверьте статус плагина на тестовой странице проверки ЭЦП.",
                    "score": 1,
                    "comment": "Браузерный плагин ЭЦП постоянно падает или не видит сертификат после обновления Chrome!",
                    "root_cause": RootCauseType.SYSTEM_ISSUE,
                    "audit_summary": "Сбой обнаружения КриптоПро Browser plug-in из-за несовместимости с новой версией браузера.",
                    "is_system_issue": True,
                    "incident_type": IncidentType.CRYPTO_PLUGIN,
                    "incident_desc": "Падение cadesplugin после обновления браузера Chromium",
                },
                {
                    "theme": "crypto_plugin",
                    "line": "L2",
                    "priority": TicketPriority.P1,
                    "operator": operator,
                    "query": "Ошибка проверки подписи: 'Цепочка сертификатов не может быть проверена'. Подписание оферты заблокировано.",
                    "bot_reply": "Для проверки подписи требуется установить промежуточный сертификат Минцифры России головного УЦ.",
                    "op_reply": "Отправил вам прямую ссылку на установщик цепочки сертификатов Минцифры. Установите и перезагрузитесь.",
                    "score": 2,
                    "comment": "Плагин выдает ошибки проверки цепочки сертификатов. Инструкция по настройке на Портале не обновлялась с прошлого года.",
                    "root_cause": RootCauseType.SYSTEM_ISSUE,
                    "audit_summary": "Отсутствие актуальных сертификатов Головного УЦ в инструкции по настройке ЭЦП.",
                    "is_system_issue": True,
                    "incident_type": IncidentType.CRYPTO_PLUGIN,
                    "incident_desc": "Ошибка валидации цепочки сертификатов ЭЦП",
                },
                {
                    "theme": "crypto_plugin",
                    "line": "L2",
                    "priority": TicketPriority.P1,
                    "operator": None,
                    "query": "Поддерживает ли Портал работу с КриптоПро версии 5.0 R3 и токенами Рутокен ЭЦП 3.0?",
                    "bot_reply": "Да, Портал поставщиков полностью поддерживает КриптоПро CSP 5.0 R3 и носители Рутокен ЭЦП 2.0/3.0 при использовании ГОСТ Р 34.10-2012.",
                    "op_reply": None,
                    "score": 3,
                    "comment": "Вроде поддерживает, но плагин регулярно подвисает при считывании контейнера с Рутокена 3.0.",
                    "root_cause": RootCauseType.SYSTEM_ISSUE,
                    "audit_summary": "Периодические зависания интерфейса взаимодействия с токенами Рутокен 3.0.",
                    "is_system_issue": True,
                    "incident_type": IncidentType.CRYPTO_PLUGIN,
                    "incident_desc": "Таймаут считывания смарт-карты Рутокен в КриптоПро",
                },
                {
                    "theme": "crypto_plugin",
                    "line": "L2",
                    "priority": TicketPriority.P0,
                    "operator": operator,
                    "query": "При нажатии 'Подписать протокол' окно плагина ЭЦП зависает на этапе 'Инициализация криптопровайдера...'.",
                    "bot_reply": "Проверьте лицензию КриптоПро CSP. При истечении срока демонстрационной лицензии вызов криптомодуля блокируется.",
                    "op_reply": "Лицензия у вас активна. Рекомендую удалить старые версии плагина через панель управления и переустановить cadesplugin версии 2.0.148.",
                    "score": 1,
                    "comment": "Зависает окно плагина КриптоПро на этапе подписания протокола. Проблема массовая у всех наших бухгалтеров!",
                    "root_cause": RootCauseType.SYSTEM_ISSUE,
                    "audit_summary": "Зависание диалогового окна cadesplugin на шаге вызова функции SignHash.",
                    "is_system_issue": True,
                    "incident_type": IncidentType.CRYPTO_PLUGIN,
                    "incident_desc": "Зависание функции cadesplugin SignHash",
                },
                {
                    "theme": "crypto_plugin",
                    "line": "L2",
                    "priority": TicketPriority.P1,
                    "operator": operator,
                    "query": "Ошибка плагина: Не удалось создать объект CAdESCOM.CPSigner. Браузер Яндекс, расширение включено.",
                    "bot_reply": "Добавьте адрес *.mos.ru в список доверенных узлов КриптоПро ЭЦП Browser plug-in.",
                    "op_reply": "Откройте 'Настройки КриптоПро ЭЦП' -> 'Доверенные сайты' и введите https://zakupki.mos.ru. После этого сохраните изменения.",
                    "score": 2,
                    "comment": "Почему настройки плагина ЭЦП слетают после каждой очистки куки в браузере? Очень неудобно!",
                    "root_cause": RootCauseType.REGULATION_DISSATISFACTION,
                    "audit_summary": "Сброс доверенных узлов в браузере пользователя, повлекший ошибку CPSigner.",
                    "is_system_issue": False,
                    "incident_type": None,
                    "incident_desc": None,
                },
                {
                    "theme": "crypto_plugin",
                    "line": "L2",
                    "priority": TicketPriority.P1,
                    "operator": operator,
                    "query": "Выскакивает ошибка: 'Указан неверный алгоритм подписи сертификата'. Сертификат выдан ФНС по ГОСТ-2012.",
                    "bot_reply": "Проверьте версию КриптоПро. Для алгоритмов ГОСТ Р 34.10-2012 необходима версия КриптоПро CSP не ниже 4.0.9944.",
                    "op_reply": "У вас установлена устаревшая сборка КриптоПро 4.0. Обновитесь до актуальной сборки с сайта производителя.",
                    "score": 2,
                    "comment": "Сложная настройка сертификатов и плагина. Нет автоматической утилиты самодиагностики рабочего места на Портале.",
                    "root_cause": RootCauseType.OPERATOR_ERROR,
                    "audit_summary": "Использование устаревшей сборки криптопровайдера на стороне клиента.",
                    "is_system_issue": False,
                    "incident_type": None,
                    "incident_desc": None,
                },
                {
                    "theme": "crypto_plugin",
                    "line": "L2",
                    "priority": TicketPriority.P2,
                    "operator": operator,
                    "query": "Сертификат отображается как 'Недействителен' в окне выбора подписи на Портале.",
                    "bot_reply": "Убедитесь, что список отзыва сертификатов (CRL) успешно скачан и установлен в систему.",
                    "op_reply": "Списки отзыва УЦ обновились. Выполнил проверку вашего сертификата — статус 'Действителен'. Попробуйте снова.",
                    "score": 3,
                    "comment": "Сбои в проверке актуальности сертификатов ЭЦП отнимают кучу рабочего времени.",
                    "root_cause": RootCauseType.SYSTEM_ISSUE,
                    "audit_summary": "Задержка обновления локальных списков отзыва сертификатов.",
                    "is_system_issue": True,
                    "incident_type": None,
                    "incident_desc": None,
                },
                # 6 сценариев UPD / Signing
                {
                    "theme": "contract_signing",
                    "line": "L2",
                    "priority": TicketPriority.P1,
                    "operator": operator,
                    "query": "Не могу прикрепить УПД к исполненному контракту №9923/26. Выдает 'Ошибка формата XML файла УПД по приказу 820'.",
                    "bot_reply": "Портал принимает универсальные передаточные документы строго в формате приказа ФНС России № ММВ-7-15/820@ (версия 5.01).",
                    "op_reply": "В вашем УПД отсутствует обязательный идентификатор государственного контракта (ИГК). Добавьте его в файл перед загрузкой.",
                    "score": 2,
                    "comment": "Сложности и сбои при подписании оферт и прикреплении УПД! Формат УПД по 820 приказу отклоняется системой без внятного лога ошибок.",
                    "root_cause": RootCauseType.SYSTEM_ISSUE,
                    "audit_summary": "Сбой автоматической валидации схемы XML универсального передаточного документа при передаче в ЕАИСТ.",
                    "is_system_issue": True,
                    "incident_type": IncidentType.API_ERROR,
                    "incident_desc": "Ошибка валидатора УПД при сопоставлении с ИГК контракта",
                },
                {
                    "theme": "contract_signing",
                    "line": "L2",
                    "priority": TicketPriority.P1,
                    "operator": operator,
                    "query": "Зависает статус оферты в положении 'Подписание заказчиком' более 3 рабочих дней. Что делать по регламенту?",
                    "bot_reply": "Согласно регламенту, у заказчика есть 3 рабочих дня на подписание оферты. После этого система направляет уведомление контролеру.",
                    "op_reply": "Направил запрос заказчику с напоминанием о сроках регламента подписания. Статус находится на контроле.",
                    "score": 2,
                    "comment": "Процесс подписания оферт непрозрачен, заказчики затягивают сроки, а система не накладывает штрафы.",
                    "root_cause": RootCauseType.REGULATION_DISSATISFACTION,
                    "audit_summary": "Претензия поставщика к затягиванию сроков подписания оферты со стороны заказчика.",
                    "is_system_issue": False,
                    "incident_type": None,
                    "incident_desc": None,
                },
                {
                    "theme": "contract_signing",
                    "line": "L2",
                    "priority": TicketPriority.P1,
                    "operator": operator,
                    "query": "Ошибка синхронизации УПД с системой ЭДО 'Диадок'. Документ подписан, но на Портал поставщиков статус не вернулся.",
                    "bot_reply": "Интеграционный шлюз с внешними операторами ЭДО обрабатывает статусы пакетами каждые 2 часа.",
                    "op_reply": "Инициировал принудительную синхронизацию идентификатора пакета ЭДО. Статус документа на Портале обновлен на 'Подписан'.",
                    "score": 2,
                    "comment": "Сбои интеграции при передаче УПД из Диадока. Приходится каждый раз писать в техподдержку для ручной синхронизации.",
                    "root_cause": RootCauseType.SYSTEM_ISSUE,
                    "audit_summary": "Задержка передачи статусов подписания УПД через интеграционный шлюз ЭДО.",
                    "is_system_issue": True,
                    "incident_type": IncidentType.API_ERROR,
                    "incident_desc": "Задержка интеграционного шлюза ЭДО Диадок",
                },
                {
                    "theme": "contract_signing",
                    "line": "L2",
                    "priority": TicketPriority.P2,
                    "operator": operator,
                    "query": "Как отозвать ошибочно прикрепленный файл УПД до подписания контракта заказчиком?",
                    "bot_reply": "Пока контракт не подписан заказчиком, вы можете нажать кнопку 'Аннулировать документ' в карточке исполнения контракта.",
                    "op_reply": "Помог поставщику сформировать уведомление об аннулировании некорректного УПД.",
                    "score": 3,
                    "comment": "Кнопка аннулирования УПД запрятана в глубине меню, еле нашли с оператором.",
                    "root_cause": RootCauseType.NONE,
                    "audit_summary": "Консультация по аннулированию ошибочного закрывающего документа.",
                    "is_system_issue": False,
                    "incident_type": None,
                    "incident_desc": None,
                },
                {
                    "theme": "contract_signing",
                    "line": "L2",
                    "priority": TicketPriority.P1,
                    "operator": operator,
                    "query": "Ошибка 'Сумма позиций УПД не совпадает с суммой спецификации контракта'. Разница в 1 копейку из-за округления НДС!",
                    "bot_reply": "Сумма в УПД должна строго до копейки совпадать с суммой соответствующего этапа спецификации контракта.",
                    "op_reply": "Рекомендуем скорректировать копейку в строке с наибольшей стоимостью для прохождения контроля ЕАИСТ.",
                    "score": 2,
                    "comment": "Из-за округления копейки НДС невозможно прикрепить УПД и получить оплату за выполненные работы!",
                    "root_cause": RootCauseType.SYSTEM_ISSUE,
                    "audit_summary": "Блокировка закрытия контракта алгоритмом жесткой валидации сумм НДС.",
                    "is_system_issue": True,
                    "incident_type": None,
                    "incident_desc": None,
                },
                {
                    "theme": "contract_signing",
                    "line": "L2",
                    "priority": TicketPriority.P1,
                    "operator": None,
                    "query": "Какой предельный срок подписания проекта контракта победителем котировочной сессии?",
                    "bot_reply": "Победитель обязан подписать проект контракта в течение 1 рабочего дня с момента его публикации заказчиком.",
                    "op_reply": None,
                    "score": 3,
                    "comment": "Сроки подписания слишком жесткие, не успеваем согласовать протокол разногласий.",
                    "root_cause": RootCauseType.REGULATION_DISSATISFACTION,
                    "audit_summary": "Недовольство регламентными сроками подписания проекта контракта по 44-ФЗ.",
                    "is_system_issue": False,
                    "incident_type": None,
                    "incident_desc": None,
                },
                # 4 сценария Navigation
                {
                    "theme": "portal_navigation",
                    "line": "L1",
                    "priority": TicketPriority.P2,
                    "operator": operator,
                    "query": "Где теперь находится кнопка 'Подать оферту' в новой версии раздела котировочных сессий? Все перерыл!",
                    "bot_reply": "Кнопка 'Подать предложение' перемещена в правый верхний блок карточки сессии над графиком торгов.",
                    "op_reply": "Подсказал поставщику: перейдите в раздел 'Закупки' -> 'Котировочные сессии', откройте карточку и нажмите синюю кнопку в правом сайдбаре.",
                    "score": 2,
                    "comment": "Затруднения пользователей при навигации и поиске разделов Портала! После редизайна найти нужную кнопку невозможно!",
                    "root_cause": RootCauseType.OPERATOR_ERROR,
                    "audit_summary": "Сложности навигации поставщика в обновленном интерфейсе котировочных сессий.",
                    "is_system_issue": False,
                    "incident_type": None,
                    "incident_desc": None,
                },
                {
                    "theme": "portal_navigation",
                    "line": "L1",
                    "priority": TicketPriority.P2,
                    "operator": None,
                    "query": "Не могу найти вкладку 'Мои оферты' в личном кабинете поставщика.",
                    "bot_reply": "Вкладка 'Оферты' находится в левом меню в подразделе 'Каталог продукции' -> 'Мои публикации'.",
                    "op_reply": None,
                    "score": 3,
                    "comment": "Интерфейс перегружен подменю, навигация не интуитивная, поиск по разделам работает плохо.",
                    "root_cause": RootCauseType.NONE,
                    "audit_summary": "Консультация по расположению реестра опубликованных оферт в ЛК.",
                    "is_system_issue": False,
                    "incident_type": None,
                    "incident_desc": None,
                },
                {
                    "theme": "portal_navigation",
                    "line": "L1",
                    "priority": TicketPriority.P2,
                    "operator": operator,
                    "query": "Как настроить фильтр по региону поставки в реестре потребностей заказчиков?",
                    "bot_reply": "Откройте расширенный фильтр в шапке реестра и укажите субъект РФ в поле 'Место поставки товара'.",
                    "op_reply": "Показал, как сохранить выбранный фильтр в качестве избранного для быстрого доступа.",
                    "score": 3,
                    "comment": "Фильтры поиска сбрасываются при перезагрузке страницы. Приходится настраивать заново.",
                    "root_cause": RootCauseType.OPERATOR_ERROR,
                    "audit_summary": "Несохранение пользовательских фильтров поиска потребностей.",
                    "is_system_issue": False,
                    "incident_type": None,
                    "incident_desc": None,
                },
                {
                    "theme": "portal_navigation",
                    "line": "L1",
                    "priority": TicketPriority.P2,
                    "operator": operator,
                    "query": "Где посмотреть историю изменений поданной оферты и протоколы согласования?",
                    "bot_reply": "История версий доступна на вкладке 'Журнал событий' внутри карточки конкретной оферты.",
                    "op_reply": "Передал скриншот расположения вкладки 'Журнал событий'.",
                    "score": 2,
                    "comment": "Очень сложно ориентироваться в новом интерфейсе, нужные вкладки скрыты за выпадающими списками.",
                    "root_cause": RootCauseType.NONE,
                    "audit_summary": "Консультация по просмотру аудиторского следа изменений оферты.",
                    "is_system_issue": False,
                    "incident_type": None,
                    "incident_desc": None,
                },
                # 4 положительных сценария (high scores, fast deflection)
                {
                    "theme": "positive",
                    "line": "L1",
                    "priority": TicketPriority.P2,
                    "operator": None,
                    "query": "Подскажите перечень документов для первичной аккредитации поставщика на Портале.",
                    "bot_reply": "Для аккредитации юридического лица требуются: выписка из ЕГРЮЛ (не старше 30 дней), скан устава, документ о полномочиях руководителя и сертификат усиленной КЭП. Форма подачи: ЛК -> Профиль -> Аккредитация.",
                    "op_reply": None,
                    "score": 5,
                    "comment": "Отличный сервис! Бот за 3 секунды выдал точный перечень документов по регламенту аккредитации, все понятно и четко!",
                    "root_cause": RootCauseType.NONE,
                    "audit_summary": "Безупречная консультация виртуального ассистента по порядку аккредитации поставщика.",
                    "is_system_issue": False,
                    "incident_type": None,
                    "incident_desc": None,
                },
                {
                    "theme": "positive",
                    "line": "L1",
                    "priority": TicketPriority.P2,
                    "operator": None,
                    "query": "Какая комиссия взимается с победителя за участие в котировочной сессии?",
                    "bot_reply": "Участие в котировочных сессиях для поставщиков бесплатно. Плата за победу в соответствии с регламентом Портала поставщиков Москвы не взимается.",
                    "op_reply": None,
                    "score": 5,
                    "comment": "Быстрый и исчерпывающий ответ бота. Спасибо за оперативность!",
                    "root_cause": RootCauseType.NONE,
                    "audit_summary": "Быстрое разъяснение регламента отсутствия комиссии для участников.",
                    "is_system_issue": False,
                    "incident_type": None,
                    "incident_desc": None,
                },
                {
                    "theme": "positive",
                    "line": "L1",
                    "priority": TicketPriority.P1,
                    "operator": operator,
                    "query": "Спасибо оператору за помощь в настройке карточки товара и быстрое решение вопроса!",
                    "bot_reply": "Рады были помочь! Обращайтесь в любое время.",
                    "op_reply": "Всегда рады помочь! Успешных вам торгов и побед на Портале поставщиков!",
                    "score": 5,
                    "comment": "Оператор Анна помогла быстро исправить категорию оферты, все заработало моментально. Очень вежливое и профессиональное обслуживание!",
                    "root_cause": RootCauseType.NONE,
                    "audit_summary": "Высокое качество обслуживания оператором, благодарность поставщика.",
                    "is_system_issue": False,
                    "incident_type": None,
                    "incident_desc": None,
                },
                {
                    "theme": "positive",
                    "line": "L1",
                    "priority": TicketPriority.P2,
                    "operator": None,
                    "query": "Как подписаться на уведомления о новых закупках по моему коду ОКПД2?",
                    "bot_reply": "В личном кабинете перейдите в 'Настройки профиля' -> 'Подписки и уведомления', добавьте интересующие коды ОКПД2 и выберите канал (email или уведомления на Портале).",
                    "op_reply": None,
                    "score": 5,
                    "comment": "Удобная инструкция бота, настроил рассылку за 1 минуту.",
                    "root_cause": RootCauseType.NONE,
                    "audit_summary": "Четкая пошаговая навигация от интеллектуального помощника.",
                    "is_system_issue": False,
                    "incident_type": None,
                    "incident_desc": None,
                },
            ]

            # Создание тикетов в базе данных
            for idx, sc in enumerate(scenarios):
                ticket_created_at = now - timedelta(
                    days=(idx % 8) + 1, hours=(idx * 2) % 20, minutes=idx * 7
                )
                ticket_closed_at = ticket_created_at + timedelta(
                    minutes=15 + idx * 3
                )

                t = TicketModel(
                    id=uuid6.uuid7(),
                    chat_id=chat.id,
                    line_id=lines[sc["line"]].id,
                    assigned_operator_id=sc["operator"].id
                    if sc["operator"]
                    else None,
                    priority=sc["priority"].value,
                    status=TicketStatus.RESOLVED.value,
                    created_at=ticket_created_at,
                    assigned_at=ticket_created_at + timedelta(seconds=45)
                    if sc["operator"]
                    else None,
                    opened_at=ticket_created_at + timedelta(seconds=60)
                    if sc["operator"]
                    else None,
                    closed_at=ticket_closed_at,
                )
                session.add(t)
                await session.flush()

                # Реплика клиента
                m_client = MessageModel(
                    id=uuid6.uuid7(),
                    ticket_id=t.id,
                    sender_type=MessageSenderType.CLIENT.value,
                    sender_id=supplier.id,
                    text=sc["query"],
                    moderation_status=MessageModerationStatus.PASSED.value,
                    created_at=ticket_created_at + timedelta(seconds=5),
                )
                session.add(m_client)

                # Ответ бота
                if sc["bot_reply"]:
                    m_bot = MessageModel(
                        id=uuid6.uuid7(),
                        ticket_id=t.id,
                        sender_type=MessageSenderType.BOT.value,
                        sender_id=None,
                        text=sc["bot_reply"],
                        moderation_status=MessageModerationStatus.PASSED.value,
                        created_at=ticket_created_at + timedelta(seconds=12),
                    )
                    session.add(m_bot)

                # Ответ оператора
                if sc["op_reply"] and sc["operator"]:
                    m_op = MessageModel(
                        id=uuid6.uuid7(),
                        ticket_id=t.id,
                        sender_type=MessageSenderType.OPERATOR.value,
                        sender_id=sc["operator"].id,
                        text=sc["op_reply"],
                        moderation_status=MessageModerationStatus.PASSED.value,
                        created_at=ticket_created_at + timedelta(minutes=2),
                    )
                    session.add(m_op)

                # Отзыв
                fb = TicketFeedbackModel(
                    id=uuid6.uuid7(),
                    ticket_id=t.id,
                    score=sc["score"],
                    comment=sc["comment"],
                    created_at=ticket_closed_at + timedelta(minutes=1),
                )
                session.add(fb)

                # Аудит
                audit = TicketAuditModel(
                    id=uuid6.uuid7(),
                    ticket_id=t.id,
                    politeness_score=5 if sc["score"] >= 4 else 3,
                    completeness_score=5 if sc["score"] >= 4 else 3,
                    root_cause=sc["root_cause"].value,
                    summary=sc["audit_summary"],
                    is_system_issue=sc["is_system_issue"],
                    created_at=ticket_closed_at + timedelta(minutes=2),
                )
                session.add(audit)

                # Системный инцидент
                if sc.get("incident_type") and sc.get("incident_desc"):
                    inc = SystemIncidentModel(
                        id=uuid6.uuid7(),
                        ticket_id=t.id,
                        incident_type=sc["incident_type"].value,
                        description=sc["incident_desc"],
                        status=IncidentStatus.OPEN.value,
                        created_at=ticket_closed_at,
                    )
                    session.add(inc)

            await session.flush()
            logger.info("Успешно создано 30 демонстрационных обращений.")

        # Сидирование активных тикетов для операторов L1, L2, L3 в АРМ (если их нет)
        stmt_active_count = select(func.count(TicketModel.id)).where(
            TicketModel.status.in_(
                [TicketStatus.ASSIGNED.value, TicketStatus.IN_PROGRESS.value]
            )
        )
        active_tickets_count = (await session.scalar(stmt_active_count)) or 0
        if active_tickets_count == 0:
            logger.info(
                "Генерация активных демонстрационных тикетов для АРМ Операторов L1, L2, L3..."
            )
            now_dt = datetime.now(settings.TIMEZONE)
            active_scenarios = [
                # L1 - Регламенты и каталог
                {
                    "line": "L1",
                    "operator": operators_by_line.get("L1"),
                    "status": TicketStatus.IN_PROGRESS.value,
                    "priority": TicketPriority.P1.value,
                    "query": "Не подгружается машиночитаемая доверенность (МЧД) из реестра ФНС. Пишет «Доверенность не найдена или не активна».",
                    "bot_reply": "Проверьте статус регистрации доверенности в распределенном реестре ФНС России.",
                    "summary": "Проблема синхронизации машиночитаемой доверенности (МЧД) версии 003 из распределенного реестра ФНС.",
                    "suggested_response": "Здравствуйте! Проверили статус доверенности в распределенном реестре ФНС. Для успешной привязки МЧД в личном кабинете Портала поставщиков убедитесь, что в профиле сотрудника указан СНИЛС, совпадающий с доверенностью.",
                },
                {
                    "line": "L1",
                    "operator": operators_by_line.get("L1"),
                    "status": TicketStatus.ASSIGNED.value,
                    "priority": TicketPriority.P2.value,
                    "query": "Ошибка импорта YML: тег <param name='Цвет'> не проходит валидацию на строке 48. Как загрузить оферты в каталог?",
                    "bot_reply": "Согласно регламенту ведения каталога СТЕ, тег <param> должен содержать обязательный атрибут unit или присутствовать в классификаторе.",
                    "summary": "Ошибка импорта каталога YML: тег <param name='Цвет'> не проходит валидацию на строке 48.",
                    "suggested_response": "Здравствуйте! В категории 'Канцтовары' параметр 'Цвет' требует выбора предопределенного значения из классификатора Портала.",
                },
                # L2 - ЭЦП и технические сбои
                {
                    "line": "L2",
                    "operator": operators_by_line.get("L2"),
                    "status": TicketStatus.IN_PROGRESS.value,
                    "priority": TicketPriority.P0.value,
                    "query": "Срочно! Идет котировочная сессия КС-9482, ошибка КриптоПро 0x80090016: Набор ключей не существует.",
                    "bot_reply": "Ошибка 0x80090016 указывает на невозможность считывания закрытого ключа. Переподключите USB-токен.",
                    "summary": "Критический сбой плагина КриптоПро 0x80090016 при подписании оферты котировочной сессии КС-9482.",
                    "suggested_response": "Здравствуйте! Переподключите USB-токен Рутокен, перезапустите службу 'КриптоПро CSP' и убедитесь, что установлены корневые сертификаты УЦ ФНС.",
                },
                {
                    "line": "L2",
                    "operator": operators_by_line.get("L2"),
                    "status": TicketStatus.ASSIGNED.value,
                    "priority": TicketPriority.P1.value,
                    "query": "Не могу прикрепить УПД к исполненному контракту №9923/26. Выдает ошибку формата по приказу 820.",
                    "bot_reply": "Портал принимает закрывающие документы строго в формате приказа ФНС России № ММВ-7-15/820@.",
                    "summary": "Ошибка валидации схемы XML универсального передаточного документа (УПД) по приказу ФНС 820.",
                    "suggested_response": "Здравствуйте! В вашем XML-файле УПД отсутствует обязательный реквизит ИГК. Добавьте тег <СвГосКонтр ИдентГосКонтр='...'/> в документ.",
                },
                # L3 - Разработчики и инфраструктура
                {
                    "line": "L3",
                    "operator": operators_by_line.get("L3"),
                    "status": TicketStatus.IN_PROGRESS.value,
                    "priority": TicketPriority.P0.value,
                    "query": "Критическая авария: отказ интеграционного шлюза ЕАИСТ / ЭДО Диадок, зависли 45 пакетов УПД.",
                    "bot_reply": "Служба мониторинга зафиксировала сетевой таймаут интеграционного шлюза.",
                    "summary": "Сбой интеграционного шлюза ЕАИСТ / ЭДО Диадок: задержка отправки пакетов УПД, таймауты SOAP-запросов.",
                    "suggested_response": "Здравствуйте! Инцидент передан дежурному инженеру DevOps. Ведется перезапуск интеграционного шлюза и дренаж очереди пакетов.",
                },
                {
                    "line": "L3",
                    "operator": operators_by_line.get("L3"),
                    "status": TicketStatus.ASSIGNED.value,
                    "priority": TicketPriority.P1.value,
                    "query": "Очередь асинхронного парсера YML-каталога зависла на 0% в Redis, таймаут воркеров.",
                    "bot_reply": "Запрос передан инженерам третьей линии для анализа логов очередей Celery/Redis.",
                    "summary": "Зависание очереди асинхронного парсера YML-каталога в Redis: превышение лимита памяти воркеров Celery.",
                    "suggested_response": "Здравствуйте! Добавили дополнительные поды-обработчики очереди парсинга, очередь начала рассасываться.",
                },
            ]

            for act in active_scenarios:
                t_act = TicketModel(
                    id=uuid6.uuid7(),
                    chat_id=chat.id,
                    line_id=lines[act["line"]].id,
                    assigned_operator_id=act["operator"].id
                    if act["operator"]
                    else None,
                    priority=act["priority"],
                    status=act["status"],
                    created_at=now_dt - timedelta(minutes=15),
                    assigned_at=now_dt - timedelta(minutes=10)
                    if act["operator"]
                    else None,
                    opened_at=now_dt - timedelta(minutes=8)
                    if act["status"] == TicketStatus.IN_PROGRESS.value
                    else None,
                )
                session.add(t_act)
                await session.flush()

                m_cl = MessageModel(
                    id=uuid6.uuid7(),
                    ticket_id=t_act.id,
                    sender_type=MessageSenderType.CLIENT.value,
                    sender_id=supplier.id,
                    text=act["query"],
                    moderation_status=MessageModerationStatus.PASSED.value,
                    created_at=now_dt - timedelta(minutes=14),
                )
                session.add(m_cl)

                if act.get("bot_reply"):
                    m_bt = MessageModel(
                        id=uuid6.uuid7(),
                        ticket_id=t_act.id,
                        sender_type=MessageSenderType.BOT.value,
                        sender_id=None,
                        text=act["bot_reply"],
                        moderation_status=MessageModerationStatus.PASSED.value,
                        created_at=now_dt - timedelta(minutes=13),
                    )
                    session.add(m_bt)

                # Добавление Copilot подсказки для оператора
                if act.get("summary"):
                    copilot_s = TicketCopilotSummaryModel(
                        id=uuid6.uuid7(),
                        ticket_id=t_act.id,
                        summary=act["summary"],
                        suggested_line_code=act["line"],
                        suggested_response=act.get("suggested_response"),
                        recommended_chunk_ids=[],
                        similar_resolved_tickets=[],
                        created_at=now_dt - timedelta(minutes=12),
                    )
                    session.add(copilot_s)

            await session.flush()
            logger.info(
                "Активные обращения для операторов L1, L2, L3 успешно созданы."
            )

        # Авто-сидирование базы знаний (kb_documents, kb_nodes)
        stmt_kb = select(func.count()).select_from(KbDocumentModel)
        kb_docs_count = (await session.scalars(stmt_kb)).first() or 0
        if kb_docs_count == 0:
            logger.info(
                "Таблица kb_documents пуста. Выполняется авто-сидирование базы знаний..."
            )
            sql_paths = [
                Path("storage/kb_data.sql"),
                Path("/app/storage/kb_data.sql"),
                Path("../artifacts_export/kb_data.sql"),
                Path("artifacts_export/kb_data.sql"),
            ]
            for sql_p in sql_paths:
                if sql_p.exists():
                    try:
                        sql_content = sql_p.read_text(encoding="utf-8")
                    except UnicodeDecodeError:
                        try:
                            sql_content = sql_p.read_text(encoding="utf-16")
                        except Exception:
                            sql_content = sql_p.read_text(
                                encoding="utf-8-sig", errors="ignore"
                            )

                    for statement in sql_content.split(";\n"):
                        clean_stmt = statement.strip()
                        if (
                            clean_stmt
                            and not clean_stmt.startswith("--")
                            and not clean_stmt.startswith("SET ")
                        ):
                            try:
                                await session.execute(text(clean_stmt))
                            except Exception as ex:
                                logger.debug(
                                    "Игнорирование ошибки вставки: %s", ex
                                )
                    await session.commit()
                    logger.info(
                        "Авто-сидирование базы знаний успешно завершено."
                    )
                    break

        await session.commit()
        logger.info("Сидирование демонстрационных данных успешно завершено.")


if __name__ == "__main__":
    asyncio.run(seed())
