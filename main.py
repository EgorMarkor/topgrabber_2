import re
import asyncio
import logging
import json
import os
import html
import csv
import copy
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from telethon import TelegramClient, events
from telethon.errors import (
    SessionPasswordNeededError,
    PhoneNumberInvalidError,
    PhoneCodeInvalidError,
    PhoneCodeExpiredError,
    FloodWaitError
)
from yookassa import Payment, Configuration
from pymorphy3 import MorphAnalyzer
import snowballstemmer
import uuid

# Настройка логирования
logging.basicConfig(level=logging.INFO)

API_TOKEN = "7930844421:AAFKC9cUVVdttJHa3fpnUSnAWgr8Wa6-wPE"
bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

# ЮKassa configuration
YOOKASSA_SHOP_ID = os.getenv("YOOKASSA_SHOP_ID")
YOOKASSA_TOKEN = os.getenv("YOOKASSA_TOKEN")
PRO_PRICE = "1990.00"
RETURN_URL = "https://t.me/TOPGrabber_bot"
if YOOKASSA_SHOP_ID and YOOKASSA_TOKEN:
    Configuration.account_id = YOOKASSA_SHOP_ID
    Configuration.secret_key = YOOKASSA_TOKEN

# Хранилище Telethon-клиентов и данных по пользователям
user_clients = {}  # runtime data: {user_id: {"client": TelegramClient,
# "phone": str, "phone_hash": str,
# "parsers": list,  # each item {'chats': list, 'keywords': list}
# "task": asyncio.Task}}

DATA_FILE = "user_data.json"
TEXT_FILE = "texts.json"

with open(TEXT_FILE, "r", encoding="utf-8") as f:
    TEXTS = json.load(f)

# Maximum number of chats allowed for PRO plan
CHAT_LIMIT = 5

# Morphological analysis utilities
morph = MorphAnalyzer()
stemmer_en = snowballstemmer.stemmer("english")

def normalize_word(word: str) -> str:
    """Return normalized form for keyword matching."""
    word = word.lower()
    if re.search("[а-яА-Я]", word):
        return morph.parse(word)[0].normal_form
    return stemmer_en.stemWord(word)

def t(key, **kwargs):
    text = TEXTS.get(key, key)
    if kwargs:
        try:
            text = text.format(**kwargs)
        except Exception:
            pass
    return text

def load_user_data():
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            for u in data.values():
                u.setdefault('subscription_expiry', 0)
                u.setdefault('recurring', False)
                u.setdefault('reminder3_sent', False)
                u.setdefault('reminder1_sent', False)
                u.setdefault('inactive_notified', False)
                u.setdefault('used_promos', [])
                for p in u.get('parsers', []):
                    p.setdefault('results', [])
                    p.setdefault('name', 'Без названия')
                    p.setdefault('api_id', '')
                    p.setdefault('api_hash', '')
            return data
        except Exception:
            logging.exception("Failed to load user data")
    return {}


def save_user_data(data):
    try:
        data_copy = copy.deepcopy(data)
        for u in data_copy.values():
            for p in u.get('parsers', []):
                p.pop('handler', None)
                p.pop('event', None)
        with open(DATA_FILE, "w", encoding="utf-8") as f:
            json.dump(data_copy, f, ensure_ascii=False, indent=2)
    except Exception:
        logging.exception("Failed to save user data")


user_data = load_user_data()  # persistent data: {str(user_id): {...}}

def create_pro_payment(user_id: int):
    if not (YOOKASSA_SHOP_ID and YOOKASSA_TOKEN):
        return None, None
    try:
        payment = Payment.create(
            {
                "amount": {"value": PRO_PRICE, "currency": "RUB"},
                "confirmation": {"type": "redirect", "return_url": RETURN_URL},
                "description": f"Подписка PRO для пользователя {user_id}",
            },
            str(uuid.uuid4()),
        )
        return payment.id, payment.confirmation.confirmation_url
    except Exception:
        logging.exception("Failed to create payment")
    return None, None

def check_pro_payment(payment_id: str):
    try:
        payment = Payment.find_one(payment_id)
        return payment.status
    except Exception:
        logging.exception("Failed to check payment")
    return None

def check_subscription(user_id: int):
    data = user_data.get(str(user_id))
    if not data:
        return
    exp = data.get('subscription_expiry', 0)
    now = int(datetime.utcnow().timestamp())
    days_left = (exp - now) // 86400
    if exp and days_left <= 0:
        if not data.get('inactive_notified'):
            # send last results and mark notified
            asyncio.create_task(send_all_results(user_id))
            asyncio.create_task(bot.send_message(user_id, t('subscription_inactive')))
            data['inactive_notified'] = True
            save_user_data(user_data)
        return
    if not data.get('recurring'):
        if days_left == 3 and not data.get('reminder3_sent'):
            asyncio.create_task(bot.send_message(user_id, t('subscription_reminder', days=3)))
            data['reminder3_sent'] = True
        elif days_left == 1 and not data.get('reminder1_sent'):
            asyncio.create_task(bot.send_message(user_id, t('subscription_reminder', days=1)))
            data['reminder1_sent'] = True
        if data.get('reminder3_sent') or data.get('reminder1_sent'):
            save_user_data(user_data)


# Текст для информационного сообщения
INFO_TEXT = (
    "TopGrabber – это сервис для автоматического поиска потенциальных клиентов"
    " в чатах Telegram. Вы можете настроить параметры поиска, указав нужные "
    "ключевые слова и ссылки на чаты, в которых хотите искать клиентов. Наш бот"
    " уведомит вас о найденных подходящих сообщениях.\n"
    "Инструкция к боту (https://dzen.ru/a/ZuHH1h_M5kqcam1A)\n"
    "Бот для получения сообщений (https://t.me/TOPGrabber_bot)\n\n"
    "Минимальное количество чатов - 5шт\n"
    "Цена:\n1 990₽/ 30 дней\n"
    "Купить 1 дополнительный чат:\n490₽/ 30 дней\n\n"
    "Copyright © 2024 TOPGrabberbot — AI-Парсер сообщений | "
    "ИП Антуфьев Б.В. (https://telegra.ph/Rekvizity-08-20-2) "
    "ОГРН 304770000133140 ИНН 026408848802 | "
    "Публичная оферта (https://telegra.ph/Publichnaya-oferta-09-11)"
)

# Текст для помощи
HELP_TEXT = (
    "Если возникли вопросы, изучите Инструкцию к боту "
    "(https://dzen.ru/a/ZuHH1h_M5kqcam1A) или напишите в поддержку: "
    "https://t.me/+PqfIWqHquts4YjQy"
)


async def start_monitor(user_id: int, parser: dict):
    info = user_clients.get(user_id)
    if not info:
        return
    client = info['client']
    chat_ids = parser.get('chats')
    keywords = parser.get('keywords')
    exclude = [normalize_word(w) for w in parser.get('exclude_keywords', [])]
    if not chat_ids or not keywords:
        return

    event_builder = events.NewMessage(chats=chat_ids)

    async def monitor(event, keywords=keywords, parser=parser):
        sender = await event.get_sender()
        if getattr(sender, 'bot', False):
            return
        text = event.raw_text or ''
        words = [normalize_word(w) for w in re.findall(r'\w+', text.lower())]
        for kw in keywords:
            if normalize_word(kw) in words and not any(e in words for e in exclude):
                chat = await event.get_chat()
                title = getattr(chat, 'title', str(event.chat_id))
                username = getattr(sender, 'username', None)
                sender_name = f"@{username}" if username else getattr(sender, 'first_name', 'Unknown')
                msg_time = event.message.date.strftime('%Y-%m-%d %H:%M:%S')
                link = 'Ссылка недоступна'
                chat_username = getattr(chat, 'username', None)
                if chat_username:
                    link = f"https://t.me/{chat_username}/{event.id}"
                preview = html.escape(text[:400])
                await bot.send_message(
                    user_id,
                    f"🔔 Найдено '{html.escape(kw)}' в чате '{html.escape(title)}'\n"
                    f"Username: {html.escape(sender_name)}\n"
                    f"DateTime: {msg_time}\n"
                    f"Link: {html.escape(link)}\n"
                    f"<pre>{preview}</pre>",
                    parse_mode="HTML",
                )
                parser.setdefault('results', []).append({
                    'keyword': kw,
                    'chat': title,
                    'sender': sender_name,
                    'datetime': msg_time,
                    'link': link,
                    'text': text,
                })
                save_user_data(user_data)
                break

    client.add_event_handler(monitor, event_builder)
    parser['handler'] = monitor
    parser['event'] = event_builder
    if not client.is_connected():
        await client.connect()
    if 'task' not in info:
        info['task'] = asyncio.create_task(client.run_until_disconnected())

def stop_monitor(user_id: int, parser: dict):
    info = user_clients.get(user_id)
    if not info:
        return
    handler = parser.get('handler')
    event = parser.get('event')
    if handler and event:
        try:
            info['client'].remove_event_handler(handler, event)
        except Exception:
            pass
    parser.pop('handler', None)
    parser.pop('event', None)

# Определение состояний FSM
class AuthStates(StatesGroup):
    waiting_api_id = State()
    waiting_api_hash = State()
    waiting_phone = State()
    waiting_code = State()
    waiting_password = State()
    waiting_chats = State()
    waiting_keywords = State()


class PromoStates(StatesGroup):
    waiting_promo = State()


class ParserStates(StatesGroup):
    waiting_name = State()
    waiting_chats = State()
    waiting_keywords = State()
    waiting_account_api_id = State()
    waiting_account_api_hash = State()


class EditParserStates(StatesGroup):
    waiting_chats = State()
    waiting_keywords = State()
    waiting_exclude = State()
    waiting_name = State()
    waiting_account_api_id = State()
    waiting_account_api_hash = State()


class ExpandProStates(StatesGroup):
    """States for expanding PRO plan."""
    waiting_chats = State()
    waiting_confirm = State()


@dp.message_handler(commands=["help"])
async def cmd_help(message: types.Message):
    """Отправить справочную информацию."""
    await message.answer("[[Как начать]]\n[[FAQ]]\nhttps://t.me/+PqfIWqHquts4YjQy")


@dp.message_handler(commands=['enable_recurring'])
async def enable_recurring(message: types.Message):
    data = user_data.setdefault(str(message.from_user.id), {})
    data['recurring'] = True
    save_user_data(user_data)
    await message.answer(t('recurring_enabled'))


@dp.message_handler(commands=['disable_recurring'])
async def disable_recurring(message: types.Message):
    data = user_data.setdefault(str(message.from_user.id), {})
    data['recurring'] = False
    save_user_data(user_data)
    await message.answer(t('recurring_disabled'))


@dp.message_handler(commands=['info'])
async def cmd_info(message: types.Message):
    data = user_data.get(str(message.from_user.id))
    if not data:
        await message.answer("Нет сохранённых данных.")
        return
    parsers = data.get('parsers') or []
    if not parsers:
        await message.answer("Парсеры не настроены.")
        return
    lines = []
    for idx, p in enumerate(parsers, 1):
        name = p.get('name', f'Парсер {idx}')
        chats = p.get('chats') or []
        kws = p.get('keywords') or []
        api_id = p.get('api_id', '')
        lines.append(
            f"#{idx} {name}\nAPI ID: {api_id}\nЧаты: {chats}\nКлючевые слова: {', '.join(kws)}"
        )
    await message.answer("\n\n".join(lines))


def main_menu_keyboard() -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=1)
    kb.add(
        types.InlineKeyboardButton(
            "🛠 Настройка и оплата парсеров", callback_data="menu_setup"
        ),
        types.InlineKeyboardButton(
            "📤 Экспорт результатов в таблицу", callback_data="menu_export"
        ),
        types.InlineKeyboardButton(
            "📚 Помощь и документация", callback_data="menu_help"
        ),
        types.InlineKeyboardButton(
            "🤝 Профиль и Партнёрская программа", callback_data="menu_profile"
        ),
    )
    return kb


def parser_settings_keyboard(idx: int) -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=1)
    kb.add(
        types.InlineKeyboardButton(
            "🛠 Изменить название", callback_data=f"edit_name_{idx}"
        ),
        types.InlineKeyboardButton(
            "📂 Изменить чаты", callback_data=f"edit_chats_{idx}"
        ),
        types.InlineKeyboardButton(
            "📂 Изменить слова", callback_data=f"edit_keywords_{idx}"
        ),
        types.InlineKeyboardButton(
            "📂 Изменить искл-слова", callback_data=f"edit_exclude_{idx}"
        ),
        types.InlineKeyboardButton(
            "🛠 Изменить аккаунт-парсер", callback_data=f"edit_account_{idx}"
        ),
        types.InlineKeyboardButton(
            "💳 Тариф и оплата", callback_data=f"edit_tariff_{idx}"
        ),
    )
    kb.add(
        types.InlineKeyboardButton("🔙 Назад", callback_data="back_main")
    )
    return kb


def parser_info_text(user_id: int, parser: dict, created: bool = False) -> str:
    idx = parser.get('id') or 1
    name = parser.get('name', f'Парсер_{idx}')
    chat_count = len(parser.get('chats', []))
    include_count = len(parser.get('keywords', []))
    exclude_count = len(parser.get('exclude_keywords', []))
    account_label = parser.get('api_id') or 'не привязан'
    data = user_data.get(str(user_id), {})
    plan_name = 'PRO'
    if data.get('subscription_expiry'):
        paid_to = datetime.utcfromtimestamp(data['subscription_expiry']).strftime('%Y-%m-%d')
    else:
        paid_to = '—'
    chat_limit = f'/{CHAT_LIMIT}' if plan_name == 'PRO' else ''
    status_emoji = '🟢' if parser.get('handler') else '⏸'
    status_text = 'Активен' if parser.get('handler') else 'Остановлен'
    if created:
        return t('parser_created', id=idx)
    return t(
        'parser_info',
        name=name,
        id=idx,
        chat_count=chat_count,
        chat_limit=chat_limit,
        include_count=include_count,
        exclude_count=exclude_count,
        account_label=account_label,
        plan_name=plan_name,
        paid_to=paid_to,
        status_emoji=status_emoji,
        status_text=status_text,
    )


@dp.message_handler(commands=['start'], state="*")
async def cmd_start(message: types.Message, state: FSMContext):
    await state.finish()
    check_subscription(message.from_user.id)
    data = user_data.setdefault(str(message.from_user.id), {})
    if not data.get('started'):
        data['started'] = True
        save_user_data(user_data)
    await message.answer(t('welcome'), reply_markup=main_menu_keyboard())


@dp.message_handler(commands=['menu'], state="*")
async def cmd_menu(message: types.Message, state: FSMContext):
    await state.finish()
    await message.answer(t('menu_main'), reply_markup=main_menu_keyboard())


@dp.message_handler(commands=['result'])
async def cmd_result(message: types.Message):
    """Отправить последнюю таблицу результатов."""
    await send_all_results(message.from_user.id)


@dp.message_handler(commands=['clear_result'])
async def cmd_clear_result(message: types.Message):
    """Отправить последнюю таблицу и очистить её."""
    await send_all_results(message.from_user.id)
    data = user_data.get(str(message.from_user.id))
    if data:
        for parser in data.get('parsers', []):
            parser['results'] = []
        save_user_data(user_data)


@dp.message_handler(commands=['delete_card'])
async def cmd_delete_card(message: types.Message):
    """Удалить сохранённые данные карты пользователя."""
    data = user_data.get(str(message.from_user.id))
    if data:
        data.pop('card', None)
        save_user_data(user_data)
    await message.answer("Данные карты удалены.")


@dp.message_handler(commands=['delete_parser'])
async def cmd_delete_parser(message: types.Message):
    """Начать процесс удаления парсера."""
    data = user_data.get(str(message.from_user.id))
    if not data:
        await message.answer("Данные не найдены.")
        return
    parsers = [
        (idx, p)
        for idx, p in enumerate(data.get('parsers', []))
        if not p.get('paid')
    ]
    if not parsers:
        await message.answer("Нет доступных парсеров для удаления.")
        return
    kb = types.InlineKeyboardMarkup(row_width=1)
    for idx, p in parsers:
        name = p.get('name', f'Парсер {idx+1}')
        kb.add(types.InlineKeyboardButton(name, callback_data=f'delp_select_{idx}'))
    kb.add(types.InlineKeyboardButton("🔙 Назад", callback_data="back_main"))
    await message.answer("Выберите парсер для удаления:", reply_markup=kb)


@dp.callback_query_handler(lambda c: c.data.startswith('delp_select_'))
async def cb_delp_select(call: types.CallbackQuery):
    idx = int(call.data.split('_')[2])
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        types.InlineKeyboardButton("Нет", callback_data='delp_cancel'),
        types.InlineKeyboardButton("Да", callback_data=f'delp_confirm_{idx}')
    )
    await call.message.answer("Удалить парсер?", reply_markup=kb)
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == 'delp_cancel')
async def cb_delp_cancel(call: types.CallbackQuery):
    await call.message.answer("Удаление отменено.")
    await call.message.answer(t('menu_main'), reply_markup=main_menu_keyboard())
    await call.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('delp_confirm_'))
async def cb_delp_confirm(call: types.CallbackQuery):
    idx = int(call.data.split('_')[2])
    user_id = call.from_user.id
    await send_parser_results(user_id, idx)
    data = user_data.get(str(user_id))
    if data and 0 <= idx < len(data.get('parsers', [])):
        parser = data['parsers'][idx]
        if parser.get('paid'):
            await call.message.answer("Оплаченный парсер нельзя удалить.")
            await call.answer()
            return
        stop_monitor(user_id, parser)
        data['parsers'].pop(idx)
        save_user_data(user_data)
        await call.message.answer("Парсер удалён.")
    await call.message.answer(t('menu_main'), reply_markup=main_menu_keyboard())
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == 'back_main')
async def cb_back_main(call: types.CallbackQuery):
    await call.message.answer(t('menu_main'), reply_markup=main_menu_keyboard())
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == 'menu_setup')
async def cb_menu_setup(call: types.CallbackQuery):
    kb = types.InlineKeyboardMarkup(row_width=1)
    kb.add(
        types.InlineKeyboardButton("🚀 Новый парсер", callback_data="setup_new"),
        types.InlineKeyboardButton("✏️ Мои парсеры", callback_data="setup_list"),
        types.InlineKeyboardButton("💳 Оплата", callback_data="setup_pay"),
        types.InlineKeyboardButton("🔙 Назад", callback_data="back_main"),
    )
    await call.message.answer(t('menu_setup'), reply_markup=kb)
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == 'setup_new')
async def cb_setup_new(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    await cmd_add_parser(call.message, state)


@dp.callback_query_handler(lambda c: c.data == 'setup_list')
async def cb_setup_list(call: types.CallbackQuery):
    await cb_active_parsers(call)


@dp.callback_query_handler(lambda c: c.data == 'setup_pay')
async def cb_setup_pay(call: types.CallbackQuery, state: FSMContext):
    """Show list of parsers for payment actions."""
    data = user_data.get(str(call.from_user.id))
    if not data or not data.get('parsers'):
        await call.message.answer("Парсеры не настроены.")
        await call.answer()
        return
    kb = types.InlineKeyboardMarkup(row_width=1)
    for idx, p in enumerate(data.get('parsers'), 1):
        name = p.get('name', f'Парсер {idx}')
        kb.add(types.InlineKeyboardButton(name, callback_data=f'pay_select_{idx-1}'))
    kb.add(types.InlineKeyboardButton("🔙 Назад", callback_data="menu_setup"))
    await call.message.answer("Выберите парсер:", reply_markup=kb)
    await call.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('pay_select_'))
async def cb_pay_select(call: types.CallbackQuery):
    """Show payment options for selected parser."""
    idx = int(call.data.split('_')[2])
    kb = types.InlineKeyboardMarkup(row_width=1)
    kb.add(
        types.InlineKeyboardButton("Продлить подписку", callback_data=f'pay_renew_{idx}'),
        types.InlineKeyboardButton("Расширить Pro", callback_data=f'pay_expand_{idx}'),
        types.InlineKeyboardButton("Перейти на Infinity", callback_data=f'pay_infinity_{idx}'),
        types.InlineKeyboardButton("🔙 Назад", callback_data='setup_pay'),
    )
    await call.message.answer("Выберите действие:", reply_markup=kb)
    await call.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('pay_renew_'))
async def cb_pay_renew(call: types.CallbackQuery, state: FSMContext):
    """Renew PRO subscription."""
    await _process_tariff_pro(call.message, state)
    await call.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('pay_expand_'))
async def cb_pay_expand(call: types.CallbackQuery, state: FSMContext):
    """Start process to expand PRO plan chats."""
    idx = int(call.data.split('_')[2])
    await state.update_data(expand_idx=idx)
    await call.message.answer("Сколько чатов вам нужно?")
    await ExpandProStates.waiting_chats.set()
    await call.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('pay_infinity_'))
async def cb_pay_infinity(call: types.CallbackQuery):
    """Inform about INFINITY plan."""
    await call.message.answer(
        "Тариф INFINITY — 149 990 ₽/мес. Неограниченные чаты и слова, персональный аккаунт-менеджер.\n"
        "Для подключения напишите @TopGrabberSupport"
    )
    await call.answer()


@dp.message_handler(state=ExpandProStates.waiting_chats)
async def expand_pro_chats(message: types.Message, state: FSMContext):
    """Handle number of chats for PRO expansion."""
    text = message.text.strip()
    if not text.isdigit() or int(text) <= 0:
        await message.answer("Введите количество чатов числом")
        return
    chats = int(text)
    price = 1990 + max(0, chats - 5) * 490
    await state.update_data(chats=chats, price=price)
    kb = types.InlineKeyboardMarkup(row_width=1)
    kb.add(
        types.InlineKeyboardButton("✅ Подтвердить", callback_data='expand_confirm'),
        types.InlineKeyboardButton("❌ Отмена", callback_data='expand_cancel'),
        types.InlineKeyboardButton("🔙 Назад", callback_data='expand_back'),
    )
    await message.answer(
        f"Стоимость тарифа PRO на {chats} чатов составит {price} ₽/мес. Подтвердить оплату?",
        reply_markup=kb,
    )
    await ExpandProStates.waiting_confirm.set()


@dp.callback_query_handler(lambda c: c.data == 'expand_confirm', state=ExpandProStates.waiting_confirm)
async def cb_expand_confirm(call: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    price = data.get('price')
    chats = data.get('chats')
    await call.message.answer(
        f"Для расширения до {chats} чатов необходимо оплатить {price} ₽. \nСвяжитесь с поддержкой: @TopGrabberSupport",
    )
    await state.finish()
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == 'expand_cancel', state=ExpandProStates.waiting_confirm)
async def cb_expand_cancel(call: types.CallbackQuery, state: FSMContext):
    await call.message.answer("Действие отменено.")
    await state.finish()
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == 'expand_back', state=ExpandProStates.waiting_confirm)
async def cb_expand_back(call: types.CallbackQuery, state: FSMContext):
    await call.message.answer("Сколько чатов вам нужно?")
    await ExpandProStates.waiting_chats.set()
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == 'menu_export')
async def cb_menu_export(call: types.CallbackQuery):
    kb = types.InlineKeyboardMarkup(row_width=1)
    kb.add(
        types.InlineKeyboardButton("📤 Общий результат", callback_data="export_all"),
        types.InlineKeyboardButton("📂 Выбрать парсер", callback_data="export_choose"),
        types.InlineKeyboardButton("🔔 Моментальные уведомления", callback_data="export_alert"),
        types.InlineKeyboardButton("🔙 Назад", callback_data="back_main"),
    )
    await call.message.answer(t('menu_export'), reply_markup=kb)
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == 'export_all')
async def cb_export_all(call: types.CallbackQuery):
    await send_all_results(call.from_user.id)
    await call.message.answer(t('menu_main'), reply_markup=main_menu_keyboard())
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == 'export_choose')
async def cb_export_choose(call: types.CallbackQuery):
    await cb_result(call)


@dp.callback_query_handler(lambda c: c.data == 'export_alert')
async def cb_export_alert(call: types.CallbackQuery):
    link = f"https://t.me/TopGrabberAlertBot?start={call.from_user.id}"
    await call.message.answer(
        "Подключите алерт-бот — и новые лиды будут прилетать прямо в Telegram с текстом запроса, ссылкой на сообщение и автором.\n"
        f"{link}"
    )
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == 'menu_help')
async def cb_menu_help(call: types.CallbackQuery):
    kb = types.InlineKeyboardMarkup(row_width=1)
    kb.add(
        types.InlineKeyboardButton("❓ Как начать", callback_data="help_start"),
        types.InlineKeyboardButton("🧑‍💻 Поддержка", callback_data="help_support"),
        types.InlineKeyboardButton("📄 О нас", callback_data="help_about"),
        types.InlineKeyboardButton("🚀 Новый парсер", callback_data="setup_new"),
        types.InlineKeyboardButton("🔙 Назад", callback_data="back_main"),
    )
    await call.message.answer(t('menu_help'), reply_markup=kb)
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == 'help_start')
async def cb_help_start(call: types.CallbackQuery):
    await cmd_help(call.message)
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == 'help_support')
async def cb_help_support(call: types.CallbackQuery):
    await call.message.answer("Свяжитесь с поддержкой: https://t.me/TopGrabberSupport")
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == 'help_about')
async def cb_help_about(call: types.CallbackQuery):
    await cb_info(call)


@dp.callback_query_handler(lambda c: c.data == 'menu_profile')
async def cb_menu_profile(call: types.CallbackQuery):
    data = user_data.get(str(call.from_user.id), {})
    now = int(datetime.utcnow().timestamp())
    if data.get('subscription_expiry', 0) > now:
        plan_name = 'PRO'
        paid_to = datetime.utcfromtimestamp(data['subscription_expiry']).strftime('%Y-%m-%d')
    else:
        plan_name = 'Нет активной подписки'
        paid_to = '—'
    rec_status = '🔁' if data.get('recurring') else ''
    text = t(
        'menu_profile',
        user_id=call.from_user.id,
        username=call.from_user.username or '',
        plan_name=plan_name,
        paid_to=paid_to,
        rec_status=rec_status,
        promo_code=data.get('promo_code', 'N/A'),
        ref_count=data.get('ref_count', 0),
        ref_active_users=data.get('ref_active_users', 0),
        ref_month_income=data.get('ref_month_income', 0),
        ref_total=data.get('ref_total', 0),
        ref_balance=data.get('ref_balance', 0),
    )
    kb = types.InlineKeyboardMarkup(row_width=1)
    kb.add(
        types.InlineKeyboardButton(
            "💳 Оплата с партнерского баланса", callback_data="profile_paybalance"
        ),
        types.InlineKeyboardButton(
            "💸 Вывести средства", callback_data="profile_withdraw"
        ),
        types.InlineKeyboardButton(
            "⛔️ Удалить карту", callback_data="profile_delete_card"
        ),
        types.InlineKeyboardButton("🔙 Назад", callback_data="back_main"),
    )
    await call.message.answer(text, reply_markup=kb)
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == 'profile_paybalance')
async def cb_profile_paybalance(call: types.CallbackQuery):
    await call.message.answer("Функция оплаты с партнёрского баланса пока недоступна.")
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == 'profile_withdraw')
async def cb_profile_withdraw(call: types.CallbackQuery):
    await call.message.answer("Функция вывода средств пока недоступна.")
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == 'profile_delete_card')
async def cb_profile_delete_card(call: types.CallbackQuery):
    await call.message.answer("Данные карты удалены.")
    await call.answer()


async def _process_tariff_pro(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    data = user_data.get(str(user_id))
    if data and data.get('subscription_expiry', 0) > int(datetime.utcnow().timestamp()):
        await cmd_add_parser(message, state)
        return

    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    markup.add("Пропустить")
    await message.answer(
        "Введите промокод или нажмите 'Пропустить'.",
        reply_markup=markup,
    )
    await PromoStates.waiting_promo.set()


@dp.callback_query_handler(lambda c: c.data == 'tariff_pro')
async def cb_tariff_pro(call: types.CallbackQuery, state: FSMContext):
    await _process_tariff_pro(call.message, state)
    await call.answer()


@dp.message_handler(commands=['tariff_pro'])
async def cmd_tariff_pro(message: types.Message, state: FSMContext):
    await _process_tariff_pro(message, state)


@dp.message_handler(state=PromoStates.waiting_promo)
async def promo_entered(message: types.Message, state: FSMContext):
    code = message.text.strip().upper()
    user_id = message.from_user.id
    data = user_data.setdefault(str(user_id), {})
    used_promos = data.setdefault('used_promos', [])
    if code in used_promos:
        await message.answer(
            t('promo_already_used'),
            reply_markup=types.ReplyKeyboardRemove(),
        )
        await state.finish()
        return
    if code == 'DEMO':
        expiry = int((datetime.utcnow() + timedelta(days=7)).timestamp())
        data['subscription_expiry'] = expiry
        used_promos.append(code)
        save_user_data(user_data)
        await message.answer(
            "Промокод принят! Вам предоставлено 7 дней бесплатного тарифа PRO.",
            reply_markup=types.ReplyKeyboardRemove(),
        )
        await state.finish()
        await message.answer("Используйте /login для авторизации.")
        return

    await message.answer(
        "Перейдите по ссылке для оплаты тарифа PRO.",
        reply_markup=types.ReplyKeyboardRemove(),
    )
    payment_id, url = create_pro_payment(user_id)
    if not payment_id:
        await message.answer("Не удалось создать платёж. Попробуйте позже.")
    else:
        user_data.setdefault(str(user_id), {})['payment_id'] = payment_id
        save_user_data(user_data)
        kb = types.InlineKeyboardMarkup()
        kb.add(types.InlineKeyboardButton("Оплатить", url=url))
        await message.answer(
            "Для активации тарифа оплатите по ссылке и затем используйте /check_payment.",
            reply_markup=kb,
        )
    await state.finish()


@dp.callback_query_handler(lambda c: c.data == 'result')
async def cb_result(call: types.CallbackQuery):
    data = user_data.get(str(call.from_user.id))
    if not data or not data.get('parsers'):
        await call.message.answer("Парсеры не настроены.")
        await call.answer()
        return
    kb = types.InlineKeyboardMarkup(row_width=1)
    for idx, p in enumerate(data.get('parsers'), 1):
        name = p.get('name', f'Парсер {idx}')
        kb.add(types.InlineKeyboardButton(name, callback_data=f"csv_{idx}"))
    kb.add(types.InlineKeyboardButton("🔙 Назад", callback_data="back_main"))
    await call.message.answer("Выберите парсер для получения CSV:", reply_markup=kb)
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == 'help_info')
async def cb_help(call: types.CallbackQuery):
    await call.message.answer(HELP_TEXT)
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == 'info')
async def cb_info(call: types.CallbackQuery):
    await call.message.answer(INFO_TEXT)
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == 'active_parsers')
async def cb_active_parsers(call: types.CallbackQuery):
    data = user_data.get(str(call.from_user.id))
    if not data or not data.get('parsers'):
        await call.message.answer("Парсеры не настроены.")
        await call.answer()
        return
    kb = types.InlineKeyboardMarkup(row_width=1)
    for idx, p in enumerate(data.get('parsers'), 1):
        name = p.get('name', f'Парсер {idx}')
        kb.add(types.InlineKeyboardButton(name, callback_data=f"edit_{idx}"))
    kb.add(types.InlineKeyboardButton("🔙 Назад", callback_data="back_main"))
    await call.message.answer("Активные парсеры:", reply_markup=kb)
    await call.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('csv_'))
async def cb_send_csv(call: types.CallbackQuery):
    idx = int(call.data.split('_')[1]) - 1
    user_id = call.from_user.id
    check_subscription(user_id)
    data = user_data.get(str(user_id))
    if not data:
        await call.message.answer("Данные не найдены.")
        await call.answer()
        return
    parsers = data.get('parsers', [])
    if idx < 0 or idx >= len(parsers):
        await call.message.answer("Парсер не найден.")
        await call.answer()
        return
    parser = parsers[idx]
    results = parser.get('results', [])
    if not results:
        await call.message.answer("Нет сохранённых результатов для этого парсера.")
        await call.answer()
        return
    path = f"results_{user_id}_{idx+1}.csv"
    with open(path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["keyword", "chat", "sender", "datetime", "link", "text"])
        for r in results:
            writer.writerow([
                r.get('keyword', ''),
                r.get('chat', ''),
                r.get('sender', ''),
                r.get('datetime', ''),
                r.get('link', ''),
                r.get('text', '').replace('\n', ' '),
            ])
    await bot.send_document(user_id, types.InputFile(path))
    os.remove(path)
    await call.message.answer(t('menu_main'), reply_markup=main_menu_keyboard())
    await call.answer()


@dp.message_handler(commands=['export'])
async def cmd_export(message: types.Message):
    check_subscription(message.from_user.id)
    await send_all_results(message.from_user.id)


@dp.message_handler(commands=['check_payment'])
async def cmd_check_payment(message: types.Message):
    user_id = message.from_user.id
    data = user_data.get(str(user_id))
    payment_id = data.get('payment_id') if data else None
    if not payment_id:
        await message.answer("Платёж не найден. Используйте /tariff_pro для оформления.")
        return
    status = check_pro_payment(payment_id)
    if status == 'succeeded':
        expiry = int((datetime.utcnow() + timedelta(days=30)).timestamp())
        data['subscription_expiry'] = expiry
        data.pop('payment_id', None)
        save_user_data(user_data)
        await message.answer("Оплата подтверждена! Используйте /login для авторизации.")
    else:
        await message.answer(f"Платёж не завершён. Текущий статус: {status}")

async def send_all_results(user_id: int):
    data = user_data.get(str(user_id))
    if not data:
        return
    rows = []
    for parser in data.get('parsers', []):
        for r in parser.get('results', []):
            rows.append([
                r.get('keyword', ''),
                r.get('chat', ''),
                r.get('sender', ''),
                r.get('datetime', ''),
                r.get('link', ''),
                r.get('text', '').replace('\n', ' '),
            ])
    if not rows:
        await bot.send_message(user_id, t('no_results'))
        return
    path = f"results_{user_id}_all.csv"
    with open(path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["keyword", "chat", "sender", "datetime", "link", "text"])
        writer.writerows(rows)
    await bot.send_document(user_id, types.InputFile(path), caption=t('csv_export_ready'))
    os.remove(path)


async def send_parser_results(user_id: int, idx: int):
    """Отправить CSV с результатами выбранного парсера."""
    data = user_data.get(str(user_id))
    if not data:
        return
    parsers = data.get('parsers', [])
    if idx < 0 or idx >= len(parsers):
        return
    parser = parsers[idx]
    results = parser.get('results', [])
    if not results:
        await bot.send_message(user_id, t('no_results'))
        return
    path = f"results_{user_id}_{idx+1}.csv"
    with open(path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["keyword", "chat", "sender", "datetime", "link", "text"])
        for r in results:
            writer.writerow([
                r.get('keyword', ''),
                r.get('chat', ''),
                r.get('sender', ''),
                r.get('datetime', ''),
                r.get('link', ''),
                r.get('text', '').replace('\n', ' '),
            ])
    await bot.send_document(user_id, types.InputFile(path))
    os.remove(path)


# Handler for callbacks like "edit_1" which allow choosing what to edit for a
# specific parser. More specific callbacks such as ``edit_chats_X`` and
# ``edit_keywords_X`` are handled separately below, so here we ensure that the
# data matches exactly the ``edit_<number>`` pattern.
@dp.callback_query_handler(lambda c: c.data.startswith('edit_') and c.data.count('_') == 1)
async def cb_edit_parser(call: types.CallbackQuery):
    idx = int(call.data.split('_')[1]) - 1
    parser = user_data.get(str(call.from_user.id), {}).get('parsers', [])[idx]
    text = parser_info_text(call.from_user.id, parser)
    await call.message.answer(
        text, reply_markup=parser_settings_keyboard(idx + 1)
    )
    await call.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('edit_chats_'), state='*')
async def cb_edit_chats(call: types.CallbackQuery, state: FSMContext):
    idx = int(call.data.split('_')[2]) - 1
    await state.update_data(edit_idx=idx)
    await call.message.answer(
        "Введите новые ссылки на чаты (через пробел или запятую):"
    )
    await EditParserStates.waiting_chats.set()
    await call.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('edit_keywords_'), state='*')
async def cb_edit_keywords(call: types.CallbackQuery, state: FSMContext):
    idx = int(call.data.split('_')[2]) - 1
    await state.update_data(edit_idx=idx)
    await call.message.answer(
        "Введите новые ключевые слова (через запятую):"
    )
    await EditParserStates.waiting_keywords.set()
    await call.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('edit_exclude_'), state='*')
async def cb_edit_exclude(call: types.CallbackQuery, state: FSMContext):
    idx = int(call.data.split('_')[2]) - 1
    await state.update_data(edit_idx=idx)
    await call.message.answer(
        "Введите новые исключающие слова (через запятую):"
    )
    await EditParserStates.waiting_exclude.set()
    await call.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('edit_name_'), state='*')
async def cb_edit_name(call: types.CallbackQuery, state: FSMContext):
    idx = int(call.data.split('_')[2]) - 1
    await state.update_data(edit_idx=idx)
    await call.message.answer("Введите новое название парсера:")
    await EditParserStates.waiting_name.set()
    await call.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('edit_account_'), state='*')
async def cb_edit_account(call: types.CallbackQuery, state: FSMContext):
    idx = int(call.data.split('_')[2]) - 1
    await state.update_data(edit_idx=idx)
    await call.message.answer("Введите api_id аккаунта-парсера:")
    await EditParserStates.waiting_account_api_id.set()
    await call.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('edit_tariff_'))
async def cb_edit_tariff(call: types.CallbackQuery, state: FSMContext):
    await cb_tariff_pro(call, state)


@dp.message_handler(state=ParserStates.waiting_name)
async def get_parser_name(message: types.Message, state: FSMContext):
    name = message.text.strip()
    if not name:
        await message.answer("Название не может быть пустым. Попробуйте ещё раз:")
        return
    await state.update_data(parser_name=name)
    await message.answer(
        "Укажите ссылки на чаты или каналы (через пробел или запятую):"
    )
    await ParserStates.waiting_chats.set()

@dp.message_handler(commands=['addparser'], state='*')
async def cmd_add_parser(message: types.Message, state: FSMContext):
    await state.finish()
    user_id = message.from_user.id
    info = user_clients.get(user_id)
    if not info:
        saved = user_data.get(str(user_id))
        if not saved:
            await message.answer("Сначала авторизуйтесь командой /login")
            return
        api_id = saved.get('api_id')
        api_hash = saved.get('api_hash')
        if not api_id or not api_hash:
            await message.answer("Сначала авторизуйтесь командой /login")
            return
        session_name = f"session_{user_id}"
        client = TelegramClient(session_name, api_id, api_hash)
        await client.connect()
        if not await client.is_user_authorized():
            await message.answer("Сессия найдена, но требует входа. Используйте /login")
            return
        user_clients[user_id] = {
            'client': client,
            'phone': saved.get('phone'),
            'phone_hash': '',
            'parsers': saved.get('parsers', [])
        }
        for p in user_clients[user_id]['parsers']:
            await start_monitor(user_id, p)

    parsers = user_data.setdefault(str(user_id), {}).setdefault('parsers', [])
    parser_id = len(parsers) + 1
    parser = {
        'id': parser_id,
        'name': f'Парсер_{parser_id}',
        'chats': [],
        'keywords': [],
        'exclude_keywords': [],
        'api_id': '',
        'api_hash': '',
        'results': [],
    }
    parsers.append(parser)
    info = user_clients.setdefault(user_id, info or {})
    # Avoid duplicating the parser in runtime storage; ensure both
    # user_clients and persistent user_data reference the same list.
    info['parsers'] = parsers
    save_user_data(user_data)
    await message.answer(
        parser_info_text(user_id, parser, created=True),
        reply_markup=parser_settings_keyboard(parser_id),
    )

@dp.message_handler(commands=['login'], state="*")
async def start_login(message: types.Message, state: FSMContext):
    await state.finish()
    user_id = message.from_user.id
    check_subscription(user_id)
    data = user_data.get(str(user_id))
    now = int(datetime.utcnow().timestamp())
    if not data or data.get('subscription_expiry', 0) <= now:
        await message.answer("Сначала оплатите тариф командой /tariff_pro")
        return
    existing = user_clients.pop(user_id, None)
    if existing:
        try:
            if 'task' in existing:
                existing['task'].cancel()
            await existing['client'].disconnect()
        except Exception:
            logging.exception("Failed to disconnect previous session")
    saved = user_data.get(str(user_id))
    if saved:
        api_id = saved.get('api_id')
        api_hash = saved.get('api_hash')
        if api_id and api_hash:
            session_name = f"session_{user_id}"
            client = TelegramClient(session_name, api_id, api_hash)
            await client.connect()
            if await client.is_user_authorized():
                user_clients[user_id] = {
                    'client': client,
                    'phone': saved.get('phone'),
                    'phone_hash': '',
                    'parsers': saved.get('parsers', [])
                }
                for p in user_clients[user_id]['parsers']:
                    await start_monitor(user_id, p)
                if user_clients[user_id]['parsers']:
                    await message.answer("✅ Найдены сохранённые парсеры. Мониторинг запущен.")
                    return
        await message.answer("👋 Сессия найдена, но требуется повторный вход. Введите свой *api_id* Telegram:", parse_mode="Markdown")
    else:
        await message.answer(
            "👋 Привет! Для начала работы введите свой *api_id* Telegram:",
            parse_mode="Markdown"
        )
    await AuthStates.waiting_api_id.set()

@dp.message_handler(state=AuthStates.waiting_api_id)
async def get_api_id(message: types.Message, state: FSMContext):
    text = message.text.strip()
    if not text.isdigit():
        await message.answer("❗ *api_id* должен быть числом. Попробуйте ещё раз:", parse_mode="Markdown")
        return
    await state.update_data(api_id=int(text))
    await message.answer("Отлично. Введите *api_hash* вашего приложения:", parse_mode="Markdown")
    await AuthStates.waiting_api_hash.set()

@dp.message_handler(state=AuthStates.waiting_api_hash)
async def get_api_hash(message: types.Message, state: FSMContext):
    api_hash = message.text.strip()
    if not api_hash or len(api_hash) < 5:
        await message.answer("❗ *api_hash* должен быть корректной строкой. Попробуйте ещё раз:", parse_mode="Markdown")
        return
    await state.update_data(api_hash=api_hash)
    await message.answer(
        "Теперь введите номер телефона Telegram (с международным кодом, например +79991234567):"
    )
    await AuthStates.waiting_phone.set()

@dp.message_handler(state=AuthStates.waiting_phone)
async def get_phone(message: types.Message, state: FSMContext):
    phone = message.text.strip()
    data = await state.get_data()
    api_id = data.get('api_id')
    api_hash = data.get('api_hash')
    user_id = message.from_user.id
    session_name = f"session_{user_id}"

    try:
        client = TelegramClient(session_name, api_id, api_hash)
        await client.connect()
        result = await client.send_code_request(phone)
        phone_hash = result.phone_code_hash
    except PhoneNumberInvalidError:
        await message.answer("❌ Неверный номер телефона. Введите заново:")
        return
    except FloodWaitError as e:
        await message.answer(f"⚠️ Telegram просит подождать {e.seconds} секунд перед следующей попыткой.")
        await state.finish()
        return
    except Exception as e:
        logging.exception(e)
        await message.answer(f"⚠️ Ошибка при запросе кода: {e}. Попробуйте /start.")
        await state.finish()
        return

    user_clients[user_id] = {
        'client': client,
        'phone': phone,
        'phone_hash': phone_hash,
        'parsers': []
    }
    saved = user_data.get(str(user_id), {})
    saved.update({
        'api_id': api_id,
        'api_hash': api_hash,
        'phone': phone,
    })
    saved.setdefault('parsers', [])
    saved.setdefault('subscription_expiry', 0)
    saved.setdefault('recurring', False)
    saved.setdefault('reminder3_sent', False)
    saved.setdefault('reminder1_sent', False)
    saved.setdefault('inactive_notified', False)
    user_data[str(user_id)] = saved
    save_user_data(user_data)
    await state.update_data(phone=phone)

    await message.answer(
        "📱 Код отправлен! Пожалуйста, введите код, *вставляя любые символы* (например, пробелы или дефисы) между цифрами."
        " Я автоматически уберу лишние символы.",
        parse_mode="Markdown"
    )
    await AuthStates.waiting_code.set()

@dp.message_handler(state=AuthStates.waiting_code)
async def get_code(message: types.Message, state: FSMContext):
    raw = message.text.strip()
    code = re.sub(r'\D', '', raw)
    user_id = message.from_user.id
    client_info = user_clients.get(user_id)

    if not client_info:
        await message.answer("⚠️ Сессия не найдена. Начните сначала /start.")
        await state.finish()
        return

    client = client_info['client']
    phone = client_info['phone']
    phone_hash = client_info['phone_hash']

    try:
        await client.sign_in(phone=phone, code=code, phone_code_hash=phone_hash)
    except PhoneCodeInvalidError:
        await message.answer("❌ Неверный код. Попробуйте снова, вставив символы между цифрами:")
        return
    except PhoneCodeExpiredError:
        await message.answer(
            "❌ Код истёк. Пожалуйста, перезапустите авторизацию командой /start и запросите новый код."
        )
        await state.finish()
        return
    except SessionPasswordNeededError:
        await message.answer("🔒 Ваш аккаунт защищён паролем. Введите пароль:")
        await AuthStates.waiting_password.set()
        return
    except Exception as e:
        logging.exception(e)
        await message.answer(f"⚠️ Ошибка при входе: {e}. Попробуйте /start.")
        await state.finish()
        return

    await message.answer(
        "✅ Вы успешно вошли! Теперь укажите *ссылки* на чаты или каналы для мониторинга."
        " Примеры: `https://t.me/username` или `t.me/username`. Через пробел или запятую:",
        parse_mode="Markdown"
    )
    await ParserStates.waiting_chats.set()

@dp.message_handler(state=AuthStates.waiting_password)
async def get_password(message: types.Message, state: FSMContext):
    password = message.text.strip()
    user_id = message.from_user.id
    client_info = user_clients.get(user_id)

    if not client_info:
        await message.answer("⚠️ Сессия не найдена. Начните сначала /start.")
        await state.finish()
        return

    client = client_info['client']
    try:
        await client.sign_in(password=password)
    except Exception as e:
        logging.exception(e)
        await message.answer("❌ Неверный пароль. Попробуйте ещё раз:")
        return

    await message.answer(
        "✅ Пароль принят! Теперь укажите *ссылки* на чаты или каналы для мониторинга (через пробел или запятую):",
        parse_mode="Markdown"
    )
    await ParserStates.waiting_chats.set()

async def _process_chats(message: types.Message, state: FSMContext, next_state):
    text = message.text.strip().replace(',', ' ')
    parts = [p for p in text.split() if p]
    user_id = message.from_user.id
    client = user_clients[user_id]['client']
    chat_ids = []

    for part in parts:
        try:
            entity = await client.get_entity(part)
            chat_ids.append(entity.id)
        except Exception:
            if part.lstrip("-").isdigit():
                chat_ids.append(int(part))
            else:
                await message.answer(
                    "⚠️ Чат не найден. Проверьте доступность в аккаунте и корректность ссылки.")
                return None

    if not chat_ids:
        await message.answer("⚠️ Пустой список. Введите хотя бы одну ссылку или ID:")
        return None

    if len(chat_ids) > CHAT_LIMIT:
        await message.answer(f"⚠️ Можно указать не более {CHAT_LIMIT} чатов.")
        return None

    await state.update_data(chat_ids=chat_ids)
    await message.answer("Отлично! Теперь введите ключевые слова для мониторинга (через запятую):")
    await next_state.set()
    return chat_ids


@dp.message_handler(state=AuthStates.waiting_chats)
async def get_chats_auth(message: types.Message, state: FSMContext):
    await _process_chats(message, state, AuthStates.waiting_keywords)


@dp.message_handler(state=ParserStates.waiting_chats)
async def get_chats_parser(message: types.Message, state: FSMContext):
    await _process_chats(message, state, ParserStates.waiting_keywords)

async def _process_keywords(message: types.Message, state: FSMContext):
    keywords = [w.strip().lower() for w in message.text.split(',') if w.strip()]
    if not keywords:
        await message.answer("⚠️ Список пуст. Введите хотя бы одно слово:")
        return

    user_id = message.from_user.id
    data = await state.get_data()
    chat_ids = data.get('chat_ids')
    if not chat_ids:
        await message.answer("⚠️ Сначала укажите чаты.")
        return

    await state.update_data(keywords=keywords)
    await message.answer("Укажите api_id аккаунта-парсера:")
    await ParserStates.waiting_account_api_id.set()


@dp.message_handler(state=AuthStates.waiting_keywords)
async def get_keywords_auth(message: types.Message, state: FSMContext):
    await _process_keywords(message, state)


@dp.message_handler(state=ParserStates.waiting_keywords)
async def get_keywords_parser(message: types.Message, state: FSMContext):
    await _process_keywords(message, state)


@dp.message_handler(state=ParserStates.waiting_account_api_id)
async def get_parser_api_id(message: types.Message, state: FSMContext):
    text = message.text.strip()
    if not text.isdigit():
        await message.answer("⚠️ api_id должен быть числом. Попробуйте ещё раз:")
        return
    await state.update_data(api_id=int(text))
    await message.answer("Введите api_hash аккаунта-парсера:")
    await ParserStates.waiting_account_api_hash.set()


@dp.message_handler(state=ParserStates.waiting_account_api_hash)
async def get_parser_api_hash(message: types.Message, state: FSMContext):
    api_hash = message.text.strip()
    data = await state.get_data()
    api_id = data.get('api_id')
    user_id = message.from_user.id
    chat_ids = data.get('chat_ids')
    keywords = data.get('keywords')
    name = data.get(
        'parser_name',
        f"Парсер {len(user_data.get(str(user_id), {}).get('parsers', [])) + 1}"
    )
    parser = {
        'name': name,
        'chats': chat_ids,
        'keywords': keywords,
        'api_id': api_id,
        'api_hash': api_hash,
        'results': [],
    }
    info = user_clients.setdefault(user_id, {})
    info.setdefault('parsers', []).append(parser)
    if str(user_id) in user_data:
        user_data[str(user_id)].setdefault('parsers', []).append(parser)
        save_user_data(user_data)

    await start_monitor(user_id, parser)

    await message.answer("✅ Мониторинг запущен! Я уведомлю вас о совпадениях.")
    await state.finish()


@dp.message_handler(state=EditParserStates.waiting_chats)
async def edit_chats_handler(message: types.Message, state: FSMContext):
    data = await state.get_data()
    idx = data.get('edit_idx')
    text = message.text.strip().replace(',', ' ')
    parts = [p for p in text.split() if p]
    user_id = message.from_user.id
    client = user_clients[user_id]['client']
    chat_ids = []
    for part in parts:
        try:
            entity = await client.get_entity(part)
            chat_ids.append(entity.id)
        except Exception:
            if part.lstrip("-").isdigit():
                chat_ids.append(int(part))
            else:
                await message.answer("⚠️ Чат не найден. Проверьте доступность и корректность ссылки.")
                return
    if not chat_ids:
        await message.answer("⚠️ Пустой список. Введите хотя бы одну ссылку или ID:")
        return

    if len(chat_ids) > CHAT_LIMIT:
        await message.answer(f"⚠️ Можно указать не более {CHAT_LIMIT} чатов.")
        return
    parser = user_data[str(user_id)]['parsers'][idx]
    stop_monitor(user_id, parser)
    parser['chats'] = chat_ids
    save_user_data(user_data)
    await start_monitor(user_id, parser)
    await state.finish()
    await message.answer("✅ Чаты обновлены.")


@dp.message_handler(state=EditParserStates.waiting_keywords)
async def edit_keywords_handler(message: types.Message, state: FSMContext):
    data = await state.get_data()
    idx = data.get('edit_idx')
    keywords = [w.strip().lower() for w in message.text.split(',') if w.strip()]
    if not keywords:
        await message.answer("⚠️ Список пуст. Введите хотя бы одно слово:")
        return
    user_id = message.from_user.id
    parser = user_data[str(user_id)]['parsers'][idx]
    stop_monitor(user_id, parser)
    parser['keywords'] = keywords
    save_user_data(user_data)
    await start_monitor(user_id, parser)
    await state.finish()
    await message.answer("✅ Ключевые слова обновлены.")


@dp.message_handler(state=EditParserStates.waiting_exclude)
async def edit_exclude_handler(message: types.Message, state: FSMContext):
    data = await state.get_data()
    idx = data.get('edit_idx')
    words = [w.strip().lower() for w in message.text.split(',') if w.strip()]
    user_id = message.from_user.id
    parser = user_data[str(user_id)]['parsers'][idx]
    stop_monitor(user_id, parser)
    parser['exclude_keywords'] = words
    save_user_data(user_data)
    await start_monitor(user_id, parser)
    await state.finish()
    await message.answer("✅ Исключающие слова обновлены.")


@dp.message_handler(state=EditParserStates.waiting_name)
async def edit_name_handler(message: types.Message, state: FSMContext):
    data = await state.get_data()
    idx = data.get('edit_idx')
    new_name = message.text.strip()
    user_id = message.from_user.id
    parser = user_data[str(user_id)]['parsers'][idx]
    parser['name'] = new_name
    save_user_data(user_data)
    await state.finish()
    await message.answer("✅ Название обновлено.")


@dp.message_handler(state=EditParserStates.waiting_account_api_id)
async def edit_account_api_id(message: types.Message, state: FSMContext):
    text = message.text.strip()
    if not text.isdigit():
        await message.answer("⚠️ api_id должен быть числом. Попробуйте ещё раз:")
        return
    await state.update_data(api_id=int(text))
    await message.answer("Введите новый api_hash аккаунта-парсера:")
    await EditParserStates.waiting_account_api_hash.set()


@dp.message_handler(state=EditParserStates.waiting_account_api_hash)
async def edit_account_api_hash(message: types.Message, state: FSMContext):
    data = await state.get_data()
    idx = data.get('edit_idx')
    api_id = data.get('api_id')
    api_hash = message.text.strip()
    user_id = message.from_user.id
    parser = user_data[str(user_id)]['parsers'][idx]
    stop_monitor(user_id, parser)
    parser['api_id'] = api_id
    parser['api_hash'] = api_hash
    save_user_data(user_data)
    await start_monitor(user_id, parser)
    await state.finish()
    await message.answer("✅ Аккаунт обновлён.")

if __name__ == '__main__':
    print("Bot is starting...")
    executor.start_polling(dp, skip_updates=True)
