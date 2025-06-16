import re
import asyncio
import logging
import json
import os
import html
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

# Настройка логирования
logging.basicConfig(level=logging.INFO)

API_TOKEN = "7930844421:AAFKC9cUVVdttJHa3fpnUSnAWgr8Wa6-wPE"
bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

# Хранилище Telethon-клиентов и данных по пользователям
user_clients = {}  # runtime data: {user_id: {"client": TelegramClient, "phone": str, "phone_hash": str, "chats": list, "keywords": list}}

DATA_FILE = "user_data.json"

def load_user_data():
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            logging.exception("Failed to load user data")
    return {}


def save_user_data(data):
    try:
        with open(DATA_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception:
        logging.exception("Failed to save user data")


user_data = load_user_data()  # persistent data: {str(user_id): {...}}


async def start_monitor(user_id: int):
    info = user_clients.get(user_id)
    if not info:
        return
    client = info['client']
    chat_ids = info.get('chats')
    keywords = info.get('keywords')
    if not chat_ids or not keywords:
        return

    async def monitor(event):
        sender = await event.get_sender()
        if getattr(sender, 'bot', False):
            return
        text = event.raw_text or ''
        words = re.findall(r'\w+', text.lower())
        for kw in keywords:
            if kw.lower() in words:
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
                break

    client.add_event_handler(monitor, events.NewMessage(chats=chat_ids))
    if not client.is_connected():
        await client.connect()
    asyncio.create_task(client.run_until_disconnected())

# Определение состояний FSM
class AuthStates(StatesGroup):
    waiting_api_id = State()
    waiting_api_hash = State()
    waiting_phone = State()
    waiting_code = State()
    waiting_password = State()
    waiting_chats = State()
    waiting_keywords = State()


@dp.message_handler(commands=['help'])
async def cmd_help(message: types.Message):
    await message.answer(
        "Данный бот отслеживает ключевые слова в указанных чатах.\n"
        "/start - начать или восстановить работу\n"
        "/login - принудительно начать авторизацию заново\n"
        "/info - показать текущие сохранённые настройки"
    )


@dp.message_handler(commands=['info'])
async def cmd_info(message: types.Message):
    data = user_data.get(str(message.from_user.id))
    if not data:
        await message.answer("Нет сохранённых данных.")
        return
    chats = data.get('chats') or []
    keywords = data.get('keywords') or []
    await message.answer(
        f"Чаты: {chats}\nКлючевые слова: {', '.join(keywords)}"
    )

@dp.message_handler(commands=['start', 'login'], state="*")
async def start_login(message: types.Message, state: FSMContext):
    await state.finish()
    user_id = message.from_user.id
    saved = user_data.get(str(user_id))
    if saved:
        session_name = f"session_{user_id}"
        client = TelegramClient(session_name, saved['api_id'], saved['api_hash'])
        await client.connect()
        if await client.is_user_authorized():
            user_clients[user_id] = {
                'client': client,
                'phone': saved.get('phone'),
                'phone_hash': '',
                'chats': saved.get('chats'),
                'keywords': saved.get('keywords')
            }
            if saved.get('chats') and saved.get('keywords'):
                await start_monitor(user_id)
                await message.answer("✅ Найдены сохранённые настройки. Мониторинг запущен.")
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
        'chats': None,
        'keywords': None
    }
    user_data[str(user_id)] = {
        'api_id': api_id,
        'api_hash': api_hash,
        'phone': phone,
        'chats': None,
        'keywords': None
    }
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
    await AuthStates.waiting_chats.set()

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
    await AuthStates.waiting_chats.set()

@dp.message_handler(state=AuthStates.waiting_chats)
async def get_chats(message: types.Message, state: FSMContext):
    text = message.text.strip().replace(',', ' ')
    parts = [p for p in text.split() if p]
    user_id = message.from_user.id
    client = user_clients[user_id]['client']
    chat_ids = []

    for part in parts:
        try:
            # Попытка получить сущность по ссылке или username
            entity = await client.get_entity(part)
            chat_ids.append(entity.id)
        except Exception:
            if part.isdigit():
                chat_ids.append(int(part))
            else:
                await message.answer(
                    "⚠️ Чат не найден. Проверьте доступность в аккаунте и корректность ссылки." )
                return

    if not chat_ids:
        await message.answer("⚠️ Пустой список. Введите хотя бы одну ссылку или ID:")
        return

    user_clients[user_id]['chats'] = chat_ids
    await state.update_data(chat_ids=chat_ids)
    if str(user_id) in user_data:
        user_data[str(user_id)]['chats'] = chat_ids
        save_user_data(user_data)

    await message.answer("Отлично! Теперь введите ключевые слова для мониторинга (через запятую):")
    await AuthStates.waiting_keywords.set()

@dp.message_handler(state=AuthStates.waiting_keywords)
async def get_keywords(message: types.Message, state: FSMContext):
    keywords = [w.strip().lower() for w in message.text.split(',') if w.strip()]
    if not keywords:
        await message.answer("⚠️ Список пуст. Введите хотя бы одно слово:")
        return

    user_id = message.from_user.id
    user_clients[user_id]['keywords'] = keywords
    if str(user_id) in user_data:
        user_data[str(user_id)]['keywords'] = keywords
        save_user_data(user_data)

    await start_monitor(user_id)

    await message.answer("✅ Мониторинг запущен! Я уведомлю вас о совпадениях.")
    await state.finish()

if __name__ == '__main__':
    print("Bot is starting...")
    executor.start_polling(dp, skip_updates=True)
