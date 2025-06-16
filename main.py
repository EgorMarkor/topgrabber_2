import re
import asyncio
import logging
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
user_clients = {}  # {user_id: {"client": TelegramClient, "phone": str, "phone_hash": str, "chats": list, "keywords": list}}

# Определение состояний FSM
class AuthStates(StatesGroup):
    waiting_api_id = State()
    waiting_api_hash = State()
    waiting_phone = State()
    waiting_code = State()
    waiting_password = State()
    waiting_chats = State()
    waiting_keywords = State()

@dp.message_handler(commands=['start', 'login'], state="*")
async def start_login(message: types.Message, state: FSMContext):
    await state.finish()
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
            # Пытаемся как ID
            if part.isdigit():
                chat_ids.append(int(part))
            else:
                await message.answer(
                    f"⚠️ '{part}' не является корректной ссылкой или ID. Повторите ввод:" )
                return

    if not chat_ids:
        await message.answer("⚠️ Пустой список. Введите хотя бы одну ссылку или ID:")
        return

    user_clients[user_id]['chats'] = chat_ids
    await state.update_data(chat_ids=chat_ids)

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
    client = user_clients[user_id]['client']
    chat_ids = user_clients[user_id]['chats']

    @client.on(events.NewMessage(chats=chat_ids))
    async def monitor(event):
        text = event.raw_text or ''
        lowered = text.lower()
        for kw in keywords:
            if kw in lowered:
                chat = await event.get_chat()
                title = getattr(chat, 'title', str(event.chat_id))
                preview = text[:400]
                await bot.send_message(
                    user_id,
                    f"🔔 Найдено '{kw}' в чате '{title}':\n```{preview}```",
                    parse_mode="Markdown"
                )
                break

    if not client.is_connected():
        await client.connect()
    asyncio.create_task(client.run_until_disconnected())

    await message.answer("✅ Мониторинг запущен! Я уведомлю вас о совпадениях.")
    await state.finish()

if __name__ == '__main__':
    print("Bot is starting...")
    executor.start_polling(dp, skip_updates=True)