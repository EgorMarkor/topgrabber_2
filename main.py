import re
import os
import json
import asyncio
import logging
from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
import snowballstemmer
import pymorphy3
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

# Анализаторы слов для разных языков
morph_ru = pymorphy3.MorphAnalyzer()
stemmer_en = snowballstemmer.stemmer('english')

# Телеграм-бот
bot = Bot(token="7930844421:AAFKC9cUVVdttJHa3fpnUSnAWgr8Wa6-wPE")
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

# Хранилище активных Telethon клиентов (не сериализуется)
user_clients = {}  # {user_id_str: TelegramClient}

# Файл для хранения пользовательских настроек
DATA_FILE = 'users.json'
try:
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            user_data = json.load(f)
    else:
        user_data = {}
except (json.JSONDecodeError, ValueError):
    logging.warning(f"Не удалось разобрать {DATA_FILE}, начинаем с пустых данных")
    user_data = {}

# Вспомогательная функция для сохранения (исключаем client)
def save_data():
    serializable = {}
    for uid, data in user_data.items():
        filtered = {k: v for k, v in data.items() if k != 'client'}
        serializable[uid] = filtered
    with open(DATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(serializable, f, ensure_ascii=False, indent=2)

# Нормализация слов для разных языков
def normalize_word(word: str) -> str:
    word = word.lower()
    if re.search('[а-яА-Я]', word):
        # Русские слова приводим к начальной форме
        return morph_ru.normal_forms(word)[0]
    # Для остальных языков используем английский стеммер
    return stemmer_en.stemWord(word)

# Возвращает список нормализованных слов из текста
def normalized_words(text: str):
    words = re.findall(r"\b\w+\b", text.lower())
    return [normalize_word(w) for w in words]

# FSM-состояния
class AuthStates(StatesGroup):
    waiting_api_id = State()
    waiting_api_hash = State()
    waiting_phone = State()
    waiting_code = State()
    waiting_password = State()
    waiting_add_chats = State()
    waiting_add_keywords = State()
    waiting_promo_code = State()
    waiting_promo_input = State()

# Клавиатура главного меню
def main_menu_keyboard():
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        types.InlineKeyboardButton('Помощь', callback_data='help'),
        types.InlineKeyboardButton('Информация', callback_data='info'),
        types.InlineKeyboardButton('Добавить чаты', callback_data='add_chats'),
        types.InlineKeyboardButton('Добавить слова', callback_data='add_keywords'),
        types.InlineKeyboardButton('Результаты', callback_data='results'),
        types.InlineKeyboardButton('Тариф Pro', callback_data='pro')
    )
    return kb

# ------------------------- /start -------------------------
@dp.message_handler(commands=['start'], state='*')
async def cmd_start(message: types.Message, state: FSMContext):
    await state.finish()
    text = (
        "👋 Привет! Добро пожаловать в TopGrabber — ваш инструмент для поиска горячих и теплых клиентов.\n\n"
        "📖 Инструкция: укажите в меню чаты и ключевые слова — бот уведомит вас о новых сообщениях."
    )
    await message.answer(text, reply_markup=main_menu_keyboard())

# ------------------------- Оформление Pro (с привязкой) -------------------------
@dp.callback_query_handler(lambda c: c.data == 'pro')
async def callback_pro(c: types.CallbackQuery):
    uid = str(c.from_user.id)
    if uid not in user_data or 'api_id' not in user_data[uid]:
        await c.message.answer(
            "Для оформления тарифа Pro нужно сначала привязать Telegram-аккаунт.\n"
            "Введите ваш api_id (число):"
        )
        await AuthStates.waiting_api_id.set()
    else:
        await c.message.answer(
            "Тариф Pro: 1990₽ за 30 дней (5 чатов).\nУ вас есть промокод? (да/нет)"
        )
        await AuthStates.waiting_promo_code.set()
    await c.answer()

# ------------------------- Привязка аккаунта -------------------------
@dp.message_handler(state=AuthStates.waiting_api_id)
async def process_api_id(message: types.Message, state: FSMContext):
    if not message.text.isdigit():
        await message.reply("❗ api_id должен быть числом. Попробуйте ещё раз:")
        return
    await state.update_data(api_id=int(message.text))
    await message.answer("Введите api_hash вашего приложения:")
    await AuthStates.waiting_api_hash.set()

@dp.message_handler(state=AuthStates.waiting_api_hash)
async def process_api_hash(message: types.Message, state: FSMContext):
    api_hash = message.text.strip()
    if not api_hash:
        await message.reply("❗ api_hash не может быть пустым. Попробуйте ещё раз:")
        return
    await state.update_data(api_hash=api_hash)
    await message.answer(
        "Теперь введите номер телефона (с кодом страны, например +79991234567):"
    )
    await AuthStates.waiting_phone.set()

@dp.message_handler(state=AuthStates.waiting_phone)
async def process_phone(message: types.Message, state: FSMContext):
    data = await state.get_data()
    api_id = data['api_id']
    api_hash = data['api_hash']
    phone = message.text.strip()
    session = f"session_{message.from_user.id}"
    client = TelegramClient(session, api_id, api_hash)
    try:
        await client.connect()
        res = await client.send_code_request(phone)
    except PhoneNumberInvalidError:
        await message.reply("❗ Неверный номер телефона. Попробуйте ещё раз:")
        return
    except FloodWaitError as e:
        await message.reply(f"⚠️ Подождите {e.seconds} секунд.")
        await state.finish()
        return
    except Exception as e:
        logging.exception(e)
        await message.reply("⚠️ Ошибка при отправке кода. Попробуйте позже.")
        await state.finish()
        return
    uid = str(message.from_user.id)
    # Сохраняем данные без client
    user_data.setdefault(uid, {}).update({
        'api_id': api_id,
        'api_hash': api_hash,
        'phone': phone,
        'phone_hash': res.phone_code_hash
    })
    # Client хранится отдельно
    user_clients[uid] = client
    save_data()
    await message.answer("📱 Код отправлен! Введите код (только цифры):")
    await AuthStates.waiting_code.set()

@dp.message_handler(state=AuthStates.waiting_code)
async def process_code(message: types.Message, state: FSMContext):
    uid = str(message.from_user.id)
    code = re.sub(r"\D", '', message.text)
    client = user_clients.get(uid)
    usr = user_data.get(uid, {})
    try:
        await client.sign_in(code=code, phone=usr['phone'], phone_code_hash=usr['phone_hash'])
    except PhoneCodeInvalidError:
        await message.reply("❌ Неверный код. Попробуйте ещё раз:")
        return
    except PhoneCodeExpiredError:
        await message.reply("❌ Код истёк. Начните заново /start.")
        await state.finish()
        return
    except SessionPasswordNeededError:
        await message.answer("🔒 Аккаунт защищён паролем. Введите пароль:")
        await AuthStates.waiting_password.set()
        return
    await message.answer("✅ Аккаунт привязан!", reply_markup=main_menu_keyboard())
    await setup_client(uid)
    await state.finish()

@dp.message_handler(state=AuthStates.waiting_password)
async def process_password(message: types.Message, state: FSMContext):
    uid = str(message.from_user.id)
    client = user_clients.get(uid)
    try:
        await client.sign_in(password=message.text.strip())
    except Exception:
        await message.reply("❌ Неверный пароль. Попробуйте ещё раз:")
        return
    await message.answer("✅ Пароль принят! Аккаунт привязан.", reply_markup=main_menu_keyboard())
    await setup_client(uid)
    await state.finish()

    
# ------------------------- CALLBACKS -------------------------
@dp.callback_query_handler(lambda c: c.data == 'help')
async def callback_help(c: types.CallbackQuery):
    text = (
        "Если возникли вопросы, смотрите Инструкцию или пишите в поддержку: https://t.me/ihxY6kUFLe1kNmQy"
    )
    await c.message.answer(text)
    await c.answer()

@dp.callback_query_handler(lambda c: c.data == 'info')
async def callback_info(c: types.CallbackQuery):
    text = (
        "TopGrabber — сервис поиска клиентов в Telegram."
        " Минимум 5 чатов, цена 1990₽/30д, доп. чат 490₽/30д.\n"
        "© 2025 TOPGrabberbot | ИП Антюфьев Б.В."
    )
    await c.message.answer(text)
    await c.answer()

@dp.callback_query_handler(lambda c: c.data == 'add_chats')
async def callback_add_chats(c: types.CallbackQuery):
    await c.message.answer(
        "Введите ссылки или ID чатов через пробел или запятую:\n"
        "Пример: @username -1001234567890 https://t.me/example"
    )
    await AuthStates.waiting_add_chats.set()
    await c.answer()

@dp.callback_query_handler(lambda c: c.data == 'add_keywords')
async def callback_add_keywords(c: types.CallbackQuery):
    await c.message.answer("Введите ключевые слова через запятую (напр.: продажа, маркетинг):")
    await AuthStates.waiting_add_keywords.set()
    await c.answer()

@dp.callback_query_handler(lambda c: c.data == 'results')
async def callback_results(c: types.CallbackQuery):
    user_id = str(c.from_user.id)
    matches = user_data.get(user_id, {}).get('matches', [])
    if not matches:
        await c.message.answer("ℹ️ Результатов пока нет.")
    else:
        import pandas as pd
        df = pd.DataFrame(matches)
        path = f"results_{user_id}.xlsx"
        df.to_excel(path, index=False)
        await c.message.answer_document(
            types.InputFile(path), caption="Ссылка на AlertBot: @alert_bot"
        )
    await c.answer()

# ------------------------- Добавление чатов -------------------------
@dp.message_handler(state=AuthStates.waiting_add_chats)
async def process_add_chats(message: types.Message, state: FSMContext):
    parts = re.split(r"[\s,]+", message.text.strip())
    new_ids = []
    client = user_data[str(message.from_user.id)].get('client')
    for part in parts:
        if not part: continue
        if part.isdigit() or (part.startswith('-') and part[1:].isdigit()):
            new_ids.append(int(part))
        else:
            try:
                ent = await client.get_entity(part)
                new_ids.append(ent.id)
            except:
                await message.reply(
                    "⚠️ Чат не найден. Проверьте, что он есть в вашем аккаунте или ссылка корректна, затем повторите ввод:"
                )
                return
    usr = user_data[str(message.from_user.id)]
    usr.setdefault('chats', [])
    for cid in new_ids:
        if cid not in usr['chats']:
            usr['chats'].append(cid)
    save_data()
    await message.reply("✅ Чаты добавлены.")
    await state.finish()

# ------------------------- Добавление ключевых слов -------------------------
@dp.message_handler(state=AuthStates.waiting_add_keywords)
async def process_add_keywords(message: types.Message, state: FSMContext):
    kws = [w.strip().lower() for w in message.text.split(',') if w.strip()]
    if not kws:
        await message.reply("⚠️ Список пуст. Повторите ввод:")
        return
    usr = user_data[str(message.from_user.id)]
    usr.setdefault('keywords', [])
    for kw in kws:
        if kw not in usr['keywords']:
            usr['keywords'].append(kw)
    save_data()
    await message.reply("✅ Слова сохранены.")
    await state.finish()

# ------------------------- Обработка промокода -------------------------
@dp.message_handler(state=AuthStates.waiting_promo_code)
async def process_promo_code(message: types.Message, state: FSMContext):
    ans = message.text.strip().lower()
    if ans in ('да', 'есть'):
        await message.answer("Введите промокод:")
        await AuthStates.waiting_promo_input.set()
    else:
        await message.answer("Отправляю счет без промокода.")
        await state.finish()

@dp.message_handler(state=AuthStates.waiting_promo_input)
async def process_promo_input(message: types.Message, state: FSMContext):
    code = message.text.strip()
    if code == 'PROMO2025':
        await message.answer("Промокод принят! Счет отправлен со скидкой.")
    else:
        await message.answer("Неверный промокод. Счет без скидки отправлен.")
    await state.finish()

# ------------------------- TELETHON MONITORING -------------------------
async def setup_client(user_id_str: str):
    data = user_data[user_id_str]
    session = f"session_{user_id_str}"
    client = TelegramClient(session, data['api_id'], data['api_hash'])
    await client.start()
    data['client'] = client
    user_clients[user_id_str] = client
    chats = data.get('chats', [])
    keywords = data.get('keywords', [])
    @client.on(events.NewMessage(chats=chats))
    async def monitor(event):
        sender = await event.get_sender()
        if sender and getattr(sender, 'bot', False):
            return
        text = event.raw_text or ''
        words = normalized_words(text)
        for kw in keywords:
            if normalize_word(kw) in words:
                chat = await event.get_chat()
                if hasattr(chat, 'username') and chat.username:
                    msg_link = f"https://t.me/{chat.username}/{event.message.id}"
                else:
                    msg_link = f"tg://openmessage?user_id={event.message.peer_id.user_id or chat.id}"
                uname = sender.username or str(sender.id)
                dt = event.message.date.astimezone().strftime("%Y-%m-%d %H:%M:%S")
                rec = {'Message Link': msg_link, 'Username': uname, 'DateTime': dt, 'Promo Word': kw, 'Message': text}
                data.setdefault('matches', []).append(rec)
                save_data()
                await bot.send_message(int(user_id_str), f"🔔 *Найдено '{kw}'*", parse_mode="Markdown")
                detail = (
                    f"*Message Link:* {msg_link}\n"
                    f"*Username:* {uname}\n"
                    f"*DateTime:* {dt}\n"
                    f"*Promo Word:* {kw}\n"
                    f"*Message:* {text[:400]}"
                )
                await bot.send_message(int(user_id_str), detail, parse_mode="Markdown")
                break
    asyncio.create_task(client.run_until_disconnected())
    

async def on_startup(dp):
    # Восстанавливаем Telethon-клиентов для всех привязанных пользователей
    for uid, data in user_data.items():
        if all(k in data for k in ('api_id', 'api_hash', 'chats', 'keywords')):
            await setup_client(uid)

if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)
