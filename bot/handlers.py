import asyncio
import os
import csv
import html
from datetime import datetime, timedelta
from aiogram import types
from aiogram.dispatcher import FSMContext
from telethon import TelegramClient
from telethon.errors import (
    SessionPasswordNeededError,
    PhoneNumberInvalidError,
    PhoneCodeInvalidError,
    PhoneCodeExpiredError,
    FloodWaitError,
)

from .config import dp, bot
from .states import PromoStates, ParserStates, EditParserStates, ExpandProStates, TopUpStates, PartnerTransferStates
from .utils import ui_send_new, ui_from_callback_edit, safe_send_message, get_or_create_user_entry
from .data import user_data, get_user_data_entry, save_user_data
from .text_utils import t, INFO_TEXT, HELP_TEXT, normalize_word
from .payments import create_topup_payment, wait_topup_and_credit, create_pro_payment, wait_payment_and_activate, check_payment
from .billing import total_daily_cost, predict_block_date, _round2, check_subscription
from .keyboards import main_menu_keyboard, parser_settings_keyboard
from .parsers import pause_parser, resume_parser, parser_info_text, start_monitor, send_all_results, send_parser_results, user_clients

@dp.message_handler(commands=["help"])
async def cmd_help(message: types.Message):
    """Отправить справочную информацию."""
    await ui_send_new(message.from_user.id, HELP_TEXT)


@dp.message_handler(commands=['enable_recurring'])
async def enable_recurring(message: types.Message):
    data = get_user_data_entry(message.from_user.id)
    data['recurring'] = True
    save_user_data(user_data)
    await ui_send_new(message.from_user.id, t('recurring_enabled'))


@dp.message_handler(commands=['disable_recurring'])
async def disable_recurring(message: types.Message):
    data = get_user_data_entry(message.from_user.id)
    data['recurring'] = False
    save_user_data(user_data)
    await ui_send_new(message.from_user.id, t('recurring_disabled'))


@dp.message_handler(commands=['info'])
async def cmd_info(message: types.Message):
    data = user_data.get(str(message.from_user.id))
    if not data:
        await ui_send_new(message.from_user.id, "Нет сохранённых данных.")
        return
    parsers = data.get('parsers') or []
    if not parsers:
        await ui_send_new(message.from_user.id, "Парсеры не настроены.")
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
    await ui_send_new(message.from_user.id, "\n\n".join(lines))


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
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        types.InlineKeyboardButton("▶️ Запустить", callback_data=f"parser_resume_{idx}"),
        types.InlineKeyboardButton("⏸ Пауза", callback_data=f"parser_pause_{idx}"),
    )
    kb.add(
        types.InlineKeyboardButton("🛠 Изменить название", callback_data=f"edit_name_{idx}"),
        types.InlineKeyboardButton("📂 Изменить чаты", callback_data=f"edit_chats_{idx}"),
    )
    kb.add(
        types.InlineKeyboardButton("📂 Изменить слова", callback_data=f"edit_keywords_{idx}"),
        types.InlineKeyboardButton("📂 Изменить искл-слова", callback_data=f"edit_exclude_{idx}"),
    )
    kb.add(
        types.InlineKeyboardButton("🗑 Удалить (только на паузе)", callback_data=f"parser_delete_{idx}"),
    )
    kb.add(types.InlineKeyboardButton("🔙 Назад", callback_data="back_main"))
    return kb


class TopUpStates(StatesGroup):
    waiting_amount = State()


class PartnerTransferStates(StatesGroup):
    waiting_amount = State()


@dp.callback_query_handler(lambda c: c.data.startswith('parser_pause_'))
async def cb_parser_pause(call: types.CallbackQuery):
    idx = int(call.data.split('_')[2]) - 1
    user_id = call.from_user.id
    data = user_data.get(str(user_id), {})
    if not data or idx < 0 or idx >= len(data.get('parsers', [])):
        await call.answer("Не найдено", show_alert=True)
        return
    p = data['parsers'][idx]
    if p.get('status') == 'paused':
        await call.answer("Уже на паузе")
        return
    pause_parser(user_id, p)
    await ui_from_callback_edit(call, "⏸ Парсер поставлен на паузу.")


@dp.callback_query_handler(lambda c: c.data.startswith('parser_resume_'))
async def cb_parser_resume(call: types.CallbackQuery):
    idx = int(call.data.split('_')[2]) - 1
    user_id = call.from_user.id
    data = user_data.get(str(user_id), {})
    if not data or idx < 0 or idx >= len(data.get('parsers', [])):
        await call.answer("Не найдено", show_alert=True)
        return
    # Проверим баланс хотя бы на 1 день
    per_day = total_daily_cost(user_id)  # до резюма равен сумме активных; здесь ок
    # Допускаем резюмирование даже без денег — спишется ночью; можно ужесточить при желании
    await resume_parser(user_id, data['parsers'][idx])
    await ui_from_callback_edit(call, "▶️ Парсер запущен.")


@dp.callback_query_handler(lambda c: c.data.startswith('parser_delete_'))
async def cb_parser_delete(call: types.CallbackQuery):
    idx = int(call.data.split('_')[2]) - 1
    user_id = call.from_user.id
    data = user_data.get(str(user_id), {})
    if not data or idx < 0 or idx >= len(data.get('parsers', [])):
        await call.answer("Не найдено", show_alert=True)
        return
    p = data['parsers'][idx]
    if p.get('status') != 'paused':
        await ui_from_callback_edit(call, "Удалять можно только парсеры на паузе. Сначала нажмите ⏸ Пауза.")
        await call.answer()
        return
    stop_monitor(user_id, p)
    await send_parser_results(user_id, idx)  # как и раньше — отдадим CSV перед удалением
    data['parsers'].pop(idx)
    save_user_data(user_data)
    await ui_from_callback_edit(call, "🗑 Парсер удалён.")
    await call.answer()


@dp.message_handler(commands=['topup'])
async def cmd_topup(message: types.Message, state: FSMContext):
    await ui_send_new(message.from_user.id, "Введите сумму пополнения (минимум 300 ₽):")
    await TopUpStates.waiting_amount.set()


@dp.message_handler(state=TopUpStates.waiting_amount)
async def topup_amount(message: types.Message, state: FSMContext):
    text = message.text.replace(',', '.').strip()
    try:
        amount = float(text)
    except ValueError:
        await ui_send_new(message.from_user.id, "Введите число, например 500 или 1200.50")
        return
    if amount < 300:
        await ui_send_new(message.from_user.id, "Минимальная сумма пополнения — 300 ₽. Введите другую сумму:")
        return
    user_id = message.from_user.id
    payment_id, url = create_topup_payment(user_id, amount)
    if not payment_id:
        await ui_send_new(message.from_user.id, "Не удалось создать платёж. Попробуйте позже.")
    else:
        entry = get_user_data_entry(user_id)
        entry['payment_id'] = payment_id
        save_user_data(user_data)
        kb = types.InlineKeyboardMarkup()
        kb.add(types.InlineKeyboardButton("Оплатить сейчас", url=url))
        await ui_send_new(message.from_user.id, "Нажмите кнопку для оплаты.", reply_markup=kb)
        asyncio.create_task(wait_topup_and_credit(user_id, payment_id, amount))
    await state.finish()


async def bill_user_daily(user_id: int):
    data = user_data.get(str(user_id), {})
    if not data:
        return
    per_day = total_daily_cost(user_id)
    if per_day <= 0:
        return
    bal = float(data.get('balance', 0))
    if bal >= per_day:
        data['balance'] = _round2(bal - per_day)
        save_user_data(user_data)
    else:
        paused_any = False
        for p in data.get('parsers', []):
            if p.get('status') == 'active':
                pause_parser(user_id, p)
                paused_any = True
        save_user_data(user_data)
        if paused_any:
            await safe_send_message(
                bot,
                user_id,
                "⏸ Недостаточно средств. Все парсеры поставлены на паузу. Пополните баланс командой /topup."
            )

async def daily_billing_loop():
    # Списываем сразу при старте и затем — ежедневно в 03:00 UTC (пример)
    while True:
        # 1) Списание
        for uid in list(user_data.keys()):
            try:
                await bill_user_daily(int(uid))
            except Exception:
                logging.exception("Billing error for %s", uid)
        # 2) Ждём до следующего дня 03:00 UTC
        now = datetime.utcnow()
        tomorrow = (now + timedelta(days=1)).replace(hour=3, minute=0, second=0, microsecond=0)
        sleep_seconds = (tomorrow - now).total_seconds()
        await asyncio.sleep(max(60, sleep_seconds))


def parser_info_text(user_id: int, parser: dict, created: bool = False) -> str:
    idx = parser.get('id') or 1
    name = parser.get('name', f'Парсер_{idx}')
    chat_count = len(parser.get('chats', []))
    include_count = len(parser.get('keywords', []))
    exclude_count = len(parser.get('exclude_keywords', []))
    account_label = parser.get('api_id') or 'не привязан'
    data = get_user_data_entry(user_id)
    plan_name = 'PRO'
    if data.get('subscription_expiry'):
        paid_to = datetime.utcfromtimestamp(data['subscription_expiry']).strftime('%Y-%m-%d')
    else:
        paid_to = '—'
    chat_limit = f"/{data.get('chat_limit', CHAT_LIMIT)}" if plan_name == 'PRO' else ''
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
    data = get_user_data_entry(message.from_user.id)
    if not data.get('started'):
        data['started'] = True
        save_user_data(user_data)
    uid = message.from_user.id
    await ui_send_new(uid, t('welcome'), reply_markup=main_menu_keyboard())


@dp.message_handler(commands=['menu'], state="*")
async def cmd_menu(message: types.Message, state: FSMContext):
    await state.finish()
    uid = message.from_user.id
    await ui_send_new(uid, t('menu_main'), reply_markup=main_menu_keyboard())


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
    await ui_send_new(message.from_user.id, "Данные карты удалены.")


@dp.message_handler(commands=['delete_parser'])
async def cmd_delete_parser(message: types.Message):
    """Начать процесс удаления парсера."""
    data = user_data.get(str(message.from_user.id))
    if not data:
        await ui_send_new(message.from_user.id, "Данные не найдены.")
        return
    parsers = [
        (idx, p)
        for idx, p in enumerate(data.get('parsers', []))
        if not p.get('paid')
    ]
    if not parsers:
        await ui_send_new(message.from_user.id, "Нет доступных парсеров для удаления.")
        return
    kb = types.InlineKeyboardMarkup(row_width=1)
    for idx, p in parsers:
        name = p.get('name', f'Парсер {idx + 1}')
        kb.add(types.InlineKeyboardButton(name, callback_data=f'delp_select_{idx}'))
    kb.add(types.InlineKeyboardButton("🔙 Назад", callback_data="back_main"))
    await ui_send_new(message.from_user.id, "Выберите парсер для удаления:", reply_markup=kb)


@dp.callback_query_handler(lambda c: c.data.startswith('delp_select_'))
async def cb_delp_select(call: types.CallbackQuery):
    idx = int(call.data.split('_')[2])
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        types.InlineKeyboardButton("Нет", callback_data='delp_cancel'),
        types.InlineKeyboardButton("Да", callback_data=f'delp_confirm_{idx}')
    )
    await ui_from_callback_edit(call, "Удалить парсер?", reply_markup=kb)
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == 'delp_cancel')
async def cb_delp_cancel(call: types.CallbackQuery):
    await ui_from_callback_edit(call, "Удаление отменено.")
    await ui_from_callback_edit(call, t('menu_main'), reply_markup=main_menu_keyboard())
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
            await ui_from_callback_edit(call, "Оплаченный парсер нельзя удалить.")
            await call.answer()
            return
        stop_monitor(user_id, parser)
        data['parsers'].pop(idx)
        save_user_data(user_data)
        await ui_from_callback_edit(call, "Парсер удалён.")
    await ui_from_callback_edit(call, t('menu_main'), reply_markup=main_menu_keyboard())
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == 'back_main')
async def cb_back_main(call: types.CallbackQuery):
    await ui_from_callback_edit(call, t('menu_main'), reply_markup=main_menu_keyboard())
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
    await ui_from_callback_edit(call, t('menu_setup'), reply_markup=kb)
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
        await ui_from_callback_edit(call, "Парсеры не настроены.")
        await call.answer()
        return
    kb = types.InlineKeyboardMarkup(row_width=1)
    for idx, p in enumerate(data.get('parsers'), 1):
        name = p.get('name', f'Парсер {idx}')
        kb.add(types.InlineKeyboardButton(name, callback_data=f'pay_select_{idx - 1}'))
    kb.add(types.InlineKeyboardButton("🔙 Назад", callback_data="menu_setup"))
    await ui_from_callback_edit(call, "Выберите парсер:", reply_markup=kb)
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
    await ui_from_callback_edit(call, "Выберите действие:", reply_markup=kb)
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
    await ui_from_callback_edit(call, "Сколько чатов вам нужно?")
    await ExpandProStates.waiting_chats.set()
    await call.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('pay_infinity_'))
async def cb_pay_infinity(call: types.CallbackQuery):
    """Inform about INFINITY plan."""
    keyboard111 = types.InlineKeyboardMarkup()
    keyboard111.add(types.InlineKeyboardButton(text="Подключить", url="https://t.me/antufev2025"))
    await ui_from_callback_edit(call, 
        "Тариф INFINITY — 149 990 ₽/мес. Неограниченные чаты и слова, персональный аккаунт-менеджер.\n"
        "Для подключения напишите @TopGrabberSupport",
        reply_markup=keyboard111
    )
    await call.answer()


@dp.message_handler(state=ExpandProStates.waiting_chats)
async def expand_pro_chats(message: types.Message, state: FSMContext):
    """Handle number of chats for PRO expansion."""
    text = message.text.strip()
    if not text.isdigit() or int(text) <= 0:
        await ui_send_new(message.from_user.id, "Введите количество чатов числом")
        return
    chats = int(text)
    price = PRO_MONTHLY_RUB + max(0, chats - 5) * EXTRA_CHAT_MONTHLY_RUB
    await state.update_data(chats=chats, price=price)
    kb = types.InlineKeyboardMarkup(row_width=1)
    kb.add(
        types.InlineKeyboardButton("✅ Подтвердить", callback_data='expand_confirm'),
        types.InlineKeyboardButton("❌ Отмена", callback_data='expand_cancel'),
        types.InlineKeyboardButton("🔙 Назад", callback_data='expand_back'),
    )
    await ui_send_new(message.from_user.id,
        f"Стоимость тарифа PRO на {chats} чатов составит {price} ₽/мес. Подтвердить оплату?",
        reply_markup=kb,
    )
    await ExpandProStates.waiting_confirm.set()


@dp.callback_query_handler(lambda c: c.data == 'expand_confirm', state=ExpandProStates.waiting_confirm)
async def cb_expand_confirm(call: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    price = data.get('price')
    chats = data.get('chats')
    user_id = call.from_user.id
    payment_id, url = create_payment(
        user_id,
        f"{price:.2f}",
        f"Расширение PRO до {chats} чатов для пользователя {user_id}",
    )
    if not payment_id:
        await ui_from_callback_edit(call, "Не удалось создать платёж. Попробуйте позже.")
    else:
        entry = get_user_data_entry(user_id)
        entry['payment_id'] = payment_id
        save_user_data(user_data)
        kb = types.InlineKeyboardMarkup()
        kb.add(types.InlineKeyboardButton("Оплатить сейчас", url=url))
        await ui_from_callback_edit(call, "Нажмите кнопку для оплаты.", reply_markup=kb)
        asyncio.create_task(wait_payment_and_activate(user_id, payment_id, chats))
    await state.finish()
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == 'expand_cancel', state=ExpandProStates.waiting_confirm)
async def cb_expand_cancel(call: types.CallbackQuery, state: FSMContext):
    await ui_from_callback_edit(call, "Действие отменено.")
    await state.finish()
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == 'expand_back', state=ExpandProStates.waiting_confirm)
async def cb_expand_back(call: types.CallbackQuery, state: FSMContext):
    await ui_from_callback_edit(call, "Сколько чатов вам нужно?")
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
    await ui_from_callback_edit(call, t('menu_export'), reply_markup=kb)
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == 'export_all')
async def cb_export_all(call: types.CallbackQuery):
    await send_all_results(call.from_user.id)
    await ui_from_callback_edit(call, t('menu_main'), reply_markup=main_menu_keyboard())
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == 'export_choose')
async def cb_export_choose(call: types.CallbackQuery):
    await cb_result(call)


@dp.callback_query_handler(lambda c: c.data == 'export_alert')
async def cb_export_alert(call: types.CallbackQuery):
    link = f"https://t.me/topgraber_yved_bot"
    await ui_from_callback_edit(call, 
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
    await ui_from_callback_edit(call, t('menu_help'), reply_markup=kb)
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == 'help_start')
async def cb_help_start(call: types.CallbackQuery):
    await cmd_help(call.message)
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == 'help_support')
async def cb_help_support(call: types.CallbackQuery):
    await ui_from_callback_edit(call, "Свяжитесь с поддержкой: https://t.me/TopGrabberSupport")
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
    balance = _round2(float(data.get('balance', 0)))
    per_day = total_daily_cost(call.from_user.id)
    block_dt, left_days = predict_block_date(call.from_user.id)
    extra = (
        f"\n\n"
        f"Дата блокировки: {block_dt} ({left_days} дн.)\n"
        f"Баланс: {balance:.2f} ₽\n"
        f"Общая стоимость: {per_day:.2f} ₽/день"
    )
    text = text + extra

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
        types.InlineKeyboardButton("💰 Пополнить баланс", callback_data="profile_topup"),
        types.InlineKeyboardButton("🔙 Назад", callback_data="back_main"),
    )
    await ui_from_callback_edit(call, text, reply_markup=kb)
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == 'profile_topup')
async def cb_profile_topup(call: types.CallbackQuery):
    await ui_from_callback_edit(call, "Введите сумму пополнения (минимум 300 ₽):")
    await TopUpStates.waiting_amount.set()
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == 'profile_paybalance')
async def cb_profile_paybalance(call: types.CallbackQuery, state: FSMContext):
    data = get_user_data_entry(call.from_user.id)
    ref_bal = float(data.get('ref_balance', 0))
    if ref_bal <= 0:
        await ui_from_callback_edit(call, "На партнёрском балансе недостаточно средств.")
        await call.answer()
        return
    await ui_from_callback_edit(call, f"Введите сумму для перевода с партнёрского баланса (максимум {ref_bal:.2f} ₽):")
    await PartnerTransferStates.waiting_amount.set()
    await call.answer()


@dp.message_handler(state=PartnerTransferStates.waiting_amount)
async def partner_transfer_amount(message: types.Message, state: FSMContext):
    text = message.text.replace(',', '.').strip()
    try:
        amount = float(text)
    except ValueError:
        await ui_send_new(message.from_user.id, "Введите число, например 500 или 1200.50")
        return
    if amount <= 0:
        await ui_send_new(message.from_user.id, "Сумма должна быть положительной.")
        return
    user_id = message.from_user.id
    data = get_user_data_entry(user_id)
    ref_bal = float(data.get('ref_balance', 0))
    if amount > ref_bal:
        await ui_send_new(message.from_user.id, f"Недостаточно на партнёрском балансе (доступно {ref_bal:.2f} ₽). Введите меньшую сумму:")
        return
    data['ref_balance'] = _round2(ref_bal - amount)
    data['balance'] = _round2(float(data.get('balance', 0)) + amount)
    save_user_data(user_data)
    await ui_send_new(message.from_user.id, f"✅ Переведено {amount:.2f} ₽ с партнёрского баланса на основной.")
    await state.finish()


@dp.callback_query_handler(lambda c: c.data == 'profile_withdraw')
async def cb_profile_withdraw(call: types.CallbackQuery):
    await ui_from_callback_edit(call, "Функция вывода средств пока недоступна.")
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == 'profile_delete_card')
async def cb_profile_delete_card(call: types.CallbackQuery):
    await ui_from_callback_edit(call, "Данные карты удалены.")
    await call.answer()


async def _process_tariff_pro(user_id: int, chat_id: int, state: FSMContext):
    data = get_user_data_entry(user_id)
    if data.get('subscription_expiry', 0) > int(datetime.utcnow().timestamp()):
        await ui_send_new(chat_id, "Подписка уже активна.")  # <- chat_id
        return

    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    markup.add("Пропустить")
    await ui_send_new(
        chat_id,  # <- ВАЖНО: сюда всегда chat_id, не message!
        "Введите промокод или нажмите 'Пропустить'.",
        reply_markup=markup,
    )
    await PromoStates.waiting_promo.set()


@dp.message_handler(commands=['tariff_pro'])
async def cmd_tariff_pro(message: types.Message, state: FSMContext):
    await _process_tariff_pro(
        user_id=message.from_user.id,      # для вашей БД
        chat_id=message.chat.id,           # для отправки сообщений
        state=state
    )


@dp.callback_query_handler(lambda c: c.data == 'tariff_pro')
async def cb_tariff_pro(call: types.CallbackQuery, state: FSMContext):
    await _process_tariff_pro(
        user_id=call.from_user.id,         # кто нажал кнопку
        chat_id=call.message.chat.id,      # куда отвечать
        state=state
    )
    await call.answer()


@dp.message_handler(state=PromoStates.waiting_promo)
async def promo_entered(message: types.Message, state: FSMContext):
    text_raw = (message.text or "").strip()
    code = text_raw.upper()
    user_id = message.from_user.id

    # 1) Пропуск ввода промокода
    if text_raw.lower() in {"пропустить", "skip", "/skip"}:
        await ui_send_new(user_id, "Ок, пропускаем ввод промокода.", reply_markup=types.ReplyKeyboardRemove())
        data = get_user_data_entry(user_id)
        used_promos = data.setdefault('used_promos', [])
        await ui_send_new(user_id,
            "Перейдите по ссылке для оплаты тарифа PRO.",
            reply_markup=types.ReplyKeyboardRemove(),
        )
        payment_id, url = create_pro_payment(user_id)
        if not payment_id:
            await ui_send_new(user_id, "Не удалось создать платёж. Попробуйте позже.")
        else:
            data['payment_id'] = payment_id
            save_user_data(user_data)
            kb = types.InlineKeyboardMarkup()
            kb.add(types.InlineKeyboardButton("Оплатить сейчас", url=url))
            await ui_send_new(user_id, "Нажмите кнопку для оплаты.", reply_markup=kb)
            asyncio.create_task(
                wait_payment_and_activate(user_id, payment_id, data.get('chat_limit', CHAT_LIMIT))
            )
        await state.finish()
        return

    data = get_user_data_entry(user_id)
    used_promos = data.setdefault('used_promos', [])

    # 2) Проверка уже использованных промокодов
    if code in used_promos:
        await ui_send_new(user_id,
            t('promo_already_used'),
            reply_markup=types.ReplyKeyboardRemove(),
        )
        await ui_send_new(user_id, 'Введите промокод или нажмите "Пропустить".')
        return  # остаёмся в том же стейте

    # 3) Обработка демо-промокода
    if code == 'DEMO':
        expiry = int((datetime.utcnow() + timedelta(days=7)).timestamp())
        data['subscription_expiry'] = expiry
        used_promos.append(code)
        save_user_data(user_data)
        await ui_send_new(user_id,
            "Промокод принят! Вам предоставлено 7 дней бесплатного тарифа PRO.",
            reply_markup=types.ReplyKeyboardRemove(),
        )
        await state.finish()
        await login_flow(message, state)
        return

    # 4) Если промокод неизвестный (добавляйте платные коды в known_codes)
    known_codes = {'DEMO'}
    if code not in known_codes:
        await ui_send_new(user_id,
            "Неверный промокод.",
            reply_markup=types.ReplyKeyboardRemove(),
        )
        await ui_send_new(user_id, 'Введите промокод или нажмите "Пропустить".')
        return  # остаёмся в том же стейте

    # 5) Ветка для платных промокодов (пример; сейчас недостижима при known_codes == {'DEMO'})
    await ui_send_new(user_id,
        "Перейдите по ссылке для оплаты тарифа PRO.",
        reply_markup=types.ReplyKeyboardRemove(),
    )
    payment_id, url = create_pro_payment(user_id)
    if not payment_id:
        await ui_send_new(user_id, "Не удалось создать платёж. Попробуйте позже.")
    else:
        data['payment_id'] = payment_id
        save_user_data(user_data)
        kb = types.InlineKeyboardMarkup()
        kb.add(types.InlineKeyboardButton("Оплатить сейчас", url=url))
        await ui_send_new(user_id, "Нажмите кнопку для оплаты.", reply_markup=kb)
        asyncio.create_task(
            wait_payment_and_activate(user_id, payment_id, data.get('chat_limit', CHAT_LIMIT))
        )
    await state.finish()



@dp.callback_query_handler(lambda c: c.data == 'result')
async def cb_result(call: types.CallbackQuery):
    data = user_data.get(str(call.from_user.id))
    if not data or not data.get('parsers'):
        await ui_from_callback_edit(call, "Парсеры не настроены.")
        await call.answer()
        return
    kb = types.InlineKeyboardMarkup(row_width=1)
    for idx, p in enumerate(data.get('parsers'), 1):
        name = p.get('name', f'Парсер {idx}')
        kb.add(types.InlineKeyboardButton(name, callback_data=f"csv_{idx}"))
    kb.add(types.InlineKeyboardButton("🔙 Назад", callback_data="back_main"))
    await ui_from_callback_edit(call, "Выберите парсер для получения CSV:", reply_markup=kb)
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == 'help_info')
async def cb_help(call: types.CallbackQuery):
    await ui_from_callback_edit(call, HELP_TEXT)
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == 'info')
async def cb_info(call: types.CallbackQuery):
    await ui_from_callback_edit(call, INFO_TEXT)
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == 'active_parsers')
async def cb_active_parsers(call: types.CallbackQuery):
    data = user_data.get(str(call.from_user.id))
    if not data or not data.get('parsers'):
        await ui_from_callback_edit(call, "Парсеры не настроены.")
        await call.answer()
        return
    kb = types.InlineKeyboardMarkup(row_width=1)
    for idx, p in enumerate(data.get('parsers'), 1):
        name = p.get('name', f'Парсер {idx}')
        kb.add(types.InlineKeyboardButton(name, callback_data=f"edit_{idx}"))
    kb.add(types.InlineKeyboardButton("🔙 Назад", callback_data="back_main"))
    await ui_from_callback_edit(call, "Активные парсеры:", reply_markup=kb)
    await call.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('csv_'))
async def cb_send_csv(call: types.CallbackQuery):
    idx = int(call.data.split('_')[1]) - 1
    user_id = call.from_user.id
    check_subscription(user_id)
    data = user_data.get(str(user_id))
    if not data:
        await ui_from_callback_edit(call, "Данные не найдены.")
        await call.answer()
        return
    parsers = data.get('parsers', [])
    if idx < 0 or idx >= len(parsers):
        await ui_from_callback_edit(call, "Парсер не найден.")
        await call.answer()
        return
    parser = parsers[idx]
    results = parser.get('results', [])
    if not results:
        await ui_from_callback_edit(call, "Нет сохранённых результатов для этого парсера.")
        await call.answer()
        return
    path = f"results_{user_id}_{idx + 1}.csv"
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
    await ui_from_callback_edit(call, t('menu_main'), reply_markup=main_menu_keyboard())
    await call.answer()


@dp.message_handler(commands=['export'])
async def cmd_export(message: types.Message):
    check_subscription(message.from_user.id)
    await send_all_results(message.from_user.id)


@dp.message_handler(commands=['check_payment'])
async def cmd_check_payment(message: types.Message):
    user_id = message.from_user.id
    data = get_user_data_entry(user_id)
    payment_id = data.get('payment_id')
    if not payment_id:
        await ui_send_new(user_id, "Платёж не найден.")
        return
    status = check_payment(payment_id)
    if status == 'succeeded':
        expiry = int((datetime.utcnow() + timedelta(days=30)).timestamp())
        data['subscription_expiry'] = expiry
        data.pop('payment_id', None)
        save_user_data(user_data)
        await ui_send_new(user_id, t('payment_success'))
    else:
        await ui_send_new(user_id, t('payment_failed', status=status))




@dp.callback_query_handler(lambda c: c.data.startswith('edit_exclude_'), state='*')
async def cb_edit_exclude(call: types.CallbackQuery, state: FSMContext):
    idx = int(call.data.split('_')[2]) - 1
    await state.update_data(edit_idx=idx)
    await ui_from_callback_edit(call, 
        "Введите новые исключающие слова (через запятую):"
    )
    await EditParserStates.waiting_exclude.set()
    await call.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('edit_name_'), state='*')
async def cb_edit_name(call: types.CallbackQuery, state: FSMContext):
    idx = int(call.data.split('_')[2]) - 1
    await state.update_data(edit_idx=idx)
    await ui_from_callback_edit(call, "Введите новое название парсера:")
    await EditParserStates.waiting_name.set()
    await call.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('edit_tariff_'))
async def cb_edit_tariff(call: types.CallbackQuery, state: FSMContext):
    await cb_tariff_pro(call, state)


@dp.message_handler(state=ParserStates.waiting_name)
async def get_parser_name(message: types.Message, state: FSMContext):
    name = message.text.strip()
    if not name:
        await ui_send_new(message.from_user.id, "Название не может быть пустым. Попробуйте ещё раз:")
        return
    await state.update_data(parser_name=name)
    await ui_send_new(message.from_user.id,
        "Укажите ссылки на чаты или каналы (через пробел или запятую):"
    )
    await ParserStates.waiting_chats.set()

async def start_tariff_pro_from_message(message: types.Message, state: FSMContext):
    await _process_tariff_pro(
        user_id=message.from_user.id,
        chat_id=message.chat.id,
        state=state,
    )

async def start_tariff_pro_from_call(call: types.CallbackQuery, state: FSMContext):
    await _process_tariff_pro(
        user_id=call.from_user.id,
        chat_id=call.message.chat.id,
        state=state,
    )


@dp.message_handler(commands=['addparser'], state='*')
async def cmd_add_parser(message: types.Message, state: FSMContext):
    await state.finish()
    user_id = message.from_user.id
    data = get_user_data_entry(user_id)
    now = int(datetime.utcnow().timestamp())
    if data.get('subscription_expiry', 0) <= now:
        await start_tariff_pro_from_message(message, state)
        return

    info = user_clients.get(user_id)
    if not info:
        saved = user_data.get(str(user_id))
        api_id = saved.get('api_id') if saved else None
        api_hash = saved.get('api_hash') if saved else None
        if not api_id or not api_hash:
            await login_flow(message, state)
            return
        session_name = f"session_{user_id}"
        client = TelegramClient(session_name, api_id, api_hash)
        await client.connect()
        if not await client.is_user_authorized():
            await login_flow(message, state)
            return
        user_clients[user_id] = {
            'client': client,
            'phone': saved.get('phone') if saved else None,
            'phone_hash': '',
            'parsers': saved.get('parsers', []) if saved else []
        }
        for p in user_clients[user_id]['parsers']:
            await start_monitor(user_id, p)
        info = user_clients[user_id]

    parsers = data.setdefault('parsers', [])
    parser_id = len(parsers) + 1
    parser = {
        'id': parser_id,
        'name': f'Парсер_{parser_id}',
        'chats': [],
        'keywords': [],
        'exclude_keywords': [],
        'results': [],
        'status': 'paused',
        'daily_price': 0.0,
    }
    parsers.append(parser)
    info = user_clients.setdefault(user_id, info or {})
    # Avoid duplicating the parser in runtime storage; ensure both
    # user_clients and persistent user_data reference the same list.
    info['parsers'] = parsers
    save_user_data(user_data)
    await ui_send_new(user_id,
        parser_info_text(user_id, parser, created=True),
        reply_markup=parser_settings_keyboard(parser_id),
    )


from playwright.async_api import async_playwright, TimeoutError as PWTimeout
import random
import string

APPS_URL = "https://my.telegram.org/apps"
AUTH_URL = "https://my.telegram.org/auth"

async def rand_shortname(prefix="myapp"):
    tail = "".join(random.choices(string.ascii_lowercase + string.digits, k=6))
    return f"{prefix}{tail}"

async def wait_for_single_input(page, input_type="text", timeout=30000):
    # Возвращает первый видимый input нужного типа
    locator = page.locator(f"input[type='{input_type}']:visible").first
    await locator.wait_for(state="visible", timeout=timeout)
    return locator

def try_regex_parse_api_creds(html_text: str):
    # оставим как fallback
    id_match = re.search(r"App\s+api_id[^0-9]*(\d+)", html_text, re.IGNORECASE)
    if not id_match:
        id_match = re.search(r"\bapi_id[^0-9]*(\d+)", html_text, re.IGNORECASE)
    hash_match = re.search(r"App\s+api_hash[^a-f0-9]*([a-f0-9]{32,64})", html_text, re.IGNORECASE)
    if not hash_match:
        hash_match = re.search(r"\bapi_hash[^a-f0-9]*([a-f0-9]{32,64})", html_text, re.IGNORECASE)
    api_id = id_match.group(1) if id_match else None
    api_hash = hash_match.group(1) if hash_match else None
    return api_id, api_hash

async def extract_api_creds_on_apps(page, timeout=8000):
    """ Открывает /apps и достаёт ключи строго из DOM:
    .form-group:has(label[for='app_id']) → span.form-control → текст (внутри может быть <strong>)
    .form-group:has(label[for='app_hash']) → span.form-control → текст """
    await page.goto(APPS_URL)
    await page.wait_for_load_state("domcontentloaded")
    # Прямое извлечение по селекторам из вашей вёрстки
    try:
        id_span = page.locator(".form-group:has(label[for='app_id']) span.form-control").first
        hash_span = page.locator(".form-group:has(label[for='app_hash']) span.form-control").first
        id_text = await id_span.inner_text(timeout=timeout)
        id_text = id_text.strip()
        hash_text = await hash_span.inner_text(timeout=timeout)
        hash_text = hash_text.strip()
        api_id = re.search(r"\d+", id_text).group(0) if id_text else None
        api_hash = re.search(r"[a-fA-F0-9]{32,64}", hash_text).group(0) if hash_text else None
        if api_id and api_hash:
            return api_id, api_hash
    except PWTimeout:
        # блоки не отрисовались — попробуем после «networkidle», потом fallback по HTML
        await page.wait_for_load_state("networkidle")
        try:
            id_span = page.locator(".form-group:has(label[for='app_id']) span.form-control").first
            hash_span = page.locator(".form-group:has(label[for='app_hash']) span.form-control").first
            id_text = await id_span.inner_text(timeout=timeout)
            id_text = id_text.strip()
            hash_text = await hash_span.inner_text(timeout=timeout)
            hash_text = hash_text.strip()
            api_id = re.search(r"\d+", id_text).group(0) if id_text else None
            api_hash = re.search(r"[a-fA-F0-9]{32,64}", hash_text).group(0) if hash_text else None
            if api_id and api_hash:
                return api_id, api_hash
        except Exception:
            pass
    except Exception:
        pass
    # Fallback — старый парсинг всем HTML
    html = await page.content()
    return try_regex_parse_api_creds(html)

async def create_app_if_missing(page, app_title, short_name, url=None, platform="desktop", desc=""):
    """ Без изменений логики, но использует новый extract_api_creds_on_apps. """
    await page.goto(APPS_URL)
    await page.wait_for_load_state("networkidle")
    api_id, api_hash = await extract_api_creds_on_apps(page)
    if api_id and api_hash:
        return api_id, api_hash
    # найти и заполнить форму (как было раньше)
    title_input = page.locator("input[name='app_title']").first
    shortname_input = page.locator("input[name='app_shortname']").first
    url_input = page.locator("input[name='app_url']").first
    platform_select = page.locator("select[name='app_platform']").first
    desc_textarea = page.locator("textarea[name='app_desc']").first
    submit_btn = page.locator("button[type='submit'], input[type='submit']").first
    if not await title_input.is_visible():
        # возможно, сразу показались креды в другом шаблоне
        api_id, api_hash = await extract_api_creds_on_apps(page)
        if api_id and api_hash:
            return api_id, api_hash
        raise RuntimeError("Не нашёл форму создания приложения на /apps.")
    await title_input.fill(app_title)
    await shortname_input.fill(short_name)
    if url and await url_input.is_visible():
        await url_input.fill(url)
    if await platform_select.is_visible():
        await platform_select.select_option(platform)
    if desc and await desc_textarea.is_visible():
        await desc_textarea.fill(desc)
    if await submit_btn.is_visible():
        await submit_btn.click()
    else:
        await shortname_input.press("Enter")
    await page.wait_for_load_state("networkidle")
    # ещё раз пытаемся извлечь уже выданные креды
    api_id, api_hash = await extract_api_creds_on_apps(page)
    if not (api_id and api_hash):
        await asyncio.sleep(1.5)
        api_id, api_hash = await extract_api_creds_on_apps(page)
    if not (api_id and api_hash):
        raise RuntimeError("Создание прошло, но ключи не нашлись — проверьте вручную /apps.")
    return api_id, api_hash

async def login_my_telegram(page, phone: str, my_code: str):
    await page.goto(AUTH_URL)
    await page.wait_for_load_state("networkidle")
    # 1) ввод телефона
    try:
        phone_input = await wait_for_single_input(page, "text", timeout=30000)
    except PWTimeout:
        # иногда поле телефона имеет type=tel
        phone_input = page.locator("input[type='tel']:visible").first
        await phone_input.wait_for(state="visible", timeout=30000)
    await phone_input.fill(phone)
    await phone_input.press("Enter")
    # 2) код (из Telegram/SMS)
    code_input = await wait_for_single_input(page, "text", timeout=180000)
    await code_input.fill(my_code)
    await code_input.press("Enter")
    # 3) 2FA (пароль), если попросит
    try:
        pwd_input = page.locator("input[type='password']:visible").first
        if await pwd_input.is_visible():
            pwd = await asyncio.get_event_loop().run_in_executor(None, getpass, "Включена 2FA. Введите пароль: ")
            await pwd_input.fill(pwd)
            await pwd_input.press("Enter")
    except PWTimeout:
        pass
    # ждём, пока попадём внутрь
    await page.wait_for_timeout(500)
    await page.wait_for_load_state("networkidle")


async def get_api_creds(phone: str, my_code: str, headless=True):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless, args=["--disable-blink-features=AutomationControlled"])
        context = await browser.new_context()
        page = await context.new_page()
        await login_my_telegram(page, phone, my_code)
        shortname = await rand_shortname()
        api_id, api_hash = await create_app_if_missing(
            page, app_title="My App", short_name=shortname, platform="desktop"
        )
        await context.close()
        await browser.close()
        return api_id, api_hash

async def login_flow(message: types.Message, state: FSMContext):
    await state.finish()
    user_id = message.from_user.id
    check_subscription(user_id)
    data = user_data.get(str(user_id))
    now = int(datetime.utcnow().timestamp())
    if not data or data.get('subscription_expiry', 0) <= now:
        await start_tariff_pro_from_message(message, state)
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
                    await ui_send_new(user_id, "✅ Найдены сохранённые парсеры. Мониторинг запущен.")
                    return
        await ui_send_new(user_id, "👋 Сессия найдена, но требуется повторный вход. Введите номер телефона Telegram (с международным кодом, например +79991234567):")
    else:
        await ui_send_new(user_id,
            "👋 Привет! Для начала работы введите номер телефона Telegram (с международным кодом, например +79991234567):",
        )
    await AuthStates.waiting_phone.set()


@dp.message_handler(state=AuthStates.waiting_phone)
async def get_phone(message: types.Message, state: FSMContext):
    phone = (message.text or "").strip()
    user_id = message.from_user.id

    # легкая валидация номера
    normalized = phone.replace(" ", "").replace("-", "")
    if not normalized.startswith("+"):
        normalized = "+" + normalized
    if not normalized[1:].isdigit():
        await ui_send_new(user_id, "❌ Похоже, номер указан неверно. Укажите номер в формате +1234567890")
        return

    await state.update_data(phone=normalized)
    await ui_send_new(user_id, "Код отправлен в Telegram/SMS для my.telegram.org. Введите код для my.telegram.org:")
    await AuthStates.waiting_my_code.set()


@dp.message_handler(state=AuthStates.waiting_my_code)
async def get_my_code(message: types.Message, state: FSMContext):
    my_code = message.text.strip()
    user_id = message.from_user.id
    data = await state.get_data()
    phone = data.get('phone')

    try:
        api_id, api_hash = await get_api_creds(phone, my_code)
    except Exception as e:
        logging.exception(e)
        await ui_send_new(user_id, f"⚠️ Ошибка при получении API ключей: {e}. Попробуйте ввести код заново.")
        return

    if not api_id or not api_hash:
        await ui_send_new(user_id, "⚠️ Не удалось получить API ключи. Попробуйте ввести код заново.")
        return

    # Сохраняем api_id и api_hash
    saved = user_data.get(str(user_id), {})
    saved.update({
        'api_id': int(api_id),
        'api_hash': api_hash,
        'phone': phone,
    })
    save_user_data(user_data)

    # Теперь создаем Telethon клиент и запрашиваем код для сессии
    session_name = f"session_{user_id}"
    client = TelegramClient(session_name, int(api_id), api_hash)
    await client.connect()

    try:
        result = await client.send_code_request(phone)
        phone_hash = result.phone_code_hash
    except Exception as e:
        logging.exception(e)
        await ui_send_new(user_id, f"⚠️ Ошибка при запросе кода для сессии: {e}. Начните сначала /start.")
        await state.finish()
        return

    user_clients[user_id] = {
        'client': client,
        'phone': phone,
        'phone_hash': phone_hash,
        'parsers': []
    }

    await state.update_data(api_id=int(api_id), api_hash=api_hash, phone_hash=phone_hash)
    await ui_send_new(user_id, "Код отправлен в Telegram/SMS для создания сессии. Введите код для сессии:")
    await AuthStates.waiting_telethon_code.set()


@dp.message_handler(state=AuthStates.waiting_telethon_code)
async def get_telethon_code(message: types.Message, state: FSMContext):
    raw = message.text.strip()
    code = re.sub(r'\D', '', raw)
    user_id = message.from_user.id
    data = await state.get_data()
    api_id = data.get('api_id')
    api_hash = data.get('api_hash')
    phone = data.get('phone')
    phone_hash = data.get('phone_hash')

    session_name = f"session_{user_id}"
    client = TelegramClient(session_name, api_id, api_hash)
    await client.connect()

    try:
        await client.sign_in(phone=phone, code=code, phone_code_hash=phone_hash)
    except PhoneCodeInvalidError:
        await ui_send_new(user_id, "❌ Неверный код. Попробуйте снова:")
        return
    except PhoneCodeExpiredError:
        await ui_send_new(user_id, "❌ Код истёк. Перезапустите /start.")
        await state.finish()
        return
    except SessionPasswordNeededError:
        await ui_send_new(user_id, "🔒 Аккаунт защищён паролем. Введите пароль:")
        await AuthStates.waiting_password.set()
        return
    except Exception as e:
        logging.exception(e)
        await ui_send_new(user_id, f"⚠️ Ошибка при входе: {e}. Попробуйте /start.")
        await state.finish()
        return

    user_clients[user_id] = {
        'client': client,
        'phone': phone,
        'phone_hash': '',
        'parsers': []
    }

    await ui_send_new(user_id,
        "✅ Вы успешно вошли! Теперь укажите *ссылки* на чаты или каналы для мониторинга (через пробел или запятую):",
        parse_mode="Markdown"
    )
    await AuthStates.waiting_chats.set()


@dp.message_handler(state=AuthStates.waiting_password)
async def get_password(message: types.Message, state: FSMContext):
    password = message.text.strip()
    user_id = message.from_user.id
    client_info = user_clients.get(user_id)

    if not client_info:
        await ui_send_new(user_id, "⚠️ Сессия не найдена. Начните сначала /start.")
        await state.finish()
        return

    client = client_info['client']
    try:
        await client.sign_in(password=password)
    except Exception as e:
        logging.exception(e)
        await ui_send_new(user_id, "❌ Неверный пароль. Попробуйте ещё раз:")
        return

    await ui_send_new(user_id,
        "✅ Пароль принят! Теперь укажите *ссылки* на чаты или каналы для мониторинга (через пробел или запятую):",
        parse_mode="Markdown"
    )
    await AuthStates.waiting_chats.set()


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
                await ui_send_new(user_id,
                    "⚠️ Чат не найден. Проверьте доступность в аккаунте и корректность ссылки.")
                return None

    if not chat_ids:
        await ui_send_new(user_id, "⚠️ Пустой список. Введите хотя бы одну ссылку или ID:")
        return None

    limit = get_user_data_entry(user_id).get('chat_limit', CHAT_LIMIT)
    if len(chat_ids) > limit:
        await ui_send_new(user_id, f"⚠️ Можно указать не более {limit} чатов.")
        return None

    await state.update_data(chat_ids=chat_ids)
    await ui_send_new(user_id, "Отлично! Теперь введите ключевые слова для мониторинга (через запятую):")
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
        await ui_send_new(message.from_user.id, "⚠️ Пустой список. Введите хотя бы одно слово:")
        return

    user_id = message.from_user.id
    data = await state.get_data()
    chat_ids = data.get('chat_ids')
    if not chat_ids:
        await ui_send_new(message.from_user.id, "⚠️ Сначала укажите чаты.")
        return

    await state.update_data(keywords=keywords)

    data = await state.get_data()
    user_id = message.from_user.id
    chat_ids = data.get('chat_ids')
    keywords = data.get('keywords')
    name = data.get(
        'parser_name',
        f"Парсер {len(get_user_data_entry(user_id).get('parsers', [])) + 1}"
    )
    parser = {
        'name': name,
        'chats': chat_ids,
        'keywords': keywords,
        'exclude_keywords': [],
        'results': [],
    }
    info = user_clients.setdefault(user_id, {})
    info.setdefault('parsers', []).append(parser)
    if str(user_id) in user_data:
        user_data[str(user_id)].setdefault('parsers', []).append(parser)
        save_user_data(user_data)

    parser['daily_price'] = calc_parser_daily_cost(parser)
    parser['status'] = 'active'  # если хотите сразу стартовать
    save_user_data(user_data)

    await start_monitor(user_id, parser)

    await ui_send_new(message.from_user.id, "✅ Мониторинг запущен! Я уведомлю вас о совпадениях.")
    await ui_send_new(message.from_user.id, t('menu_main'), reply_markup=main_menu_keyboard())
    await state.finish()


@dp.message_handler(state=AuthStates.waiting_keywords)
async def get_keywords_auth(message: types.Message, state: FSMContext):
    await _process_keywords(message, state)


@dp.message_handler(state=ParserStates.waiting_keywords)
async def get_keywords_parser(message: types.Message, state: FSMContext):
    await _process_keywords(message, state)


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
                await ui_send_new(user_id, "⚠️ Чат не найден. Проверьте доступность и корректность ссылки.")
                return
    if not chat_ids:
        await ui_send_new(user_id, "⚠️ Пустой список. Введите хотя бы одну ссылку или ID:")
        return

    limit = get_user_data_entry(user_id).get('chat_limit', CHAT_LIMIT)
    if len(chat_ids) > limit:
        await ui_send_new(user_id, f"⚠️ Можно указать не более {limit} чатов.")
        return
    parser = user_data[str(user_id)]['parsers'][idx]
    stop_monitor(user_id, parser)
    parser['chats'] = chat_ids
    save_user_data(user_data)
    parser['daily_price'] = calc_parser_daily_cost(parser)
    await start_monitor(user_id, parser)
    await state.finish()
    await ui_send_new(user_id, "✅ Чаты обновлены.")


@dp.message_handler(state=EditParserStates.waiting_keywords)
async def edit_keywords_handler(message: types.Message, state: FSMContext):
    data = await state.get_data()
    idx = data.get('edit_idx')
    keywords = [w.strip().lower() for w in message.text.split(',') if w.strip()]
    if not keywords:
        await ui_send_new(message.from_user.id, "⚠️ Список пуст. Введите хотя бы одно слово:")
        return
    user_id = message.from_user.id
    parser = user_data[str(user_id)]['parsers'][idx]
    stop_monitor(user_id, parser)
    parser['keywords'] = keywords
    save_user_data(user_data)
    await start_monitor(user_id, parser)
    parser['daily_price'] = calc_parser_daily_cost(parser)
    await state.finish()
    await ui_send_new(message.from_user.id, "✅ Ключевые слова обновлены.")


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
    parser['daily_price'] = calc_parser_daily_cost(parser)

    await state.finish()
    await ui_send_new(message.from_user.id, "✅ Исключающие слова обновлены.")


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
    await ui_send_new(message.from_user.id, "✅ Название обновлено.")


if __name__ == '__main__':
    print("Bot is starting...")


    async def on_startup(dispatcher):
        asyncio.create_task(daily_billing_loop())


