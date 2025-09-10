import logging
from aiogram import Bot, types
from aiogram.utils.exceptions import (
    MessageNotModified,
    MessageToEditNotFound,
    Unauthorized,
    CantInitiateConversation,
    ChatNotFound,
    BotBlocked,
)

from .config import bot
from .data import get_user_data_entry, save_user_data, user_data
from .text_utils import t


async def safe_send_message(
    bot: Bot,
    user_id: int,
    text: str,
    reply_markup=None,
    parse_mode=None,
) -> types.Message | None:
    try:
        if reply_markup is None:
            kb = types.InlineKeyboardMarkup()
            kb.add(types.InlineKeyboardButton("🔙 Назад", callback_data="back_main"))
            reply_markup = kb
        chat = await bot.get_chat(user_id)
        is_recipient_bot = getattr(chat, "is_bot", False)
        if is_recipient_bot:
            logging.warning(f"Skip send: recipient is a bot (user_id={user_id})")
            return None

        return await bot.send_message(
            user_id,
            text,
            reply_markup=reply_markup,
            parse_mode=parse_mode,
        )
    except (
        Unauthorized,
        CantInitiateConversation,
        ChatNotFound,
        BotBlocked,
    ) as e:
        logging.error(f"Cannot send to {user_id}: {e}")
        return None
    except Exception as e:
        logging.error(f"Unexpected send error to {user_id}: {e}")
        return None


def get_or_create_user_entry(user_id: int):
    return get_user_data_entry(user_id)


async def ui_send_new(
    user_id: int,
    text: str,
    reply_markup: types.InlineKeyboardMarkup | None = None,
    parse_mode: str | None = None,
) -> types.Message | None:
    data = get_or_create_user_entry(user_id)
    try:
        m = await safe_send_message(
            bot,
            user_id,
            text,
            reply_markup=reply_markup,
            parse_mode=parse_mode,
        )
        if m:
            data["ui_msg_id"] = m.message_id
            save_user_data(user_data)
        return m
    except Exception as e:
        logging.error(f"Error in ui_send_new for user {user_id}: {e}")
        return None


async def ui_from_callback_edit(
    call: types.CallbackQuery,
    text: str,
    reply_markup: types.InlineKeyboardMarkup | None = None,
    parse_mode: str | None = None,
) -> types.Message | None:
    data = get_or_create_user_entry(call.from_user.id)
    try:
        if reply_markup is None:
            kb = types.InlineKeyboardMarkup()
            kb.add(types.InlineKeyboardButton("🔙 Назад", callback_data="back_main"))
            reply_markup = kb
        m = await call.message.edit_text(text, reply_markup=reply_markup, parse_mode=parse_mode)
    except (MessageNotModified, MessageToEditNotFound):
        m = await safe_send_message(
            bot,
            call.from_user.id,
            text,
            reply_markup=reply_markup,
            parse_mode=parse_mode,
        )
    if m:
        data["ui_msg_id"] = m.message_id
        save_user_data(user_data)
    await call.answer()
    return m
