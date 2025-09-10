from aiogram import types


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
