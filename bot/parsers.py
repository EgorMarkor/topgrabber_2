import re
import html
import asyncio
import os
import csv
from datetime import datetime
from telethon import events

from .config import bot, bot2, CHAT_LIMIT
from .text_utils import normalize_word, t
from .data import user_data, save_user_data, get_user_data_entry
from .utils import safe_send_message
from .billing import calc_parser_daily_cost

user_clients = {}


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


async def start_monitor(user_id: int, parser: dict):
    if parser.get('status', 'paused') != 'active':
        return
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
                message_text = (
                    f"🔔 Найдено '{html.escape(kw)}' в чате '{html.escape(title)}'\n"
                    f"Username: {html.escape(sender_name)}\n"
                    f"DateTime: {msg_time}\n"
                    f"Link: {html.escape(link)}\n"
                    f"<pre>{preview}</pre>"
                )
                if not bot2 or await safe_send_message(bot2, user_id, message_text, parse_mode="HTML") is None:
                    await safe_send_message(
                        bot,
                        user_id,
                        "Пожалуйста, начните чат с ботом уведомлений сначала: https://t.me/topgraber_yved_bot",
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


def pause_parser(user_id: int, parser: dict):
    parser['status'] = 'paused'
    stop_monitor(user_id, parser)
    save_user_data(user_data)


async def resume_parser(user_id: int, parser: dict):
    parser['status'] = 'active'
    parser['daily_price'] = calc_parser_daily_cost(parser)
    save_user_data(user_data)
    await start_monitor(user_id, parser)


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
        await safe_send_message(bot, user_id, t('no_results'))
        return
    path = f"results_{user_id}_all.csv"
    with open(path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["keyword", "chat", "sender", "datetime", "link", "text"])
        writer.writerows(rows)
    from aiogram import types
    await bot.send_document(user_id, types.InputFile(path), caption=t('csv_export_ready'))
    os.remove(path)


async def send_parser_results(user_id: int, idx: int):
    data = user_data.get(str(user_id))
    if not data:
        return
    parsers = data.get('parsers', [])
    if idx < 0 or idx >= len(parsers):
        return
    parser = parsers[idx]
    results = parser.get('results', [])
    if not results:
        await safe_send_message(bot, user_id, t('no_results'))
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
    from aiogram import types
    await bot.send_document(user_id, types.InputFile(path))
    os.remove(path)
