import asyncio
from datetime import datetime, timedelta

from .config import PRO_MONTHLY_RUB, EXTRA_CHAT_MONTHLY_RUB, DAYS_IN_MONTH, bot
from .data import get_user_data_entry, user_data, save_user_data
from .utils import safe_send_message
from .parsers import send_all_results
from .text_utils import t


def _round2(x: float) -> float:
    return float(f"{x:.2f}")


def calc_parser_daily_cost(parser: dict) -> float:
    chats = len(parser.get('chats', []))
    base = PRO_MONTHLY_RUB / DAYS_IN_MONTH
    extras = max(0, chats - 5) * (EXTRA_CHAT_MONTHLY_RUB / DAYS_IN_MONTH)
    return _round2(base + extras)


def total_daily_cost(user_id: int) -> float:
    data = user_data.get(str(user_id), {})
    total = 0.0
    for p in data.get('parsers', []):
        if p.get('status', 'paused') == 'active':
            total += p.get('daily_price') or calc_parser_daily_cost(p)
    return _round2(total)


def predict_block_date(user_id: int) -> tuple[str, int]:
    data = user_data.get(str(user_id), {})
    now = int(datetime.utcnow().timestamp())
    exp = data.get('subscription_expiry', 0)
    if exp > now:
        days = (exp - now) // 86400
        dt = datetime.utcfromtimestamp(exp).strftime('%d.%m.%Y')
        return dt, days
    bal = float(data.get('balance', 0))
    per_day = total_daily_cost(user_id)
    if per_day <= 0 or bal <= 0:
        return "—", 0
    days = int(bal // per_day)
    dt = (datetime.utcnow() + timedelta(days=days)).strftime('%d.%m.%Y')
    return dt, days


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
        from .parsers import pause_parser
        for p in data.get('parsers', []):
            if p.get('status') == 'active':
                pause_parser(user_id, p)
                paused_any = True
        save_user_data(user_data)
        if paused_any:
            await safe_send_message(
                bot,
                user_id,
                "⏸ Недостаточно средств. Все парсеры поставлены на паузу. Пополните баланс командой /topup.",
            )


async def daily_billing_loop():
    while True:
        for uid in list(user_data.keys()):
            try:
                await bill_user_daily(int(uid))
            except Exception:
                pass
        now = datetime.utcnow()
        tomorrow = (now + timedelta(days=1)).replace(hour=3, minute=0, second=0, microsecond=0)
        sleep_seconds = (tomorrow - now).total_seconds()
        await asyncio.sleep(max(60, sleep_seconds))


def check_subscription(user_id: int):
    data = get_user_data_entry(user_id)
    exp = data.get('subscription_expiry', 0)
    now = int(datetime.utcnow().timestamp())
    days_left = (exp - now) // 86400
    if exp and days_left <= 0:
        if not data.get('inactive_notified'):
            asyncio.create_task(send_all_results(user_id))
            asyncio.create_task(safe_send_message(bot, user_id, t('subscription_inactive')))
            data['inactive_notified'] = True
            save_user_data(user_data)
        return
    if not data.get('recurring'):
        if days_left == 3 and not data.get('reminder3_sent'):
            asyncio.create_task(safe_send_message(bot, user_id, t('subscription_reminder', days=3)))
            data['reminder3_sent'] = True
        elif days_left == 1 and not data.get('reminder1_sent'):
            asyncio.create_task(safe_send_message(bot, user_id, t('subscription_reminder', days=1)))
            data['reminder1_sent'] = True
        if data.get('reminder3_sent') or data.get('reminder1_sent'):
            save_user_data(user_data)
