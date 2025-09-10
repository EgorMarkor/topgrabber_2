from datetime import datetime, timedelta
import uuid
from yookassa import Payment

from .config import YOOKASSA_SHOP_ID, YOOKASSA_TOKEN, RETURN_URL, PRO_MONTHLY_RUB, bot
from .data import get_user_data_entry, save_user_data, user_data
from .text_utils import t
from .billing import _round2
from .utils import safe_send_message


def create_payment(user_id: int, amount: str, description: str, user_email: str = None, user_phone: str = None):
    if not (YOOKASSA_SHOP_ID and YOOKASSA_TOKEN):
        return None, None
    try:
        receipt = {
            "customer": {},
            "items": [
                {
                    "description": description,
                    "quantity": "1.0",
                    "amount": {"value": amount, "currency": "RUB"},
                    "vat_code": 1,
                    "payment_subject": "service",
                    "payment_mode": "full_prepayment",
                }
            ]
        }
        receipt["customer"]["email"] = user_email if user_email else "test@example.com"
        if user_phone:
            clean_phone = "".join(filter(str.isdigit, user_phone))
            if clean_phone.startswith("7"):
                clean_phone = "+" + clean_phone
            elif clean_phone.startswith("8"):
                clean_phone = "+7" + clean_phone[1:]
            else:
                clean_phone = "+7" + clean_phone
            receipt["customer"]["phone"] = clean_phone
        else:
            receipt["customer"]["phone"] = "+79777207868"

        payment = Payment.create(
            {
                "amount": {"value": amount, "currency": "RUB"},
                "confirmation": {"type": "redirect", "return_url": RETURN_URL},
                "description": description,
                "capture": True,
                "receipt": receipt,
            },
            str(uuid.uuid4()),
        )
        return payment.id, payment.confirmation.confirmation_url
    except Exception:
        return None, None


def create_topup_payment(user_id: int, amount_rub: float):
    amount = f"{amount_rub:.2f}"
    return create_payment(user_id, amount, f"Пополнение баланса {user_id} на {amount} ₽")


async def wait_topup_and_credit(user_id: int, payment_id: str, amount: float):
    for _ in range(60):
        status = check_payment(payment_id)
        if status == 'succeeded':
            data = get_user_data_entry(user_id)
            data['balance'] = _round2(float(data.get('balance', 0)) + amount)
            data.pop('payment_id', None)
            save_user_data(user_data)
            await safe_send_message(bot, user_id, f"✅ Оплата прошла. Баланс пополнен на {amount:.2f} ₽.")
            return
        if status in ('canceled', 'expired'):
            data = get_user_data_entry(user_id)
            data.pop('payment_id', None)
            save_user_data(user_data)
            await safe_send_message(bot, user_id, t('payment_failed', status=status))
            return
        from asyncio import sleep
        await sleep(5)
    await safe_send_message(bot, user_id, t('payment_failed', status='timeout'))


def create_pro_payment(user_id: int):
    return create_payment(user_id, f"{PRO_MONTHLY_RUB:.2f}", f"Подписка PRO для пользователя {user_id}")


def check_payment(payment_id: str):
    try:
        payment = Payment.find_one(payment_id)
        return payment.status
    except Exception:
        return None


async def wait_payment_and_activate(user_id: int, payment_id: str, chats: int):
    for _ in range(60):
        status = check_payment(payment_id)
        if status == 'succeeded':
            data = get_user_data_entry(user_id)
            expiry = int((datetime.utcnow() + timedelta(days=30)).timestamp())
            data['subscription_expiry'] = expiry
            data['chat_limit'] = chats
            data.pop('payment_id', None)
            save_user_data(user_data)
            await safe_send_message(bot, user_id, t('payment_success'))
            return
        if status in ('canceled', 'expired'):
            data = get_user_data_entry(user_id)
            data.pop('payment_id', None)
            save_user_data(user_data)
            await safe_send_message(bot, user_id, t('payment_failed', status=status))
            return
        from asyncio import sleep
        await sleep(5)
    await safe_send_message(bot, user_id, t('payment_failed', status='timeout'))
