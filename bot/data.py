import json
import os
import copy
import logging

from .config import DATA_FILE, CHAT_LIMIT


def load_user_data():
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            from .billing import calc_parser_daily_cost  # local import to avoid circular
            for u in data.values():
                u.setdefault('subscription_expiry', 0)
                u.setdefault('recurring', False)
                u.setdefault('reminder3_sent', False)
                u.setdefault('reminder1_sent', False)
                u.setdefault('inactive_notified', False)
                u.setdefault('used_promos', [])
                u.setdefault('chat_limit', CHAT_LIMIT)
                u.setdefault('balance', 0.0)
                u.setdefault('billing_enabled', True)
                for p in u.get('parsers', []):
                    p.setdefault('results', [])
                    p.setdefault('name', 'Без названия')
                    p.setdefault('api_id', '')
                    p.setdefault('api_hash', '')
                    p.setdefault('status', 'paused')
                    p.setdefault('daily_price', 0.0)
                    if not p.get('daily_price'):
                        p['daily_price'] = calc_parser_daily_cost(p)
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


user_data = load_user_data()


def get_user_data_entry(user_id: int):
    data = user_data.setdefault(str(user_id), {})
    data.setdefault('chat_limit', CHAT_LIMIT)
    data.setdefault('balance', 0.0)
    return data
