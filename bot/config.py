import logging
import os
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from yookassa import Configuration

load_dotenv()
logging.basicConfig(level=logging.INFO)

API_TOKEN = os.getenv("API_TOKEN")
if not API_TOKEN:
    logging.error("API_TOKEN is not set in environment")
    raise RuntimeError("API_TOKEN missing")
bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

API_TOKEN2 = os.getenv("API_TOKEN2")
if API_TOKEN2:
    bot2 = Bot(token=API_TOKEN2)
else:
    bot2 = None
    logging.warning("API_TOKEN2 is not set; notifications bot disabled")

YOOKASSA_SHOP_ID = os.getenv("YOOKASSA_SHOP_ID")
YOOKASSA_TOKEN = os.getenv("YOOKASSA_TOKEN")
if YOOKASSA_SHOP_ID and YOOKASSA_TOKEN:
    Configuration.account_id = YOOKASSA_SHOP_ID
    Configuration.secret_key = YOOKASSA_TOKEN
else:
    logging.warning("YOOKASSA credentials are missing; payment features may not work")

PRO_MONTHLY_RUB = 1490.00
EXTRA_CHAT_MONTHLY_RUB = 490.00
DAYS_IN_MONTH = 30

RETURN_URL = "https://t.me/TOPGrabber_bot"

DATA_FILE = "user_data.json"
TEXT_FILE = "texts.json"

CHAT_LIMIT = 5
