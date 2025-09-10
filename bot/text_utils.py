import re
import json
from pymorphy3 import MorphAnalyzer
import snowballstemmer
from .config import TEXT_FILE

morph = MorphAnalyzer()
stemmer_en = snowballstemmer.stemmer("english")

with open(TEXT_FILE, "r", encoding="utf-8") as f:
    TEXTS = json.load(f)


def normalize_word(word: str) -> str:
    """Return normalized form for keyword matching."""
    word = word.lower()
    if re.search("[а-яА-Я]", word):
        return morph.parse(word)[0].normal_form
    return stemmer_en.stemWord(word)


def t(key, **kwargs):
    text = TEXTS.get(key, key)
    if kwargs:
        try:
            text = text.format(**kwargs)
        except Exception:
            pass
    return text

INFO_TEXT = (
    "TopGrabber – это сервис для автоматического поиска потенциальных клиентов"
    " в чатах Telegram. Вы можете настроить параметры поиска, указав нужные "
    "ключевые слова и ссылки на чаты, в которых хотите искать клиентов. Наш бот"
    " уведомит вас о найденных подходящих сообщениях.\n"
    "Инструкция к боту[](https://dzen.ru/a/ZuHH1h_M5kqcam1A)\n"
    "Бот для получения сообщений[](https://t.me/TOPGrabber_bot)\n\n"
    "Минимальное количество чатов - 5шт\n"
    "Цена:\n1 490₽/ 30 дней\n"
    "Купить 1 дополнительный чат:\n490₽/ 30 дней\n\n"
    "Copyright © 2024 TOPGrabberbot — AI-Парсер сообщений | "
    "ИП Антуфьев Б.В.[](https://telegra.ph/Rekvizity-08-20-2) "
    "ОГРН 304770000133140 ИНН 026408848802 | "
    "Публичная оферта[](https://telegra.ph/Publichnaya-oferta-09-11)"
)

HELP_TEXT = (
    "Если возникли вопросы, изучите Инструкцию к боту[](https://dzen.ru/a/ZuHH1h_M5kqcam1A) или напишите в поддержку: https://t.me/+PqfIWqHquts4YjQy"
)
