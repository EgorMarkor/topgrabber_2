# topgrabber_2

Эта версия бота поддерживает поиск ключевых слов с учётом разных
падежей и форм. Для русских слов используется морфологический анализ
(`pymorphy3`), для остальных языков — стемминг (`snowballstemmer`).
Так можно находить сообщения даже при изменении словоформ и на разных
языках.

Новая версия позволяет создавать несколько парсеров для одного аккаунта.
Добавляйте их командой `/addparser` и бот запустит мониторинг без повторной
авторизации, если сессия уже активна.

## Дополнения
- Все текстовые сообщения вынесены в `texts.json`.
- Команда `/export` позволяет получить CSV-файл со всеми результатами.
- Команды `/enable_recurring` и `/disable_recurring` управляют рекуррентной оплатой.
