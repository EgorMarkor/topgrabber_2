import asyncio
from aiogram.utils import executor

from bot.config import dp
from bot.billing import daily_billing_loop
import bot.handlers  # noqa: F401


async def on_startup(dispatcher):
    asyncio.create_task(daily_billing_loop())


if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)
