import asyncio
import logging

import aiogram.exceptions
from aiogram import Bot
from aiogram.dispatcher.dispatcher import Dispatcher

from database.config import init
from config import settings
from handler.aggregate import aggregation_router

bot = Bot(token=settings.TOKEN)
dp = Dispatcher()


# Не уверен, что это лучшее решение, но изначально инит был вообще в хэндлере.
@dp.startup()
async def startup():
    await init()


async def main():
    dp.include_routers(aggregation_router)
    try:
        await bot.delete_webhook()
        await dp.start_polling(bot, skip_updates=True)
    except aiogram.exceptions.TelegramNetworkError as e:
        logging.error(e)
        await bot.session.close()


if __name__ == '__main__':
    asyncio.run(main())
