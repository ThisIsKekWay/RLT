from typing import Text
import aggregator
from aiogram import Router, types, F
from aiogram.filters import Command
from exceptions import ParsingFailedException, WrongDateException
aggregation_router = Router()


@aggregation_router.message(Command('start'))
async def start(msg: types.Message):
    usrnm = msg.from_user.first_name
    content = Text(f"Привет, {usrnm}")
    await msg.answer(content)


@aggregation_router.message(Command('help'))
async def start(msg: types.Message):
    content = Text("Бот поддерживает только одну форму запроса:\n"
                   "{'dt_from': 'Дата начала области расчета'\n,"
                   "'dt_upto': 'Дата окончания области расчета'\n,"
                   "'group_type': 'Тип группировки рассчета.\n"
                   "Может быть hour, day, month'}"
                   )
    await msg.answer(content)


@aggregation_router.message(F.text)
async def get_message(msg: types.Message):
    content = msg.text
    await aggregator.init()
    try:
        data = await aggregator.get_payload(content)
        result = await aggregator.aggregate_pool(data)
        await msg.answer(str(result))
    except ParsingFailedException as e:
        await msg.answer(e.message)
    except WrongDateException as e:
        await msg.answer(e.message)
