from motor.motor_asyncio import AsyncIOMotorClient

from beanie import init_beanie
from database.model import SampleCollection


async def init():
    client = AsyncIOMotorClient("mongodb://localhost:27017")
    await init_beanie(database=client.samples, document_models=[SampleCollection])
