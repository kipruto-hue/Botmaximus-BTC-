"""Async MongoDB client (PyMongo's native async API — motor is deprecated)."""
from pymongo import AsyncMongoClient

from botmaximus.config import settings

_client: AsyncMongoClient | None = None


def get_client() -> AsyncMongoClient:
    global _client
    if _client is None:
        _client = AsyncMongoClient(settings.mongo_uri, tz_aware=True)
    return _client


def get_db():
    return get_client()[settings.db_name]


async def ping() -> bool:
    try:
        await get_client().admin.command("ping")
        return True
    except Exception:
        return False


async def close() -> None:
    global _client
    if _client is not None:
        await _client.close()
        _client = None
