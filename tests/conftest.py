import fakeredis.aioredis
import httpx
import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from app.core.config import Settings
from app.main import create_app


@pytest.fixture
def test_settings() -> Settings:
    return Settings(
        app_env="test",
        embedded_scheduler=False,
        mysql_dsn="sqlite+aiosqlite:///:memory:",
        delay_shard_count=4,
        flusher_batch_size=50,
        flusher_flush_interval_ms=20,
        retry_base_delay_ms=10,
        max_receipt_bytes=16384,
    )


@pytest.fixture
async def app_bundle(test_settings: Settings):
    fake_redis = fakeredis.aioredis.FakeRedis(decode_responses=False)
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    app = create_app(settings=test_settings, redis_client=fake_redis, db_engine=engine)

    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            yield app, client, fake_redis, engine

    await fake_redis.aclose()
    await engine.dispose()
