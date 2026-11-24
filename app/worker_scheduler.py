import asyncio
import signal

from redis.asyncio import Redis

from app.core.config import get_settings
from app.core.db import create_db_engine, create_session_factory, init_db
from app.core.redis_client import RedisLuaManager
from app.services.dispatcher import TaskDispatcher
from app.services.flusher import AsyncFlusher
from app.services.scheduler_loop import SchedulerEngine


async def run_worker_scheduler() -> None:
    cfg = get_settings()
    r_client = Redis.from_url(cfg.redis_url, decode_responses=False)
    engine = create_db_engine(cfg)
    await init_db(engine, days_ahead=cfg.partition_precreate_days)
    session_factory = create_session_factory(engine)
    redis_mgr = RedisLuaManager(
        r_client, prefix=cfg.redis_key_prefix, delay_shard_count=cfg.delay_shard_count
    )
    flusher = AsyncFlusher(
        engine=engine,
        session_factory=session_factory,
        batch_size=cfg.flusher_batch_size,
        flush_interval_ms=cfg.flusher_flush_interval_ms,
    )
    dispatcher = TaskDispatcher(
        redis_mgr=redis_mgr,
        flusher=flusher,
        session_factory=session_factory,
        settings=cfg,
    )
    scheduler = SchedulerEngine(
        redis_mgr=redis_mgr,
        dispatcher=dispatcher,
        flusher=flusher,
        engine=engine,
        session_factory=session_factory,
        settings=cfg,
    )

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    flusher.start()
    scheduler.start()
    try:
        await stop_event.wait()
    finally:
        await scheduler.stop()
        await flusher.stop()
        await r_client.aclose()
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(run_worker_scheduler())
