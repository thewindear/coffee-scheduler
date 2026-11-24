from contextlib import asynccontextmanager

from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncEngine

from app.api.v1.cron_jobs import router as cron_router
from app.api.v1.tasks import router as tasks_router
from app.core.config import Settings, get_settings
from app.core.db import create_db_engine, create_session_factory, init_db
from app.core.redis_client import RedisLuaManager
from app.services.dispatcher import TaskDispatcher
from app.services.flusher import AsyncFlusher
from app.services.scheduler_loop import SchedulerEngine


def create_app(
    settings: Settings | None = None,
    redis_client: Redis | None = None,
    db_engine: AsyncEngine | None = None,
) -> FastAPI:
    cfg = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        own_redis = redis_client is None
        own_engine = db_engine is None
        r_client = redis_client or Redis.from_url(cfg.redis_url, decode_responses=False)
        engine = db_engine or create_db_engine(cfg)
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

        app.state.settings = cfg
        app.state.redis = r_client
        app.state.engine = engine
        app.state.session_factory = session_factory
        app.state.redis_mgr = redis_mgr
        app.state.flusher = flusher
        app.state.dispatcher = dispatcher
        app.state.scheduler = scheduler

        flusher.start()
        if cfg.embedded_scheduler:
            scheduler.start()

        try:
            yield
        finally:
            if cfg.embedded_scheduler:
                await scheduler.stop()
            await flusher.stop()
            if own_redis:
                await r_client.aclose()
            if own_engine:
                await engine.dispose()

    application = FastAPI(title="Coffee Scheduler", version="0.1.0", lifespan=lifespan)
    application.include_router(tasks_router)
    application.include_router(cron_router)
    Instrumentator().instrument(application).expose(application, endpoint="/metrics")

    @application.get("/health", tags=["health"])
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    return application


app = create_app()
