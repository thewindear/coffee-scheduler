from datetime import UTC, datetime, timedelta

from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.core.config import Settings
from app.models.tables import Base


def create_db_engine(settings: Settings) -> AsyncEngine:
    kwargs: dict = {"echo": False, "pool_pre_ping": True}
    if not settings.mysql_dsn.startswith("sqlite"):
        kwargs["pool_size"] = settings.db_pool_size
        kwargs["max_overflow"] = settings.db_max_overflow
    return create_async_engine(settings.mysql_dsn, **kwargs)


def create_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


async def ensure_task_records_partitions(engine: AsyncEngine, days_ahead: int = 7) -> None:
    """Ensure MySQL RANGE COLUMNS(created_at) partitions exist for today + days_ahead.

    No-op for non-MySQL dialects (e.g., SQLite in unit tests).
    """
    if engine.dialect.name != "mysql":
        return

    today = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
    async with engine.begin() as conn:
        # Check whether task_records is already partitioned
        res = await conn.execute(
            text(
                "SELECT PARTITION_NAME FROM information_schema.PARTITIONS "
                "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'task_records' "
                "AND PARTITION_NAME IS NOT NULL"
            )
        )
        existing = {row[0] for row in res.fetchall()}

        if not existing:
            # Convert freshly created table into a daily RANGE COLUMNS(created_at) partitioned table
            defs = []
            for offset in range(days_ahead + 1):
                day = today + timedelta(days=offset)
                next_day = day + timedelta(days=1)
                pname = f"p{day.strftime('%Y%m%d')}"
                defs.append(
                    f"PARTITION {pname} VALUES LESS THAN ('{next_day.strftime('%Y-%m-%d 00:00:00')}')"
                )
            sql = f"ALTER TABLE task_records PARTITION BY RANGE COLUMNS(created_at) ({', '.join(defs)})"
            await conn.execute(text(sql))
            return

        for offset in range(days_ahead + 1):
            day = today + timedelta(days=offset)
            next_day = day + timedelta(days=1)
            pname = f"p{day.strftime('%Y%m%d')}"
            if pname not in existing:
                sql = (
                    f"ALTER TABLE task_records ADD PARTITION ("
                    f"PARTITION {pname} VALUES LESS THAN ('{next_day.strftime('%Y-%m-%d 00:00:00')}'))"
                )
                await conn.execute(text(sql))


async def init_db(engine: AsyncEngine, days_ahead: int = 7) -> None:
    """Create tables if missing and ensure MySQL daily partitions."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    await ensure_task_records_partitions(engine, days_ahead=days_ahead)
