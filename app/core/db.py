from datetime import UTC, datetime, timedelta

from sqlalchemy import Connection, text
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


def task_record_partition_definitions(days_ahead: int) -> dict[str, str]:
    today = datetime.now(UTC).date()
    definitions = {}
    for offset in range(days_ahead + 1):
        day = today + timedelta(days=offset)
        name = f"p{day:%Y%m%d}"
        definitions[name] = (
            f"PARTITION {name} VALUES LESS THAN ('{day + timedelta(days=1):%Y-%m-%d} 00:00:00')"
        )
    return definitions


def partition_task_records_sql(definitions: dict[str, str]) -> str:
    return f"ALTER TABLE task_records PARTITION BY RANGE COLUMNS(created_at) ({', '.join(definitions.values())})"


def ensure_mysql_task_records_partitions(conn: Connection, days_ahead: int) -> None:
    definitions = task_record_partition_definitions(days_ahead)
    lock_name = "SHA2(CONCAT(DATABASE(), '/coffee_task_partitions'), 256)"
    if conn.scalar(text(f"SELECT GET_LOCK({lock_name}, 10)")) != 1:
        raise RuntimeError("Could not acquire task partition maintenance lock")
    try:
        existing = set(
            conn.scalars(
                text(
                    "SELECT PARTITION_NAME FROM information_schema.PARTITIONS "
                    "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'task_records' "
                    "AND PARTITION_NAME IS NOT NULL"
                )
            )
        )
        if not existing:
            conn.execute(text(partition_task_records_sql(definitions)))
            return
        for name, definition in definitions.items():
            if name not in existing:
                conn.execute(text(f"ALTER TABLE task_records ADD PARTITION ({definition})"))
    finally:
        conn.execute(text(f"SELECT RELEASE_LOCK({lock_name})"))


async def ensure_task_records_partitions(engine: AsyncEngine, days_ahead: int = 7) -> None:
    if engine.dialect.name == "mysql":
        async with engine.begin() as conn:
            await conn.run_sync(ensure_mysql_task_records_partitions, days_ahead)


async def init_db(engine: AsyncEngine, days_ahead: int = 7) -> None:
    """Create tables if missing and ensure MySQL daily partitions."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    await ensure_task_records_partitions(engine, days_ahead=days_ahead)
