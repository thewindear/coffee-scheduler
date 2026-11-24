from pathlib import Path
from typing import Any

from redis.asyncio import Redis

LUA_DIR = Path(__file__).resolve().parent.parent / "lua"


class RedisLuaManager:
    """Manages Redis key naming and pre-registered Lua scripts."""

    def __init__(self, redis: Redis, prefix: str = "cs", delay_shard_count: int = 16) -> None:
        self.redis = redis
        self.prefix = prefix
        self.delay_shard_count = delay_shard_count
        self._scripts: dict[str, Any] = {}

    def task_key(self, task_id: str) -> str:
        return f"{self.prefix}:task:{task_id}"

    def idemp_key(self, key: str) -> str:
        return f"{self.prefix}:idemp:{key}"

    def delay_key(self, shard: int) -> str:
        return f"{self.prefix}:delay:{shard}"

    def delay_shard_for(self, task_id: str) -> int:
        return sum(task_id.encode("utf-8")) % self.delay_shard_count

    def ready_key(self, queue: str) -> str:
        return f"{self.prefix}:ready:{queue}"

    def lease_key(self, queue: str) -> str:
        return f"{self.prefix}:lease:{queue}"

    def dlq_key(self, queue: str) -> str:
        return f"{self.prefix}:dlq:{queue}"

    def worker_key(self, queue: str, worker_id: str) -> str:
        return f"{self.prefix}:worker:{queue}:{worker_id}"

    def active_queues_key(self) -> str:
        return f"{self.prefix}:queues"

    def get_script(self, name: str) -> Any:
        if name not in self._scripts:
            lua_path = LUA_DIR / f"{name}.lua"
            source = lua_path.read_text(encoding="utf-8")
            self._scripts[name] = self.redis.register_script(source)
        return self._scripts[name]
