import hashlib
import json
from pathlib import Path

from redis.asyncio import Redis
from redis.commands.core import AsyncScript

LUA_DIR = Path(__file__).resolve().parent.parent / "lua"


class RedisLuaManager:
    """Manages Redis key naming and pre-registered Lua scripts."""

    def __init__(self, redis: Redis, prefix: str = "cs", delay_shard_count: int = 16) -> None:
        self.redis = redis
        self.prefix = prefix
        self.delay_shard_count = delay_shard_count
        self._scripts: dict[str, AsyncScript] = {}

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

    def worker_pattern(self, queue: str | None = None) -> str:
        scope = hashlib.sha256(queue.encode()).hexdigest() if queue is not None else "*"
        return f"{self.prefix}:worker_activity:{scope}:*"

    def worker_key(self, queue: str, service_name: str, worker_id: str) -> str:
        digest = hashlib.sha256(json.dumps([service_name, worker_id]).encode()).hexdigest()
        return self.worker_pattern(queue).removesuffix("*") + digest

    def active_queues_key(self) -> str:
        return f"{self.prefix}:queues"

    async def preload_scripts(self) -> None:
        async with self.redis.pipeline(transaction=False) as pipe:
            for path in sorted(LUA_DIR.glob("[!_]*.lua")):
                pipe.script_load(self.get_script(path.stem).script)
            await pipe.execute()

    def get_script(self, name: str) -> AsyncScript:
        if name not in self._scripts:
            lua_path = LUA_DIR / f"{name}.lua"
            source = lua_path.read_text(encoding="utf-8")
            if name in {"enqueue", "pump"}:
                source = (LUA_DIR / "_ready.lua").read_text(encoding="utf-8") + "\n" + source
            if name in {"heartbeat", "ack", "nack", "check_lease"}:
                source = (LUA_DIR / "_lease.lua").read_text(encoding="utf-8") + "\n" + source
            self._scripts[name] = self.redis.register_script(source)
        return self._scripts[name]
