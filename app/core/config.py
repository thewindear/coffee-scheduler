from functools import lru_cache
from ipaddress import ip_network

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_env: str = "dev"
    host: str = "0.0.0.0"
    port: int = 8000
    embedded_scheduler: bool = True

    redis_url: str = "redis://127.0.0.1:6379/0"
    redis_key_prefix: str = "cs"
    delay_shard_count: int = 16

    mysql_dsn: str = "mysql+asyncmy://root:coffee_dev_password@127.0.0.1:3306/coffee_scheduler"
    db_pool_size: int = 20
    db_max_overflow: int = 20

    pump_interval_ms: int = 50
    pump_batch_size: int = 200
    lease_reap_interval_ms: int = 500
    cron_scan_interval_ms: int = 1000
    flusher_batch_size: int = 500
    flusher_flush_interval_ms: int = 100

    max_receipt_bytes: int = 16384
    task_hash_ttl_seconds: int = 86400
    idempotency_ttl_seconds: int = 86400
    worker_heartbeat_ttl_seconds: int = Field(default=60, ge=1)
    trusted_proxy_ips: list[str] = Field(default_factory=list)
    require_https: bool = True
    partition_precreate_days: int = 7

    default_max_retries: int = 3
    default_lease_seconds: int = 60
    retry_base_delay_ms: int = 1000
    max_query_span_days: int = 7

    delivery_dispatch_interval_ms: int = 100
    delivery_reconcile_interval_ms: int = 2000
    delivery_batch_size: int = 100
    default_artifact_retention_seconds: int = 604800
    default_e2e_timeout_seconds: int = 3600
    delivery_config_json: str | None = None

    @field_validator("trusted_proxy_ips")
    @classmethod
    def validate_proxies(cls, addresses: list[str]) -> list[str]:
        for address in addresses:
            ip_network(address, strict=False)
        return addresses


@lru_cache
def get_settings() -> Settings:
    return Settings()
