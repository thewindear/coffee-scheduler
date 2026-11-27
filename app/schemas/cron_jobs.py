from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, model_validator


class CronJobCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=128)
    queue: str = Field(..., min_length=1, max_length=64)
    business_id: str | None = Field(default=None, min_length=1, max_length=64)
    priority: int = Field(default=5, ge=0, le=9)
    cron_expr: str = Field(..., min_length=5, max_length=64)
    payload: dict[str, Any] = Field(default_factory=dict)
    max_retries: int = Field(default=3, ge=0, le=20)
    lease_seconds: int = Field(default=60, ge=1, le=86400)
    enabled: bool = True


class CronJobUpdate(BaseModel):
    queue: str | None = Field(default=None, min_length=1, max_length=64)
    priority: int | None = Field(default=None, ge=0, le=9)
    cron_expr: str | None = Field(default=None, min_length=5, max_length=64)
    payload: dict[str, Any] | None = None
    max_retries: int | None = Field(default=None, ge=0, le=20)
    lease_seconds: int | None = Field(default=None, ge=1, le=86400)
    enabled: bool | None = None

    @model_validator(mode="after")
    def reject_null_updates(self):
        if any(getattr(self, name) is None for name in self.model_fields_set):
            raise ValueError("Cron fields cannot be set to null")
        return self


class CronJobResponse(BaseModel):
    id: int
    name: str
    queue: str
    priority: int
    cron_expr: str
    payload: dict[str, Any]
    max_retries: int
    lease_seconds: int
    enabled: bool
    next_run_at: int
    owner_service: str | None
    business_id: str | None
    version: int
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}
