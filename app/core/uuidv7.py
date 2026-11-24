import os
import time
import uuid
from datetime import UTC, datetime, timedelta


def uuid7(timestamp_ms: int | None = None) -> str:
    """Generate an RFC 9562 UUIDv7 string with 48-bit millisecond timestamp prefix."""
    ts_ms = int(time.time() * 1000) if timestamp_ms is None else timestamp_ms
    rand_bytes = os.urandom(10)
    rand_a = int.from_bytes(rand_bytes[:2], "big") & 0x0FFF
    rand_b = int.from_bytes(rand_bytes[2:], "big") & 0x3FFFFFFFFFFFFFFF

    value = (ts_ms & 0xFFFFFFFFFFFF) << 80
    value |= 0x7 << 76  # version 7
    value |= rand_a << 64
    value |= 0x2 << 62  # RFC 4122/9562 variant
    value |= rand_b
    return str(uuid.UUID(int=value))


def extract_uuid7_timestamp_ms(task_id: str) -> int | None:
    """Extract the 48-bit millisecond timestamp from a UUIDv7 string, or None if invalid/not v7."""
    try:
        u = uuid.UUID(task_id)
    except (ValueError, AttributeError):
        return None
    if u.version != 7:
        return None
    return (u.int >> 80) & 0xFFFFFFFFFFFF


def day_bounds_utc_naive(timestamp_ms: int) -> tuple[datetime, datetime]:
    """Return [day_start, next_day_start) as naive UTC datetimes for MySQL partition pruning."""
    dt = datetime.fromtimestamp(timestamp_ms / 1000.0, tz=UTC).replace(tzinfo=None)
    day_start = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    return day_start, day_start + timedelta(days=1)
