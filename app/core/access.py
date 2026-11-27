import hashlib
import hmac
import secrets
from dataclasses import dataclass

from fastapi import HTTPException
from sqlalchemy import and_, or_, select, true
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.tables import ServiceCredential, ServiceGrant, ServiceIdentity

OPERATIONS = (
    "submit",
    "pull",
    "task_read",
    "task_retry",
    "task_rerun",
    "dlq_read",
    "dlq_retry",
    "monitor",
    "metrics",
    "cron_read",
    "cron_manage",
)


def forbidden() -> HTTPException:
    return HTTPException(403, "Operation is outside the service's authorized scope")


@dataclass(frozen=True)
class Grant:
    operation: str
    business_id: str = ""
    queue: str = ""
    task_type: str = ""


@dataclass(frozen=True)
class Identity:
    service_name: str
    business_id: str | None
    administrator: bool
    grants: tuple[Grant, ...]

    def scopes(self, operation: str) -> tuple[Grant, ...]:
        scopes = (
            (Grant(operation),)
            if self.administrator
            else tuple(grant for grant in self.grants if grant.operation == operation)
        )
        if not scopes:
            raise forbidden()
        return scopes

    def require(self, operation: str, business_id=None, queue=None, task_type=None) -> None:
        if not any(
            (not grant.business_id or grant.business_id == business_id)
            and (not grant.queue or grant.queue == queue)
            and (not grant.task_type or grant.task_type == task_type)
            for grant in self.scopes(operation)
        ):
            raise forbidden()

    def queue_scopes(self, operation: str, queue: str) -> tuple[Grant, ...]:
        scopes = tuple(g for g in self.scopes(operation) if not g.queue or g.queue == queue)
        if not scopes:
            raise forbidden()
        return scopes

    def condition(self, operation: str, *, business, queue=None, task_type=None):
        return or_(
            *[
                and_(
                    true(),
                    *([business == g.business_id] if g.business_id else []),
                    *([queue == g.queue] if g.queue else []),
                    *([task_type == g.task_type] if g.task_type else []),
                )
                for g in self.scopes(operation)
            ]
        )


async def load_identity(
    session: AsyncSession, service: ServiceIdentity, *, lock: bool = False
) -> Identity:
    stmt = select(ServiceGrant).where(ServiceGrant.service_name == service.name)
    grants = (await session.scalars(stmt.with_for_update() if lock else stmt)).all()
    return Identity(
        service.name,
        service.business_id,
        service.administrator,
        tuple(Grant(g.operation, g.business_id, g.queue, g.task_type) for g in grants),
    )


def _digest(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


async def authenticate(session: AsyncSession, token: str) -> Identity | None:
    credential_id, separator, secret = token.partition(".")
    if not separator or len(credential_id) != 32 or len(secret) != 43:
        return None
    row = (
        await session.execute(
            select(ServiceCredential, ServiceIdentity)
            .join(ServiceIdentity)
            .where(
                ServiceCredential.credential_id == credential_id,
                ServiceCredential.revoked.is_(False),
            )
        )
    ).one_or_none()
    if row is None or not hmac.compare_digest(row.ServiceCredential.digest, _digest(token)):
        return None
    return await load_identity(session, row.ServiceIdentity)


def issue_credential(session: AsyncSession, service_name: str) -> tuple[str, str]:
    credential_id = secrets.token_hex(16)
    token = f"{credential_id}.{secrets.token_urlsafe(32)}"
    session.add(
        ServiceCredential(
            credential_id=credential_id, service_name=service_name, digest=_digest(token)
        )
    )
    return credential_id, token


async def insert_once(session: AsyncSession, model, key: str, **values) -> None:
    if session.get_bind().dialect.name == "mysql":
        stmt = mysql_insert(model).values(**values)
        stmt = stmt.on_duplicate_key_update(**{key: getattr(model, key)})
    else:
        stmt = sqlite_insert(model).values(**values).on_conflict_do_nothing()
    await session.execute(stmt)


async def set_grant(
    session: AsyncSession, service_name: str, grant: Grant, *, revoke=False
) -> None:
    await session.get(ServiceIdentity, service_name, with_for_update=True)
    values = {"service_name": service_name, **vars(grant)}
    if revoke:
        row = await session.get(ServiceGrant, values)
        if row is not None:
            await session.delete(row)
    else:
        await insert_once(session, ServiceGrant, "service_name", **values)
