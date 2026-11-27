from fastapi import Header, Request
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.delivery_config import DeliveryRegistry, IdentityConfig
from app.services.dispatcher import TaskDispatcher


def get_dispatcher(request: Request) -> TaskDispatcher:
    return request.app.state.dispatcher


def get_session_factory(request: Request) -> async_sessionmaker[AsyncSession]:
    return request.app.state.session_factory


def get_delivery_registry(request: Request) -> DeliveryRegistry:
    return request.app.state.delivery_registry


def get_optional_identity(
    request: Request,
    authorization: str | None = Header(default=None),
    x_client_token: str | None = Header(default=None),
) -> IdentityConfig | None:
    token = x_client_token
    if not token and authorization and authorization.lower().startswith("bearer "):
        token = authorization[7:].strip()
    registry: DeliveryRegistry = request.app.state.delivery_registry
    return registry.resolve_identity(token)
