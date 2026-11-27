from ipaddress import ip_address

from fastapi import Depends, HTTPException, Request
from fastapi.routing import APIRoute
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.access import Identity, authenticate
from app.services.dispatcher import TaskDispatcher

bearer = HTTPBearer(auto_error=False)


def get_dispatcher(request: Request) -> TaskDispatcher:
    return request.app.state.dispatcher


def get_session_factory(request: Request) -> async_sessionmaker[AsyncSession]:
    return request.app.state.session_factory


async def get_identity(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer),
) -> Identity:
    if getattr(request.state, "identity", None) is not None:
        return request.state.identity
    if credentials:
        async with request.app.state.session_factory() as session:
            identity = await authenticate(session, credentials.credentials)
        if identity is not None:
            if request.app.state.settings.require_https and request.url.scheme != "https":
                raise HTTPException(403, "HTTPS is required")
            return identity
    raise HTTPException(
        401, "Valid Bearer credentials are required", headers={"WWW-Authenticate": "Bearer"}
    )


class AuthenticatedRoute(APIRoute):
    def get_route_handler(self):
        handler = super().get_route_handler()

        async def authenticated(request: Request):
            request.state.identity = await get_identity(request, await bearer(request))
            return await handler(request)

        return authenticated


def client_ip(request: Request) -> str | None:
    if request.client:
        try:
            return str(ip_address(request.client.host))
        except ValueError:
            pass
    return None


def require_metrics(identity: Identity = Depends(get_identity)) -> None:
    identity.require("metrics")
