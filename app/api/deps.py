from fastapi import Request
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.services.dispatcher import TaskDispatcher


def get_dispatcher(request: Request) -> TaskDispatcher:
    return request.app.state.dispatcher


def get_session_factory(request: Request) -> async_sessionmaker[AsyncSession]:
    return request.app.state.session_factory
