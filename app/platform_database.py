from __future__ import annotations

from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker
from starlette.requests import Request

from app.config import PLATFORM_DATABASE_PATH
from app.db_migrate import run_platform_sqlite_migrations


class _PlatformEngine:
    engine = None
    SessionLocal = None


def _ensure_engine():
    if _PlatformEngine.engine is None:
        PLATFORM_DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)
        url = f"sqlite:///{PLATFORM_DATABASE_PATH}"
        eng = create_engine(
            url,
            connect_args={"check_same_thread": False},
        )

        @event.listens_for(eng, "connect")
        def _sqlite_enable_foreign_keys(dbapi_conn, _connection_record):
            cur = dbapi_conn.cursor()
            cur.execute("PRAGMA foreign_keys=ON")
            cur.close()

        _PlatformEngine.engine = eng
        _PlatformEngine.SessionLocal = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=_PlatformEngine.engine,
        )
        run_platform_sqlite_migrations(_PlatformEngine.engine)


def platform_engine():
    _ensure_engine()
    return _PlatformEngine.engine


def get_platform_db(request: Request):
    _ensure_engine()
    assert _PlatformEngine.SessionLocal is not None
    db = _PlatformEngine.SessionLocal()
    try:
        yield db
    finally:
        db.close()
