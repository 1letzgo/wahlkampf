from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker
from starlette.requests import Request

from app.config import DEFAULT_MANDANT_SLUG, sqlite_database_path


class Base(DeclarativeBase):
    pass


_engines: dict[str, object] = {}
_sessionmakers: dict[str, sessionmaker] = {}


def get_engine_for_mandant(slug: str):
    slug = slug.strip().lower()
    if slug not in _engines:
        path = sqlite_database_path(slug)
        path.parent.mkdir(parents=True, exist_ok=True)
        url = f"sqlite:///{path}"
        _engines[slug] = create_engine(
            url,
            connect_args={"check_same_thread": False},
        )
    return _engines[slug]


def get_sessionmaker(slug: str) -> sessionmaker:
    slug = slug.strip().lower()
    if slug not in _sessionmakers:
        _sessionmakers[slug] = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=get_engine_for_mandant(slug),
        )
    return _sessionmakers[slug]


def discard_mandant_engine(slug: str) -> None:
    """SQLAlchemy-Engine für einen Mandanten aus dem Cache werfen (z. B. nach OV-Löschung)."""
    slug = slug.strip().lower()
    _sessionmakers.pop(slug, None)
    eng = _engines.pop(slug, None)
    if eng is not None:
        eng.dispose()


def get_db(request: Request):
    slug = request.path_params.get("mandant_slug")
    if slug:
        slug = slug.strip().lower()
    else:
        slug = request.session.get("mandant_slug") or DEFAULT_MANDANT_SLUG
    SessionLocal = get_sessionmaker(slug)
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
