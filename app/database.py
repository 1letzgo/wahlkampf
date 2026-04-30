from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.config import sqlite_database_path


_engines: dict[str, object] = {}
_sessionmakers: dict[str, sessionmaker] = {}


def get_engine_for_mandant(slug: str):
    """Nur noch für Legacy-/Hilfsskripte; Mandantendaten sind in platform.db."""
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
