"""Leichte Schema-Anpassungen für SQLite (ohne Alembic)."""

from __future__ import annotations

from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine


def run_sqlite_migrations(engine: Engine) -> None:
    if engine.dialect.name != "sqlite":
        return
    insp = inspect(engine)
    if not insp.has_table("users"):
        return
    cols = {c["name"] for c in insp.get_columns("users")}
    with engine.begin() as conn:
        if "is_admin" not in cols:
            conn.execute(
                text("ALTER TABLE users ADD COLUMN is_admin INTEGER NOT NULL DEFAULT 0"),
            )
        if "is_approved" not in cols:
            # Bestehende Konten gelten als freigegeben (vor Registrierungs-Flow)
            conn.execute(
                text(
                    "ALTER TABLE users ADD COLUMN is_approved INTEGER NOT NULL DEFAULT 1"
                ),
            )
        if "calendar_token" not in cols:
            conn.execute(text("ALTER TABLE users ADD COLUMN calendar_token VARCHAR(64)"))

    if insp.has_table("app_settings") and insp.has_table("users"):
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO app_settings (key, value)
                    SELECT 'founder_done', '1'
                    WHERE (SELECT COUNT(*) FROM users) > 0
                    AND NOT EXISTS (
                        SELECT 1 FROM app_settings WHERE key = 'founder_done'
                    )
                    """
                ),
            )

    if insp.has_table("termine"):
        term_cols = {c["name"] for c in insp.get_columns("termine")}
        with engine.begin() as conn:
            if "vorbereitung" not in term_cols:
                conn.execute(
                    text(
                        "ALTER TABLE termine ADD COLUMN vorbereitung TEXT NOT NULL DEFAULT ''"
                    ),
                )
            if "nachbereitung" not in term_cols:
                conn.execute(
                    text(
                        "ALTER TABLE termine ADD COLUMN nachbereitung TEXT NOT NULL DEFAULT ''"
                    ),
                )
            if "externe_teilnehmer_json" not in term_cols:
                conn.execute(
                    text(
                        "ALTER TABLE termine ADD COLUMN externe_teilnehmer_json TEXT NOT NULL DEFAULT '[]'"
                    ),
                )
