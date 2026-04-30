"""SQLite-Hilfen ohne Alembic: Legacy-Dateilayout + Schema-Anpassungen nur für platform.db."""

from __future__ import annotations

import os
import shutil
from pathlib import Path

from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import make_url

from app.config import BASE_DIR, mandant_dir, sqlite_database_path, upload_dir_for_slug


def migrate_legacy_flat_into_mandant(slug: str) -> None:
    """Kopiert alt `./wahlkampf.db` und `./uploads` nach `mandanten/<slug>/`, falls Ziel noch leer."""
    slug = slug.strip().lower()
    mandant_dir(slug).mkdir(parents=True, exist_ok=True)
    target_db = sqlite_database_path(slug)
    if not target_db.is_file():
        candidates = [BASE_DIR / "wahlkampf.db", Path("/data/wahlkampf.db")]
        raw = os.environ.get("DATABASE_URL", "").strip()
        if raw.startswith("sqlite"):
            try:
                u = make_url(raw)
                if u.database and u.database != ":memory:":
                    lp = Path(u.database)
                    if not lp.is_absolute():
                        lp = (BASE_DIR / lp).resolve()
                    else:
                        lp = lp.resolve()
                    if lp.is_file():
                        candidates.insert(0, lp)
            except Exception:
                pass
        for src in candidates:
            if src.is_file():
                shutil.copy2(src, target_db)
                break

    dest_u = upload_dir_for_slug(slug)
    if not dest_u.exists() or not any(dest_u.iterdir()):
        for legacy_u in (BASE_DIR / "uploads", Path("/data/uploads")):
            if legacy_u.is_dir() and any(legacy_u.iterdir()):
                dest_u.mkdir(parents=True, exist_ok=True)
                for item in legacy_u.iterdir():
                    target_item = dest_u / item.name
                    if item.is_dir():
                        shutil.copytree(item, target_item, dirs_exist_ok=True)
                    else:
                        shutil.copy2(item, target_item)
                break


def run_platform_sqlite_migrations(engine: Engine) -> None:
    """Bestehende platform.db an aktuelles PlatformBase-ORM anbinden (fehlende Spalten).

    `metadata.create_all` legt keine neuen Spalten an bestehenden Tabellen an.
    """
    if engine.dialect.name != "sqlite":
        return
    insp = inspect(engine)
    if insp.has_table("platform_users"):
        cols = {c["name"] for c in insp.get_columns("platform_users")}
        with engine.begin() as conn:
            if "display_name" not in cols:
                conn.execute(
                    text(
                        "ALTER TABLE platform_users ADD COLUMN display_name "
                        "VARCHAR(120) NOT NULL DEFAULT ''"
                    ),
                )
            if "calendar_token" not in cols:
                conn.execute(
                    text(
                        "ALTER TABLE platform_users ADD COLUMN calendar_token VARCHAR(64)"
                    ),
                )
