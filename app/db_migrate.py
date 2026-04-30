"""Leichte Schema-Anpassungen für SQLite (ohne Alembic)."""

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

    `metadata.create_all` legt keine neuen Spalten an bestehenden Tabellen an; Deployments mit
    älterer platform_users-Struktur würden sonst beim ersten SELECT scheitern.
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


def run_sqlite_migrations(engine: Engine) -> None:
    if engine.dialect.name != "sqlite":
        return
    insp = inspect(engine)
    if insp.has_table("users"):
        cols = {c["name"] for c in insp.get_columns("users")}
        with engine.begin() as conn:
            if "is_admin" not in cols:
                conn.execute(
                    text("ALTER TABLE users ADD COLUMN is_admin INTEGER NOT NULL DEFAULT 0"),
                )
            if "is_approved" not in cols:
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


def _sqlite_main_path(engine: Engine) -> Path | None:
    if engine.dialect.name != "sqlite":
        return None
    db = engine.url.database
    if not db or db == ":memory:":
        return None
    p = Path(db)
    if not p.is_absolute():
        p = (BASE_DIR / p).resolve()
    else:
        p = p.resolve()
    return p


def _legacy_plakate_sqlite_files(main: Path) -> list[Path]:
    """Kandidaten für die frühere plakate.db (Projektroot, optional PLAKATE_DATABASE_URL)."""
    seen: set[Path] = set()
    out: list[Path] = []
    for candidate in (BASE_DIR / "plakate.db",):
        if candidate.is_file():
            r = candidate.resolve()
            if r != main and r not in seen:
                seen.add(r)
                out.append(r)
    raw = os.environ.get("PLAKATE_DATABASE_URL", "").strip()
    if raw.startswith("sqlite:"):
        try:
            u = make_url(raw)
            if u.database and u.database != ":memory:":
                lp = Path(u.database)
                if not lp.is_absolute():
                    lp = (BASE_DIR / lp).resolve()
                else:
                    lp = lp.resolve()
                if lp.is_file() and lp != main and lp not in seen:
                    seen.add(lp)
                    out.append(lp)
        except Exception:
            pass
    return out


def migrate_plakate_from_legacy_sqlite(engine: Engine) -> None:
    """Einmalig Daten aus alter plakate.db in die Haupt-DB übernehmen (nur wenn plakate leer)."""
    if engine.dialect.name != "sqlite":
        return
    main_path = _sqlite_main_path(engine)
    if main_path is None:
        return
    insp = inspect(engine)
    if not insp.has_table("plakate"):
        return
    with engine.connect() as conn:
        n = conn.execute(text("SELECT COUNT(*) FROM plakate")).scalar_one()
    if n > 0:
        return
    for legacy in _legacy_plakate_sqlite_files(main_path):
        with engine.begin() as conn:
            conn.execute(
                text("ATTACH DATABASE :p AS legacy_plakate"),
                {"p": str(legacy)},
            )
            try:
                row = conn.execute(
                    text(
                        "SELECT 1 FROM legacy_plakate.sqlite_master "
                        "WHERE type = 'table' AND name = 'plakate' LIMIT 1"
                    )
                ).first()
                if not row:
                    continue
                conn.execute(
                    text(
                        """
                        INSERT INTO plakate (
                            id, latitude, longitude, hung_by_user_id, hung_at,
                            image_path, note, removed_by_user_id, removed_at
                        )
                        SELECT
                            id, latitude, longitude, hung_by_user_id, hung_at,
                            image_path, note, removed_by_user_id, removed_at
                        FROM legacy_plakate.plakate
                        """
                    )
                )
                try:
                    conn.execute(
                        text(
                            """
                            INSERT OR REPLACE INTO sqlite_sequence(name, seq)
                            VALUES (
                                'plakate',
                                (SELECT COALESCE(MAX(id), 0) FROM plakate)
                            )
                            """
                        )
                    )
                except Exception:
                    pass
                return
            finally:
                conn.execute(text("DETACH DATABASE legacy_plakate"))
