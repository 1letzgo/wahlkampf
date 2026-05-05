"""SQLite-Hilfen ohne Alembic: Legacy-Dateilayout + Schema-Anpassungen nur für platform.db."""

from __future__ import annotations

import os
import shutil
from pathlib import Path

from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import make_url

from app.config import BASE_DIR, mandant_dir, sqlite_database_path, upload_dir_for_slug


def migrate_termine_created_by_nullable_sqlite(engine: Engine) -> None:
    """Macht termine.created_by_id optional + ON DELETE SET NULL (SQLite-Tabellenumbau).

    Ohne das schlägt das Löschen eines Nutzers mit FK-Prüfung fehl, sobald noch Termine
    auf platform_users verweisen (ORM hatte NOT NULL ohne ON DELETE).
    """
    if engine.dialect.name != "sqlite":
        return
    insp = inspect(engine)
    if not insp.has_table("termine"):
        return
    with engine.connect() as conn:
        pragma_rows = conn.execute(text("PRAGMA table_info(termine)")).fetchall()
    cb_row = next((r for r in pragma_rows if r[1] == "created_by_id"), None)
    if cb_row is None:
        return
    # PRAGMA table_info: Spalte 3 = notnull (1 = NOT NULL)
    if cb_row[3] == 0:
        return

    ddl_new = """
            CREATE TABLE termine__wk_rebuild (
              id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
              mandant_slug VARCHAR(80) NOT NULL,
              title VARCHAR(200) NOT NULL,
              description TEXT NOT NULL DEFAULT '',
              vorbereitung TEXT NOT NULL DEFAULT '',
              nachbereitung TEXT NOT NULL DEFAULT '',
              location VARCHAR(300) NOT NULL DEFAULT '',
              starts_at DATETIME NOT NULL,
              ends_at DATETIME,
              image_path VARCHAR(500),
              externe_teilnehmer_json TEXT NOT NULL DEFAULT '[]',
              created_by_id INTEGER,
              created_at DATETIME NOT NULL,
              FOREIGN KEY(mandant_slug) REFERENCES ortsverbaende(slug) ON DELETE CASCADE,
              FOREIGN KEY(created_by_id) REFERENCES platform_users(id) ON DELETE SET NULL
            )
            """
    copy_sql = """
            INSERT INTO termine__wk_rebuild (
              id, mandant_slug, title, description, vorbereitung, nachbereitung,
              location, starts_at, ends_at, image_path, externe_teilnehmer_json,
              created_by_id, created_at
            )
            SELECT
              id, mandant_slug, title, description, vorbereitung, nachbereitung,
              location, starts_at, ends_at, image_path, externe_teilnehmer_json,
              created_by_id, created_at
            FROM termine
            """

    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.execute(text("PRAGMA foreign_keys=OFF"))
        conn.execute(text(ddl_new))
        conn.execute(text(copy_sql))
        conn.execute(text("DROP TABLE termine"))
        conn.execute(text("ALTER TABLE termine__wk_rebuild RENAME TO termine"))
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_termine_mandant_slug "
                "ON termine (mandant_slug)"
            ),
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_termine_starts_at ON termine (starts_at)"
            ),
        )
        conn.execute(text("PRAGMA foreign_keys=ON"))


def migrate_termin_teilnahme_status_sqlite(engine: Engine) -> None:
    """Spalte teilnahme_status: zugesagt vs. abgesagt (bestehende Zeilen = Zusage)."""
    if engine.dialect.name != "sqlite":
        return
    insp = inspect(engine)
    if not insp.has_table("termin_teilnahmen"):
        return
    cols = {c["name"] for c in insp.get_columns("termin_teilnahmen")}
    if "teilnahme_status" in cols:
        return
    with engine.begin() as conn:
        conn.execute(
            text(
                "ALTER TABLE termin_teilnahmen ADD COLUMN teilnahme_status "
                "VARCHAR(16) NOT NULL DEFAULT 'zugesagt'"
            ),
        )


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
    migrate_termine_created_by_nullable_sqlite(engine)
    migrate_termin_teilnahme_status_sqlite(engine)
    migrate_termine_promoted_all_ovs_sqlite(engine)
    migrate_termine_attachments_json_sqlite(engine)


def migrate_termine_attachments_json_sqlite(engine: Engine) -> None:
    """JSON-Liste von Dateianhängen (Pfad unter uploads + Anzeigename)."""
    if engine.dialect.name != "sqlite":
        return
    insp = inspect(engine)
    if not insp.has_table("termine"):
        return
    cols = {c["name"] for c in insp.get_columns("termine")}
    if "attachments_json" in cols:
        return
    with engine.begin() as conn:
        conn.execute(
            text(
                "ALTER TABLE termine ADD COLUMN attachments_json "
                "TEXT NOT NULL DEFAULT '[]'"
            ),
        )


def migrate_termine_promoted_all_ovs_sqlite(engine: Engine) -> None:
    """Boolean promoted_all_ovs: Kreis-Termine optional in allen OVs listen."""
    if engine.dialect.name != "sqlite":
        return
    insp = inspect(engine)
    if not insp.has_table("termine"):
        return
    cols = {c["name"] for c in insp.get_columns("termine")}
    if "promoted_all_ovs" in cols:
        return
    with engine.begin() as conn:
        conn.execute(
            text(
                "ALTER TABLE termine ADD COLUMN promoted_all_ovs "
                "BOOLEAN NOT NULL DEFAULT 0"
            ),
        )
