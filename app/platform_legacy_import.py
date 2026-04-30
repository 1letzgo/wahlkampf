"""Einmaliger Import aus alter Mandanten-wahlkampf.db (users, termine, …) nach platform.db."""

from __future__ import annotations

import sqlite3
from datetime import datetime
from typing import Any

from sqlalchemy.orm import Session

from app.config import PLATFORM_DATABASE_PATH, sqlite_database_path
from app.platform_models import (
    Ortsverband,
    OvMembership,
    PlatformUser,
    Termin,
    TerminKommentar,
    TerminTeilnahme,
)


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    r = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (name,),
    ).fetchone()
    return r is not None


def _rowdicts(conn: sqlite3.Connection, sql: str) -> list[dict[str, Any]]:
    cur = conn.execute(sql)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def _parse_dt(v: Any) -> datetime | None:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v
    if isinstance(v, str):
        try:
            return datetime.fromisoformat(v.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


def migrate_legacy_into_platform_if_needed(db_platform: Session) -> None:
    """Nur wenn platform_users leer; importiert aus erstem Mandanten mit Legacy-users + alle Termine je Mandant."""
    if db_platform.query(PlatformUser).first() is not None:
        return

    ovs = db_platform.query(Ortsverband).order_by(Ortsverband.slug).all()
    if not ovs:
        return

    users_imported = False
    for ov in ovs:
        path = sqlite_database_path(ov.slug)
        if not path.is_file():
            continue
        conn = sqlite3.connect(str(path))
        try:
            if not _table_exists(conn, "users"):
                continue
            rows = _rowdicts(conn, "SELECT * FROM users")
            if not rows or users_imported:
                continue
            for r in rows:
                db_platform.merge(
                    PlatformUser(
                        id=int(r["id"]),
                        username=str(r["username"]).strip().lower(),
                        password_hash=str(r["password_hash"]),
                        display_name=str(r.get("display_name") or ""),
                        created_at=_parse_dt(r.get("created_at")) or datetime.utcnow(),
                        calendar_token=r.get("calendar_token"),
                    )
                )
                db_platform.add(
                    OvMembership(
                        user_id=int(r["id"]),
                        ov_slug=ov.slug,
                        is_admin=bool(r.get("is_admin")),
                        is_approved=bool(r.get("is_approved")),
                    )
                )
            users_imported = True
        finally:
            conn.close()

    if not users_imported:
        return

    db_platform.commit()

    for ov in ovs:
        path = sqlite_database_path(ov.slug)
        if not path.is_file():
            continue
        conn = sqlite3.connect(str(path))
        try:
            if not _table_exists(conn, "termine"):
                continue
            t_rows = _rowdicts(conn, "SELECT * FROM termine")
            for r in t_rows:
                ms = ov.slug
                if r.get("mandant_slug"):
                    ms = str(r["mandant_slug"]).strip().lower()
                db_platform.merge(
                    Termin(
                        id=int(r["id"]),
                        mandant_slug=ms,
                        title=str(r["title"]),
                        description=str(r.get("description") or ""),
                        vorbereitung=str(r.get("vorbereitung") or ""),
                        nachbereitung=str(r.get("nachbereitung") or ""),
                        location=str(r.get("location") or ""),
                        starts_at=_parse_dt(r.get("starts_at")) or datetime.utcnow(),
                        ends_at=_parse_dt(r.get("ends_at")),
                        image_path=r.get("image_path"),
                        externe_teilnehmer_json=str(r.get("externe_teilnehmer_json") or "[]"),
                        created_by_id=int(r["created_by_id"]),
                        created_at=_parse_dt(r.get("created_at")) or datetime.utcnow(),
                    )
                )
            if t_rows:
                for r in _rowdicts(conn, "SELECT * FROM termin_teilnahmen"):
                    db_platform.merge(
                        TerminTeilnahme(
                            id=int(r["id"]),
                            termin_id=int(r["termin_id"]),
                            user_id=int(r["user_id"]),
                            created_at=_parse_dt(r.get("created_at")) or datetime.utcnow(),
                        )
                    )
                for r in _rowdicts(conn, "SELECT * FROM termin_kommentare"):
                    db_platform.merge(
                        TerminKommentar(
                            id=int(r["id"]),
                            termin_id=int(r["termin_id"]),
                            user_id=int(r["user_id"]),
                            body=str(r.get("body") or ""),
                            created_at=_parse_dt(r.get("created_at")) or datetime.utcnow(),
                        )
                    )
        finally:
            conn.close()

    db_platform.commit()
    bump_sqlite_sequences(PLATFORM_DATABASE_PATH)


def bump_sqlite_sequences(platform_db_path: Any) -> None:
    """Nach expliziten IDs: AUTOINCREMENT-Fortsetzung für SQLite."""
    conn = sqlite3.connect(str(platform_db_path))
    try:
        for table in (
            "platform_users",
            "termine",
            "termin_teilnahmen",
            "termin_kommentare",
            "ov_memberships",
        ):
            row = conn.execute(f"SELECT MAX(id) FROM {table}").fetchone()
            mx = row[0]
            if mx is None:
                continue
            conn.execute(
                "INSERT OR REPLACE INTO sqlite_sequence(name,seq) VALUES (?, ?)",
                (table, int(mx)),
            )
        conn.commit()
    finally:
        conn.close()
