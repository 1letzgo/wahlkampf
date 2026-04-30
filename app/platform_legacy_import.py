"""Einmaliger Import aus alter Mandanten-wahlkampf.db (users, termine, …) nach platform.db."""

from __future__ import annotations

import re
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session

from app.config import PLATFORM_DATABASE_PATH, sqlite_database_path, upload_dir_for_slug
from app.platform_models import (
    MandantAppSetting,
    MandantPlakat,
    Ortsverband,
    OvMembership,
    PlatformUser,
    Termin,
    TerminKommentar,
    TerminTeilnahme,
)

_PLAKAT_PATH_RE = re.compile(r"^plakate/(\d+)_(.+)$")


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


def migrate_mandant_sqlite_assets_into_platform(db_platform: Session) -> None:
    """Übernimmt app_settings und plakate aus Mandanten-wahlkampf.db in die Plattform-DB (idempotent)."""
    ovs = db_platform.query(Ortsverband).order_by(Ortsverband.slug).all()
    changed = False
    for ov in ovs:
        path = sqlite_database_path(ov.slug)
        if not path.is_file():
            continue
        slug = ov.slug
        conn = sqlite3.connect(str(path))
        try:
            if _table_exists(conn, "app_settings"):
                for r in _rowdicts(conn, "SELECT key, value FROM app_settings"):
                    k = str(r["key"])
                    if db_platform.get(MandantAppSetting, (slug, k)) is not None:
                        continue
                    db_platform.merge(
                        MandantAppSetting(
                            mandant_slug=slug,
                            key=k,
                            value=str(r["value"]),
                        ),
                    )
                    changed = True

            if _table_exists(conn, "plakate"):
                if (
                    db_platform.query(MandantPlakat)
                    .filter(MandantPlakat.mandant_slug == slug)
                    .first()
                    is not None
                ):
                    continue
                rows = _rowdicts(conn, "SELECT * FROM plakate ORDER BY id ASC")
                root = Path(upload_dir_for_slug(slug))
                for r in rows:
                    legacy_id = int(r["id"])
                    mp = MandantPlakat(
                        mandant_slug=slug,
                        latitude=float(r["latitude"]),
                        longitude=float(r["longitude"]),
                        hung_by_user_id=int(r["hung_by_user_id"]),
                        hung_at=_parse_dt(r.get("hung_at")) or datetime.utcnow(),
                        image_path=r.get("image_path"),
                        note=str(r.get("note") or ""),
                        removed_by_user_id=r.get("removed_by_user_id"),
                        removed_at=_parse_dt(r.get("removed_at")),
                    )
                    db_platform.add(mp)
                    db_platform.flush()
                    old_path = mp.image_path
                    if isinstance(old_path, str) and old_path.startswith("plakate/"):
                        m = _PLAKAT_PATH_RE.match(old_path.strip())
                        if m and int(m.group(1)) == legacy_id:
                            suffix = m.group(2)
                            new_rel = f"plakate/{mp.id}_{suffix}"
                            src = root / old_path
                            dst = root / new_rel
                            try:
                                if src.is_file():
                                    dst.parent.mkdir(parents=True, exist_ok=True)
                                    src.rename(dst)
                                    mp.image_path = new_rel
                                elif dst.is_file():
                                    mp.image_path = new_rel
                            except OSError:
                                pass
                    changed = True
        finally:
            conn.close()

    if changed:
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
            "mandant_plakate",
        ):
            exists = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
                (table,),
            ).fetchone()
            if not exists:
                continue
            row = conn.execute(f"SELECT MAX(id) FROM [{table}]").fetchone()
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
