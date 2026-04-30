"""OV anlegen: Ordner, SQLite, Mandanten-Daten."""

from __future__ import annotations

import re
import shutil

from sqlalchemy.orm import Session

from app import models
from app.config import MANDANTEN_ROOT, mandant_dir, upload_dir_for_slug
from app.database import discard_mandant_engine, get_engine_for_mandant
from app.db_migrate import migrate_plakate_from_legacy_sqlite, run_sqlite_migrations
from app.platform_models import OvMembership, Ortsverband, Termin

SLUG_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{1,79}$")


def validate_ov_slug(slug: str) -> str | None:
    s = slug.strip().lower()
    if not SLUG_RE.match(s):
        return (
            "Slug: 2–80 Zeichen, nur Kleinbuchstaben, Ziffern, Bindestrich, Unterstrich; "
            "muss mit Buchstabe oder Ziffer beginnen."
        )
    if s in {"admin", "static", "media", "login", "logout"}:
        return "Dieser Slug ist reserviert."
    return None


def provision_ortsverband_storage(slug: str) -> None:
    """Legt Mandanten-DB und Upload-Struktur an (idempotent)."""
    slug = slug.strip().lower()
    mandant_dir(slug).mkdir(parents=True, exist_ok=True)
    ud = upload_dir_for_slug(slug)
    ud.mkdir(parents=True, exist_ok=True)
    (ud / "plakate").mkdir(parents=True, exist_ok=True)
    engine = get_engine_for_mandant(slug)
    models.Base.metadata.create_all(bind=engine)
    run_sqlite_migrations(engine)
    migrate_plakate_from_legacy_sqlite(engine)


def delete_ortsverband_completely(db_platform: Session, slug: str) -> None:
    """Entfernt den OV aus der Plattform-DB, wirft Engine-Cache und löscht Mandantenordner rekursiv."""
    err = validate_ov_slug(slug)
    if err:
        raise ValueError(err)
    s = slug.strip().lower()
    ov = db_platform.get(Ortsverband, s)
    if not ov:
        raise ValueError("Ortsverband nicht gefunden.")

    db_platform.query(OvMembership).filter(OvMembership.ov_slug == s).delete(
        synchronize_session=False,
    )
    for t in db_platform.query(Termin).filter(Termin.mandant_slug == s).all():
        db_platform.delete(t)

    db_platform.delete(ov)
    db_platform.commit()

    discard_mandant_engine(s)

    root = mandant_dir(s).resolve()
    mr = MANDANTEN_ROOT.resolve()
    if root.is_dir():
        if root == mr or not root.is_relative_to(mr):
            raise RuntimeError("Ungültiger Mandantenpfad; Ordner wurde nicht gelöscht.")
        shutil.rmtree(root)


def register_ortsverband(db_platform: Session, slug: str, display_name: str) -> None:
    slug = slug.strip().lower()
    dn = " ".join(display_name.split()).strip() or slug
    db_platform.merge(Ortsverband(slug=slug, display_name=dn))
    db_platform.commit()
    provision_ortsverband_storage(slug)
