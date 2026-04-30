"""Einmalige Initialisierung Plattform-DB + Registry aus bestehenden Ordnern."""

from __future__ import annotations

from sqlalchemy.orm import sessionmaker

from app.config import DEFAULT_MANDANT_SLUG, MANDANTEN_ROOT
from app.db_migrate import migrate_legacy_flat_into_mandant
from app.ov_services import provision_ortsverband_storage, register_ortsverband
from app.platform_database import platform_engine
from app.platform_legacy_import import migrate_legacy_into_platform_if_needed
from app.platform_models import Ortsverband, PlatformBase


def bootstrap_platform() -> None:
    PlatformBase.metadata.create_all(bind=platform_engine())
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=platform_engine())
    db = SessionLocal()
    try:
        migrate_legacy_flat_into_mandant(DEFAULT_MANDANT_SLUG)

        if MANDANTEN_ROOT.is_dir():
            for child in sorted(MANDANTEN_ROOT.iterdir()):
                if not child.is_dir() or child.name.startswith("."):
                    continue
                slug = child.name.strip().lower()
                if (child / "wahlkampf.db").is_file() and db.get(Ortsverband, slug) is None:
                    db.merge(
                        Ortsverband(
                            slug=slug,
                            display_name=slug.replace("-", " ").replace("_", " ").title(),
                        ),
                    )
            db.commit()

        for ov in db.query(Ortsverband).all():
            provision_ortsverband_storage(ov.slug)

        migrate_legacy_into_platform_if_needed(db)

        if db.query(Ortsverband).count() == 0:
            register_ortsverband(db, DEFAULT_MANDANT_SLUG, "Westerstede")
    finally:
        db.close()
