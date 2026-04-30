"""Einmalige Initialisierung Plattform-DB + Registry aus bestehenden Ordnern."""

from __future__ import annotations

from sqlalchemy.orm import sessionmaker

from app.auth import hash_password
from app.config import (
    DEFAULT_MANDANT_SLUG,
    MANDANTEN_ROOT,
    SUPERADMIN_INITIAL_PASSWORD,
    SUPERADMIN_RESET_PASSWORD,
)
from app.db_migrate import migrate_legacy_flat_into_mandant
from app.ov_services import provision_ortsverband_storage, register_ortsverband
from app.platform_database import platform_engine
from app.platform_models import Ortsverband, PlatformBase, PlatformUser

_LETZGO_USERNAME = "letzgo"


def bootstrap_platform() -> None:
    PlatformBase.metadata.create_all(bind=platform_engine())
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=platform_engine())
    db = SessionLocal()
    try:
        letzgo_row = (
            db.query(PlatformUser)
            .filter(PlatformUser.username == _LETZGO_USERNAME)
            .first()
        )
        if SUPERADMIN_RESET_PASSWORD:
            if letzgo_row:
                letzgo_row.password_hash = hash_password(SUPERADMIN_RESET_PASSWORD)
                db.add(letzgo_row)
            else:
                db.add(
                    PlatformUser(
                        username=_LETZGO_USERNAME,
                        password_hash=hash_password(SUPERADMIN_RESET_PASSWORD),
                    ),
                )
            db.commit()
        elif SUPERADMIN_INITIAL_PASSWORD and letzgo_row is None:
            db.add(
                PlatformUser(
                    username=_LETZGO_USERNAME,
                    password_hash=hash_password(SUPERADMIN_INITIAL_PASSWORD),
                ),
            )
            db.commit()

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

        if db.query(Ortsverband).count() == 0:
            register_ortsverband(db, DEFAULT_MANDANT_SLUG, "Westerstede")
    finally:
        db.close()
