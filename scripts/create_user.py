#!/usr/bin/env python3
"""Legt einen Plattform-Nutzer an und ordnet ihn einem OV zu (z. B. Recovery).

Normalerweise reicht die Registrierung in der App; der erste Nutzer im System setzt
optional OV-weit founder_done in platform.db.

Superadmins werden nicht hier gesetzt, sondern über SUPERADMIN_USERNAME /
SUPERADMIN_USERNAMES.

Beispiel:

    python scripts/create_user.py --username max --password geheim \\
        --display \"Max M.\" --mandant-slug westerstede [--admin]

Plattform-DB: PLATFORM_DATABASE_PATH (Standard: ./platform.db im Projektroot).
Das OV (--mandant-slug) muss in der Plattform als Ortsverband existieren.

Existiert der Nutzername bereits und es wird nur eine neue OV-Mitgliedschaft
angelegt, bleibt das Passwort unverändert (--password wird dann ignoriert).
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

os.environ.setdefault("PLATFORM_DATABASE_PATH", str(ROOT / "platform.db"))

from app.auth import hash_password  # noqa: E402
from app.config import DEFAULT_MANDANT_SLUG  # noqa: E402
from app.platform_database import platform_engine  # noqa: E402
from app.platform_models import (  # noqa: E402
    MandantAppSetting,
    Ortsverband,
    OvMembership,
    PlatformBase,
    PlatformUser,
)
from sqlalchemy.orm import sessionmaker  # noqa: E402


def main() -> None:
    p = argparse.ArgumentParser(description="Plattform-Benutzer für Wahlkampf anlegen")
    p.add_argument("--username", required=True)
    p.add_argument("--password", required=True)
    p.add_argument("--display", default="", help="Anzeigename (optional)")
    p.add_argument(
        "--mandant-slug",
        default=DEFAULT_MANDANT_SLUG,
        help=f"OV-Slug (Standard: {DEFAULT_MANDANT_SLUG})",
    )
    p.add_argument(
        "--admin",
        action="store_true",
        help="Als OV-Administrator (Termine dieses OVs verwalten; kein Plattform-Superadmin)",
    )
    args = p.parse_args()

    slug = args.mandant_slug.strip().lower()
    eng_plat = platform_engine()
    PlatformBase.metadata.create_all(bind=eng_plat)
    SessionP = sessionmaker(autocommit=False, autoflush=False, bind=eng_plat)
    pdb = SessionP()
    try:
        if pdb.get(Ortsverband, slug) is None:
            print(
                f"Ortsverband „{slug}“ ist nicht registriert. Zuerst OV anlegen oder Slug prüfen.",
                file=sys.stderr,
            )
            sys.exit(1)

        u = args.username.strip().lower()
        was_empty = pdb.query(PlatformUser).count() == 0
        user = pdb.query(PlatformUser).filter(PlatformUser.username == u).first()
        if user is None:
            user = PlatformUser(
                username=u,
                password_hash=hash_password(args.password),
                display_name=(args.display or "").strip(),
            )
            pdb.add(user)
            pdb.flush()

        existing_mem = (
            pdb.query(OvMembership)
            .filter(OvMembership.user_id == user.id, OvMembership.ov_slug == slug)
            .first()
        )
        if existing_mem is not None:
            print(
                f"Nutzer „{u}“ ist bereits Mitglied von „{slug}“.",
                file=sys.stderr,
            )
            sys.exit(1)

        pdb.add(
            OvMembership(
                user_id=user.id,
                ov_slug=slug,
                is_admin=args.admin,
                is_approved=True,
            ),
        )

        if was_empty:
            pdb.merge(MandantAppSetting(mandant_slug=slug, key="founder_done", value="1"))

        pdb.commit()

        role = "OV-Admin" if args.admin else "Mitglied"
        print(f"Nutzer „{u}“ angelegt/zugeordnet (id={user.id}, {role}, OV={slug}).")
    finally:
        pdb.close()


if __name__ == "__main__":
    main()
