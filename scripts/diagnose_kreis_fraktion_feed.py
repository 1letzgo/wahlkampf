"""Diagnose: Warum erscheinen Kreis-Fraktionstermine (nicht) im persönlichen ALLE-Feed?

Aufruf:
    python -m scripts.diagnose_kreis_fraktion_feed <username>

Zeigt:
  - Genehmigte OV-Mitgliedschaften (inkl. Fraktionsstatus)
  - Kreis-OV-Slug + Feature-Status (feature_fraktion)
  - Liste der Kreis-Fraktionstermine in der Zukunft mit Status
    (sichtbar / gefiltert + Grund)
"""

from __future__ import annotations

import sys
from datetime import datetime

from sqlalchemy.orm import sessionmaker

from app.config import kreis_ov_slug
from app.fraktion_visibility import user_is_fraktionsmitglied
from app.mandant_features import FEATURE_FRAKTION, is_mandant_feature_enabled
from app.platform_database import platform_engine
from app.platform_models import Ortsverband, OvMembership, PlatformUser, Termin


def _hr() -> None:
    print("-" * 72)


def diagnose(username: str) -> int:
    SessionLocal = sessionmaker(bind=platform_engine())
    db = SessionLocal()
    try:
        user = db.query(PlatformUser).filter(PlatformUser.username == username).first()
        if not user:
            print(f"User '{username}' nicht gefunden.")
            return 1

        print(f"User: {user.username} (id={user.id})")
        _hr()

        memberships = (
            db.query(OvMembership)
            .filter(OvMembership.user_id == user.id)
            .order_by(OvMembership.ov_slug)
            .all()
        )
        if not memberships:
            print("Keine OV-Mitgliedschaften.")
        else:
            print("OV-Mitgliedschaften:")
            for m in memberships:
                flags = []
                flags.append("approved" if m.is_approved else "PENDING")
                if m.is_admin:
                    flags.append("admin")
                if m.fraktion_member:
                    flags.append("fraktion_member")
                print(f"  - {m.ov_slug}: {', '.join(flags)}")

        approved_slugs = {
            m.ov_slug.strip().lower() for m in memberships if m.is_approved
        }
        _hr()

        ks = (kreis_ov_slug() or "").strip().lower()
        if not ks:
            print("Kein Kreis-OV-Slug konfiguriert (KREIS_OV_SLUG fehlt).")
            return 0
        print(f"Kreis-OV-Slug: {ks}")

        kreis_ov = db.query(Ortsverband).filter(Ortsverband.slug == ks).first()
        if not kreis_ov:
            print("  → Kein Ortsverband mit diesem Slug in DB.")
            return 0
        print(f"  display_name: {kreis_ov.display_name or '(leer)'}")
        feature_on = is_mandant_feature_enabled(db, ks, FEATURE_FRAKTION)
        print(f"  feature_fraktion: {'AN' if feature_on else 'AUS'}")

        is_kreis_member = ks in approved_slugs
        is_kreis_fraktion = user_is_fraktionsmitglied(db, user.id, ks)
        print(f"  User ist genehmigtes Kreis-Mitglied: {is_kreis_member}")
        print(f"  User ist Kreis-Fraktion-Mitglied: {is_kreis_fraktion}")
        _hr()

        if not feature_on:
            print(
                "ERGEBNIS: Kreis hat feature_fraktion AUS — alle Kreis-Fraktionstermine"
                " werden aus jedem Feed entfernt."
            )
            return 0
        if not is_kreis_member:
            print(
                "ERGEBNIS: Du bist kein genehmigtes Kreis-Mitglied. Daher kommen"
                " Kreis-Termine nur via 'promoted_all_ovs=True' rein"
                " (Fraktionstermine sind das fast nie)."
            )

        now = datetime.utcnow()
        kreis_termine = (
            db.query(Termin)
            .filter(
                Termin.mandant_slug == ks,
                Termin.is_fraktion_termin.is_(True),
                Termin.starts_at >= now,
            )
            .order_by(Termin.starts_at.asc())
            .limit(40)
            .all()
        )
        if not kreis_termine:
            print("Keine zukünftigen Kreis-Fraktionstermine in der DB.")
            return 0

        print(f"Zukünftige Kreis-Fraktionstermine ({len(kreis_termine)}):")
        for t in kreis_termine:
            sichtbar = True
            grund = ""
            if not feature_on:
                sichtbar = False
                grund = "feature_fraktion AUS"
            elif not is_kreis_member and not getattr(t, "promoted_all_ovs", False):
                sichtbar = False
                grund = "User nicht im Kreis + nicht promoted"
            elif getattr(t, "fraktion_vertraulich", False) and not is_kreis_fraktion:
                sichtbar = False
                grund = "vertraulich + User kein Kreis-Fraktion-Mitglied"
            tag = "✓" if sichtbar else "✗"
            extra = "" if sichtbar else f"  [GEFILTERT: {grund}]"
            ver = "vertraulich" if getattr(t, "fraktion_vertraulich", False) else "öffentlich"
            promo = "promoted" if getattr(t, "promoted_all_ovs", False) else "-"
            print(
                f"  {tag} #{t.id:>5} {t.starts_at:%Y-%m-%d %H:%M} | {ver} | {promo} |"
                f" {(t.title or '')[:60]}{extra}"
            )
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Aufruf: python -m scripts.diagnose_kreis_fraktion_feed <username>")
        sys.exit(2)
    sys.exit(diagnose(sys.argv[1]))
