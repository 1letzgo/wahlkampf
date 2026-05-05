"""Pro-Mandant aktivierbare Funktionen (Schalter für Superadmins)."""

from __future__ import annotations

from sqlalchemy.orm import Session

from app.platform_models import MandantAppSetting

FEATURE_PLAKATE = "feature_plakate"
FEATURE_SHAREPIC = "feature_sharepic"
FEATURE_FRAKTION = "feature_fraktion"


def is_mandant_feature_enabled(
    pdb: Session,
    mandant_slug: str,
    key: str,
    *,
    default: bool = True,
) -> bool:
    """Kein Eintrag oder leer → default (True), sonst nur bei explizitem Abschalten aus."""
    ms = mandant_slug.strip().lower()
    row = pdb.get(MandantAppSetting, (ms, key))
    if row is None:
        return default
    v = (row.value or "").strip().lower()
    if v in ("0", "false", "off", "no", ""):
        return False
    return True


def merge_mandant_feature(pdb: Session, mandant_slug: str, key: str, enabled: bool) -> None:
    ms = mandant_slug.strip().lower()
    pdb.merge(MandantAppSetting(mandant_slug=ms, key=key, value="1" if enabled else "0"))
