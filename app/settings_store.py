from __future__ import annotations

import secrets
from typing import Optional

from sqlalchemy.orm import Session

from app.platform_models import MandantAppSetting, PlatformUser

ICS_KEY = "ics_token"


def ics_token_value(pdb: Session, mandant_slug: str, env_token: str) -> str | None:
    if env_token.strip():
        return env_token.strip()
    slug = mandant_slug.strip().lower()
    row = pdb.get(MandantAppSetting, (slug, ICS_KEY))
    return row.value if row else None


def ensure_ics_token_for_ui(pdb: Session, mandant_slug: str, env_token: str) -> str:
    if env_token.strip():
        return env_token.strip()
    slug = mandant_slug.strip().lower()
    row = pdb.get(MandantAppSetting, (slug, ICS_KEY))
    if row:
        return row.value
    token = secrets.token_urlsafe(32)
    pdb.add(MandantAppSetting(mandant_slug=slug, key=ICS_KEY, value=token))
    pdb.commit()
    return token


def verify_ics_token(
    pdb: Session,
    mandant_slug: str,
    env_token: str,
    provided: Optional[str],
) -> bool:
    if not provided:
        return False
    expected = ics_token_value(pdb, mandant_slug, env_token)
    if not expected:
        return False
    return secrets.compare_digest(provided, expected)


def ensure_user_calendar_token(pdb: Session, user: PlatformUser) -> str:
    """Geheimer Token für den persönlichen Kalender-Feed (nur zugesagte Termine)."""
    if user.calendar_token:
        return user.calendar_token
    for _ in range(24):
        token = secrets.token_urlsafe(18)
        clash = (
            pdb.query(PlatformUser)
            .filter(PlatformUser.calendar_token == token)
            .first()
        )
        if not clash:
            user.calendar_token = token
            pdb.commit()
            pdb.refresh(user)
            return token
    raise RuntimeError("Kalender-Token konnte nicht erzeugt werden.")
