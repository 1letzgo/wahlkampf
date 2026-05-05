"""Sichtbarkeit von Fraktionsterminen (ORM-Felder + ICS/Web konsistent)."""

from __future__ import annotations

from sqlalchemy.orm import Session

from app.config import is_superadmin_username
from app.platform_models import OvMembership, Termin


def user_is_fraktionsmitglied(pdb: Session, user_id: int, ov_slug: str) -> bool:
    ms = ov_slug.strip().lower()
    m = (
        pdb.query(OvMembership)
        .filter(
            OvMembership.user_id == user_id,
            OvMembership.ov_slug == ms,
            OvMembership.is_approved.is_(True),
            OvMembership.fraktion_member.is_(True),
        )
        .first()
    )
    return m is not None


def termin_fraktion_sichtbar_fuer_user(
    pdb: Session,
    t: Termin,
    *,
    user_id: int,
    username: str,
) -> bool:
    if not getattr(t, "is_fraktion_termin", False):
        return True
    if is_superadmin_username(username):
        return True
    if getattr(t, "fraktion_vertraulich", False):
        return user_is_fraktionsmitglied(pdb, user_id, t.mandant_slug)
    return True


def filter_termine_fraktion_ics(
    pdb: Session,
    termine: list[Termin],
    *,
    calendar_owner_user_id: int | None,
) -> list[Termin]:
    """Öffentlicher Mandanten-Feed: ohne Nutzer → keine vertraulichen Fraktionstermine."""
    out: list[Termin] = []
    for t in termine:
        if not getattr(t, "is_fraktion_termin", False):
            out.append(t)
            continue
        if not getattr(t, "fraktion_vertraulich", False):
            out.append(t)
            continue
        if calendar_owner_user_id is None:
            continue
        if user_is_fraktionsmitglied(pdb, calendar_owner_user_id, t.mandant_slug):
            out.append(t)
    return out
