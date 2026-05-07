"""Termin-Kategorien (Verband / Vorstand / Fraktion) und Sichtbarkeit."""

from __future__ import annotations

from sqlalchemy.orm import Session

from app.platform_models import OvMembership, Termin

TERMIN_KAT_VERBAND = "verband"
TERMIN_KAT_VORSTAND = "vorstand"
TERMIN_KAT_FRAKTION = "fraktion"

TERMIN_KATEGORIEN = (TERMIN_KAT_VERBAND, TERMIN_KAT_VORSTAND, TERMIN_KAT_FRAKTION)


def normalize_termin_kategorie(raw: str | None) -> str:
    s = (raw or "").strip().lower()
    return s if s in TERMIN_KATEGORIEN else TERMIN_KAT_VERBAND


def termin_kategorie_effective(t: Termin) -> str:
    """ORM-Spalte `termin_kategorie` oder Ableitung aus Legacy-Flags."""
    raw = getattr(t, "termin_kategorie", None)
    if raw and str(raw).strip():
        s = str(raw).strip().lower()
        if s in TERMIN_KATEGORIEN:
            return s
    if getattr(t, "is_fraktion_termin", False):
        if getattr(t, "fraktion_vertraulich", False):
            return TERMIN_KAT_FRAKTION
        return TERMIN_KAT_VERBAND
    return TERMIN_KAT_VERBAND


def _membership_approved(pdb: Session, user_id: int, ov_slug: str) -> OvMembership | None:
    ms = ov_slug.strip().lower()
    return (
        pdb.query(OvMembership)
        .filter(
            OvMembership.user_id == user_id,
            OvMembership.ov_slug == ms,
            OvMembership.is_approved.is_(True),
        )
        .first()
    )


def user_is_fraktionsmitglied(pdb: Session, user_id: int, ov_slug: str) -> bool:
    m = _membership_approved(pdb, user_id, ov_slug)
    return m is not None and bool(getattr(m, "fraktion_member", False))


def user_is_vorstandsmitglied(pdb: Session, user_id: int, ov_slug: str) -> bool:
    m = _membership_approved(pdb, user_id, ov_slug)
    return m is not None and bool(getattr(m, "vorstand_member", False))


def user_is_ov_admin_in(pdb: Session, user_id: int, ov_slug: str) -> bool:
    m = _membership_approved(pdb, user_id, ov_slug)
    return m is not None and bool(m.is_admin)


def user_darf_kategorie_anlegen(
    pdb: Session,
    *,
    user_id: int,
    ov_slug: str,
    kategorie: str,
) -> bool:
    """Wer darf einen Termin dieser Kategorie neu anlegen."""
    kat = normalize_termin_kategorie(kategorie)
    if kat == TERMIN_KAT_VERBAND:
        return _membership_approved(pdb, user_id, ov_slug) is not None
    if kat == TERMIN_KAT_VORSTAND:
        return user_is_vorstandsmitglied(pdb, user_id, ov_slug) or user_is_ov_admin_in(
            pdb, user_id, ov_slug
        )
    if kat == TERMIN_KAT_FRAKTION:
        return user_is_fraktionsmitglied(pdb, user_id, ov_slug) or user_is_ov_admin_in(
            pdb, user_id, ov_slug
        )
    return False


def termin_sichtbar_nach_kategorie(
    pdb: Session,
    t: Termin,
    *,
    user_id: int,
) -> bool:
    """Zusatzregeln nach Kategorie (Basis: Mitgliedschaft / Kreis bereits geprüft).

    Vorstand/Fraktion: nur echte Gruppenmitglieder — nicht automatisch nur wegen
    OV-Admin-Rolle. Plattform-Superadmins gelten hier wie normale Nutzer nach
    Mitgliedschaft/Rollen. Der Ersteller sieht seinen eigenen Termin weiterhin.
    """
    kat = termin_kategorie_effective(t)
    ms = t.mandant_slug.strip().lower()
    if kat == TERMIN_KAT_VERBAND:
        return True
    created_by = getattr(t, "created_by_id", None)
    if created_by is not None and created_by == user_id:
        return True
    if kat == TERMIN_KAT_VORSTAND:
        return user_is_vorstandsmitglied(pdb, user_id, ms)
    if kat == TERMIN_KAT_FRAKTION:
        return user_is_fraktionsmitglied(pdb, user_id, ms)
    return True


def filter_termine_fuer_ics(
    pdb: Session,
    termine: list[Termin],
    *,
    calendar_owner_user_id: int | None,
) -> list[Termin]:
    """Öffentlicher Feed ohne Nutzer: nur Verband. Persönliche Feeds: Kategorie + Rechte."""
    out: list[Termin] = []
    for t in termine:
        kat = termin_kategorie_effective(t)
        if kat == TERMIN_KAT_VERBAND:
            out.append(t)
            continue
        if calendar_owner_user_id is None:
            continue
        if termin_sichtbar_nach_kategorie(pdb, t, user_id=calendar_owner_user_id):
            out.append(t)
    return out


def apply_kategorie_to_termin_row(t: Termin, kategorie: str) -> None:
    """Persistiert Kategorie und hält Legacy-Spalten konsistent."""
    kat = normalize_termin_kategorie(kategorie)
    t.termin_kategorie = kat
    t.is_fraktion_termin = kat == TERMIN_KAT_FRAKTION
    t.fraktion_vertraulich = kat == TERMIN_KAT_FRAKTION
