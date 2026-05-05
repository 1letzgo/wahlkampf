from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from icalendar import Calendar, Event
from sqlalchemy import and_, func, or_
from sqlalchemy.orm import Session

from app.config import kreis_ov_slug
from app.platform_models import TEILNAHME_STATUS_ZUGESAGT, Termin, TerminTeilnahme
from app.termin_extern import externe_teilnehmer_decode, externe_teilnehmer_labels

TZ = ZoneInfo("Europe/Berlin")


def build_ics_calendar(
    termine: list[Termin],
    cal_name: str = "SPD Wahlkampf",
    *,
    ov_labels_for_mandant_slug: dict[str, str] | None = None,
) -> bytes:
    cal = Calendar()
    cal.add("prodid", "-//SPD Ortsverein//Wahlkampf//DE")
    cal.add("version", "2.0")
    cal.add("calscale", "GREGORIAN")
    cal.add("x-wr-calname", cal_name)

    for t in sorted(termine, key=lambda x: x.starts_at):
        ev = Event()
        ev.add("uid", f"termin-{t.id}@wahlkampf")
        summary = t.title
        if ov_labels_for_mandant_slug:
            slug = t.mandant_slug.strip().lower()
            lab = ov_labels_for_mandant_slug.get(slug)
            if lab:
                summary = f"{lab}: {t.title}"
        ev.add("summary", summary)
        desc_parts: list[str] = []
        if t.description:
            desc_parts.append(t.description)
        if t.vorbereitung:
            desc_parts.append(f"Vorbereitung:\n{t.vorbereitung}")
        if t.nachbereitung:
            desc_parts.append(f"Nachbereitung:\n{t.nachbereitung}")
        ext_names = externe_teilnehmer_labels(
            externe_teilnehmer_decode(t.externe_teilnehmer_json),
        )
        if ext_names:
            desc_parts.append("Externe Gäste: " + ", ".join(ext_names))
        if desc_parts:
            ev.add("description", "\n\n".join(desc_parts))
        if t.location:
            ev.add("location", t.location)
        start = t.starts_at
        if start.tzinfo is None:
            start = start.replace(tzinfo=TZ)
        end = t.ends_at
        if end:
            if end.tzinfo is None:
                end = end.replace(tzinfo=TZ)
        else:
            end = start + timedelta(hours=1)
        ev.add("dtstart", start)
        ev.add("dtend", end)
        ev.add("dtstamp", datetime.now(TZ))
        cal.add_component(ev)

    return cal.to_ical()


def _mandanten_filter_for_feed(ms: str):
    ms = ms.strip().lower()
    ks = kreis_ov_slug()
    if ks and ms != ks:
        return or_(
            func.lower(Termin.mandant_slug) == ms,
            and_(
                Termin.promoted_all_ovs == True,  # noqa: E712
                func.lower(Termin.mandant_slug) == ks,
            ),
        )
    return func.lower(Termin.mandant_slug) == ms


def all_termine_for_feed(db: Session, mandant_slug: str) -> list[Termin]:
    ms = mandant_slug.strip().lower()
    return (
        db.query(Termin)
        .filter(_mandanten_filter_for_feed(ms))
        .order_by(Termin.starts_at.asc())
        .all()
    )


def termine_for_user_teilnahmen(db: Session, user_id: int, mandant_slug: str) -> list[Termin]:
    """Termine dieses Mandanten-Feeds mit Zusage des Nutzers (inkl. Kreis-promoted)."""
    ms = mandant_slug.strip().lower()
    return (
        db.query(Termin)
        .join(TerminTeilnahme, TerminTeilnahme.termin_id == Termin.id)
        .filter(
            TerminTeilnahme.user_id == user_id,
            TerminTeilnahme.teilnahme_status == TEILNAHME_STATUS_ZUGESAGT,
            _mandanten_filter_for_feed(ms),
        )
        .order_by(Termin.starts_at.asc())
        .all()
    )


def termine_zugesagt_multi_mandanten(
    db: Session, user_id: int, mandant_slugs: list[str]
) -> list[Termin]:
    if not mandant_slugs:
        return []
    slugs = [s.strip().lower() for s in mandant_slugs]
    ks = kreis_ov_slug()
    mandanten_cond = func.lower(Termin.mandant_slug).in_(slugs)
    if ks:
        mandanten_cond = or_(
            mandanten_cond,
            and_(
                Termin.promoted_all_ovs == True,  # noqa: E712
                func.lower(Termin.mandant_slug) == ks,
            ),
        )
    return (
        db.query(Termin)
        .join(TerminTeilnahme, TerminTeilnahme.termin_id == Termin.id)
        .filter(
            TerminTeilnahme.user_id == user_id,
            TerminTeilnahme.teilnahme_status == TEILNAHME_STATUS_ZUGESAGT,
            mandanten_cond,
        )
        .order_by(Termin.starts_at.asc())
        .all()
    )


def all_termine_multi_mandanten(db: Session, mandant_slugs: list[str]) -> list[Termin]:
    if not mandant_slugs:
        return []
    slugs = [s.strip().lower() for s in mandant_slugs]
    ks = kreis_ov_slug()
    mandanten_cond = func.lower(Termin.mandant_slug).in_(slugs)
    if ks:
        mandanten_cond = or_(
            mandanten_cond,
            and_(
                Termin.promoted_all_ovs == True,  # noqa: E712
                func.lower(Termin.mandant_slug) == ks,
            ),
        )
    return (
        db.query(Termin)
        .filter(mandanten_cond)
        .order_by(Termin.starts_at.asc())
        .all()
    )
