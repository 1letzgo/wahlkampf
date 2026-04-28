from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from icalendar import Calendar, Event
from sqlalchemy.orm import Session

from app.models import Termin, TerminTeilnahme
from app.termin_extern import externe_teilnehmer_decode, externe_teilnehmer_labels

TZ = ZoneInfo("Europe/Berlin")


def build_ics_calendar(termine: list[Termin], cal_name: str = "SPD Wahlkampf") -> bytes:
    cal = Calendar()
    cal.add("prodid", "-//SPD Ortsverein//Wahlkampf//DE")
    cal.add("version", "2.0")
    cal.add("calscale", "GREGORIAN")
    cal.add("x-wr-calname", cal_name)

    for t in sorted(termine, key=lambda x: x.starts_at):
        ev = Event()
        ev.add("uid", f"termin-{t.id}@wahlkampf")
        ev.add("summary", t.title)
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


def all_termine_for_feed(db: Session) -> list[Termin]:
    return db.query(Termin).order_by(Termin.starts_at.asc()).all()


def termine_for_user_teilnahmen(db: Session, user_id: int) -> list[Termin]:
    """Nur Termine, für die der User eine Teilnahme (Zusage) eingetragen hat."""
    return (
        db.query(Termin)
        .join(TerminTeilnahme, TerminTeilnahme.termin_id == Termin.id)
        .filter(TerminTeilnahme.user_id == user_id)
        .order_by(Termin.starts_at.asc())
        .all()
    )
