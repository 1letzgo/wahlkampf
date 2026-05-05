"""ICS/Webcal-Abo → Fraktionstermine (dedupliziert über cal_import_key)."""

from __future__ import annotations

import hashlib
import logging
import re
import urllib.request
from datetime import date, datetime, time, timezone
from typing import Any
from urllib.parse import urlparse

from icalendar import Calendar
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.config import CAL_FETCH_TIMEOUT_SECONDS
from app.mandant_features import FEATURE_FRAKTION, is_mandant_feature_enabled
from app.platform_models import Ortsverband, Termin

logger = logging.getLogger(__name__)


def validate_and_normalize_cal_subscription_url(raw: str) -> tuple[str | None, str | None]:
    """Speicherformat: https/http (webcal:// wird normalisiert). Leer → Abo ohne URL."""
    s = (raw or "").strip()
    if not s:
        return None, None
    if len(s) > 8000:
        return None, "Die Kalender-URL ist zu lang."
    if s.lower().startswith("webcal://"):
        s = "https://" + s[len("webcal://") :]
    p = urlparse(s)
    if p.scheme not in ("http", "https") or not p.netloc:
        return None, "Bitte eine http(s)- oder webcal://-Kalenderadresse angeben."
    return s, None


def normalize_calendar_fetch_url(raw: str) -> str:
    """webcal:// → https:// für HTTP-Abruf."""
    s = (raw or "").strip()
    if s.lower().startswith("webcal://"):
        return "https://" + s[len("webcal://") :]
    return s


def fetch_ics_bytes(cal_url: str, *, timeout: int | None = None) -> bytes:
    timeout = CAL_FETCH_TIMEOUT_SECONDS if timeout is None else timeout
    fetch_u = normalize_calendar_fetch_url(cal_url.strip())
    parsed = urlparse(fetch_u)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise ValueError("Ungültige Kalender-URL.")
    req = urllib.request.Request(
        fetch_u,
        headers={
            "User-Agent": "Wahlkampf-Fraktion-Cal/1.0",
            "Accept": "text/calendar, application/calendar+json, */*",
        },
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def _aware_to_naive_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def _dtstart_to_datetime(val: Any) -> datetime | None:
    if isinstance(val, datetime):
        return _aware_to_naive_utc(val)
    if isinstance(val, date):
        return datetime.combine(val, time.min)
    return None


def _prop_as_str(component, name: str, max_len: int) -> str:
    raw = component.get(name)
    if raw is None:
        return ""
    s = str(raw).strip()
    return s[:max_len] if len(s) > max_len else s


def _event_import_key(uid: str, starts_at: datetime, recurrence_id: Any) -> str:
    rid = ""
    if recurrence_id is not None:
        try:
            dt = recurrence_id.dt
            rid = dt.isoformat() if hasattr(dt, "isoformat") else str(dt)
        except Exception:
            rid = str(recurrence_id)
    base = f"{uid}\n{starts_at.isoformat()}\n{rid}".encode("utf-8", errors="replace")
    return hashlib.sha256(base).hexdigest()


def _norm_url_token(u: str) -> str:
    return u.strip().rstrip(").,]>;")


def _ris_link_detail_score(url: str) -> int:
    """Höher = sitzungsspezifischer Link (to010/SILFDNR) vor Monatskalender si010_e."""
    if not url:
        return 0
    u = url.lower()
    q = urlparse(url).query.lower()
    if "to010.asp" in u and "silfdnr=" in q:
        return 400
    if "silfdnr=" in q or "volfdnr=" in q:
        return 350
    if "si010" in u and "dd=" in q:
        return 120
    # Nur Monat/Jahr = Kalenderübersicht (mehrere Termine teilen oft dieselbe URL)
    if "si010_e.asp" in u and "mm=" in q and "yy=" in q:
        return 25
    if "si010" in u:
        return 40
    if "ris." in u and "/bi/" in u:
        return 15
    return 5


def _pick_best_http_url(urls: list[str]) -> str | None:
    if not urls:
        return None
    return max(urls, key=_ris_link_detail_score)


def _allris_synthesize_to010_from_uid(uid_raw: str, reference_http_url: str | None) -> str | None:
    """UID wie ALLRIS-Sitzung-426 → to010.asp?SILFDNR=426 (wenn Referenz-Host aus Feed)."""
    m = re.match(r"(?i)^ALLRIS-Sitzung-(\d+)\s*$", (uid_raw or "").strip())
    if not m:
        return None
    silfdnr = m.group(1)
    ref = reference_http_url or ""
    if ref:
        p = urlparse(ref)
        if p.scheme in ("http", "https") and p.netloc:
            return f"{p.scheme}://{p.netloc}/bi/to010.asp?SILFDNR={silfdnr}"
    return None


def extract_description_and_link_from_ics_description(
    raw: str,
    *,
    calendar_uid: str = "",
) -> tuple[str, str | None]:
    """Entfernt ALLRIS-Boilerplate aus DESCRIPTION und liefert den besten Sitzungs-Link."""
    text = (raw or "").strip()
    if not text:
        return "", None

    urls = [
        _norm_url_token(m.group(0))
        for m in re.finditer(r"https?://[^\s<>]+", text, flags=re.I)
    ]
    link_pick = _pick_best_http_url(urls)
    if link_pick is None or _ris_link_detail_score(link_pick) <= 25:
        synth = _allris_synthesize_to010_from_uid(calendar_uid, link_pick or (urls[0] if urls else None))
        if synth:
            link_pick = synth[:2000]

    link = link_pick

    lines = text.splitlines()
    out: list[str] = []
    i = 0
    while i < len(lines):
        st = lines[i].strip()
        if re.match(r"(?i)^exportiert aus allris am\b", st):
            i += 1
            continue
        if re.match(r"(?i)^die sitzung in allris net:?\s*$", st):
            i += 1
            while i < len(lines) and not lines[i].strip():
                i += 1
            if i < len(lines):
                cand = _norm_url_token(lines[i].strip())
                if cand.lower().startswith("http"):
                    # Link kommt bereits aus URLs/UID-Synthese — Zeile nur entfernen
                    i += 1
                    continue
            continue
        if link and st.lower().startswith("http"):
            cu = _norm_url_token(st).lower()
            # Alle im Text vorkommenden RIS-URLs aus Beschreibung entfernen (auch wenn wir auf to010 synthetisiert haben)
            for u in urls:
                if cu == _norm_url_token(u).lower():
                    i += 1
                    break
            else:
                out.append(lines[i])
                i += 1
            continue
        out.append(lines[i])
        i += 1

    desc = "\n".join(out).strip()
    desc = re.sub(r"\n{3,}", "\n\n", desc)
    if link and len(link) > 2000:
        link = link[:2000]

    # Falls Boilerplate keine URL hatte, aber UID synthetisierbar: nur wenn noch kein starker Link
    if (link is None or _ris_link_detail_score(link) <= 25) and urls:
        synth2 = _allris_synthesize_to010_from_uid(calendar_uid, urls[0])
        if synth2:
            link = synth2[:2000]

    return desc, link


def parse_vevents_from_ics(raw: bytes) -> list[dict]:
    cal = Calendar.from_ical(raw)
    out: list[dict] = []
    for component in cal.walk():
        if component.name != "VEVENT":
            continue
        if component.get("rrule"):
            logger.debug("Kalender: VEVENT mit RRULE übersprungen (nicht expandiert).")
            continue
        dtstart_prop = component.get("dtstart")
        if dtstart_prop is None:
            continue
        dt_raw = dtstart_prop.dt
        starts_at = _dtstart_to_datetime(dt_raw)
        if starts_at is None:
            continue

        uid = _prop_as_str(component, "uid", 500)
        if not uid:
            uid = f"noid-{starts_at.isoformat()}"

        ends_at: datetime | None = None
        dtend_prop = component.get("dtend")
        if dtend_prop is not None:
            ends_at = _dtstart_to_datetime(dtend_prop.dt)

        title = _prop_as_str(component, "summary", 200) or "(Kalender)"
        location = _prop_as_str(component, "location", 300)
        desc_raw = _prop_as_str(component, "description", 20000)
        desc_clean, link_from_desc = extract_description_and_link_from_ics_description(
            desc_raw,
            calendar_uid=uid,
        )

        link_prop = _prop_as_str(component, "url", 2000).strip()
        link_cands: list[str] = []
        for cand in (link_prop, link_from_desc):
            if not cand:
                continue
            p = urlparse(cand.strip())
            if p.scheme in ("http", "https") and p.netloc:
                link_cands.append(_norm_url_token(cand.strip()))
        link_final = _pick_best_http_url(link_cands)
        if link_final:
            link_final = link_final[:2000]

        out.append(
            {
                "title": title,
                "location": location,
                "description": desc_clean,
                "link_url": link_final,
                "starts_at": starts_at,
                "ends_at": ends_at,
                "import_key": _event_import_key(
                    uid,
                    starts_at,
                    component.get("recurrence-id"),
                ),
            }
        )
    return out


def import_fraktion_termine_from_calendar(
    db: Session,
    mandant_slug: str,
    cal_url: str,
) -> tuple[int, str | None]:
    """Legt fehlende Fraktionstermine aus ICS/Webcal an."""
    ms = mandant_slug.strip().lower()
    if not is_mandant_feature_enabled(db, ms, FEATURE_FRAKTION):
        return 0, "Fraktion ist für diesen Ortsverband nicht aktiviert."

    url = cal_url.strip()
    if not url:
        return 0, "Keine Kalender-URL konfiguriert."

    try:
        raw = fetch_ics_bytes(url)
    except Exception as e:
        logger.warning("Kalender fetch failed mandant=%s: %s", ms, e)
        return 0, f"Kalender konnte nicht geladen werden: {e}"

    try:
        events = parse_vevents_from_ics(raw)
    except Exception as e:
        logger.warning("Kalender parse failed mandant=%s: %s", ms, e)
        return 0, f"Kalender konnte nicht gelesen werden: {e}"

    created = 0
    for ev in events:
        dedupe = ev["import_key"]
        exists = (
            db.query(Termin.id)
            .filter(Termin.mandant_slug == ms, Termin.cal_import_key == dedupe)
            .first()
        )
        if exists:
            continue

        title = ev["title"].strip() or "(Kalender)"
        desc = (ev["description"] or "").strip()
        loc = (ev["location"] or "").strip()

        termin = Termin(
            mandant_slug=ms,
            title=title[:200],
            description=desc[:20000] if len(desc) > 20000 else desc,
            location=loc[:300],
            starts_at=ev["starts_at"],
            ends_at=ev["ends_at"],
            created_by_id=None,
            is_fraktion_termin=True,
            fraktion_vertraulich=False,
            cal_import_key=dedupe,
            link_url=(ev.get("link_url") or None),
        )
        db.add(termin)
        try:
            db.commit()
            created += 1
        except IntegrityError:
            db.rollback()

    return created, None


def run_all_fraktion_cal_subscriptions() -> None:
    """Alle aktiven Abos (URL + Abo an + FEATURE_FRAKTION)."""
    from sqlalchemy.orm import sessionmaker

    from app.platform_database import platform_engine

    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=platform_engine())
    db = SessionLocal()
    try:
        ovs = (
            db.query(Ortsverband)
            .filter(
                Ortsverband.fraktion_cal_feed_url.isnot(None),
                Ortsverband.fraktion_cal_feed_url != "",
                Ortsverband.fraktion_cal_abo_active.is_(True),
            )
            .all()
        )
        for ov in ovs:
            url = (ov.fraktion_cal_feed_url or "").strip()
            if not url:
                continue
            if not is_mandant_feature_enabled(db, ov.slug, FEATURE_FRAKTION):
                continue
            n, err = import_fraktion_termine_from_calendar(db, ov.slug, url)
            if err:
                logger.info(
                    "Kalender mandant=%s created=%s msg=%s",
                    ov.slug,
                    n,
                    err,
                )
            elif n:
                logger.info("Kalender mandant=%s created=%s", ov.slug, n)
    finally:
        db.close()
