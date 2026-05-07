"""Externe Feeds (ICS/Webcal oder RSS, z. B. Bürgerinfo Ammerland) → Termine im OV; Neu + Aktualisieren."""

from __future__ import annotations

import hashlib
import logging
import re
import urllib.request
import xml.etree.ElementTree as ET
from datetime import date, datetime, time, timezone
from typing import Any
from urllib.parse import urlparse

from icalendar import Calendar
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.config import CAL_FETCH_TIMEOUT_SECONDS
from app.platform_models import ExternCalSubscription, Termin
from app.termin_kategorie import apply_kategorie_to_termin_row, normalize_termin_kategorie

logger = logging.getLogger(__name__)


def validate_and_normalize_cal_subscription_url(raw: str) -> tuple[str | None, str | None]:
    """Speicherformat: https/http (webcal:// wird normalisiert). Leer → Abo ohne URL."""
    s = (raw or "").strip()
    if not s:
        return None, None
    if len(s) > 8000:
        return None, "Die Feed-URL ist zu lang."
    if s.lower().startswith("webcal://"):
        s = "https://" + s[len("webcal://") :]
    p = urlparse(s)
    if p.scheme not in ("http", "https") or not p.netloc:
        return None, "Bitte eine http(s)-, webcal://- oder RSS-Adresse angeben."
    return s, None


def normalize_calendar_fetch_url(raw: str) -> str:
    """webcal:// → https:// für HTTP-Abruf."""
    s = (raw or "").strip()
    if s.lower().startswith("webcal://"):
        return "https://" + s[len("webcal://") :]
    return s


def fetch_ics_bytes(cal_url: str, *, timeout: int | None = None) -> bytes:
    """Alias für :func:`fetch_subscription_bytes` (bestehende Aufrufer)."""
    return fetch_subscription_bytes(cal_url, timeout=timeout)


def fetch_subscription_bytes(feed_url: str, *, timeout: int | None = None) -> bytes:
    """Lädt Rohbytes (ICS, RSS/XML …)."""
    timeout = CAL_FETCH_TIMEOUT_SECONDS if timeout is None else timeout
    fetch_u = normalize_calendar_fetch_url(feed_url.strip())
    parsed = urlparse(fetch_u)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise ValueError("Ungültige Feed-URL.")
    req = urllib.request.Request(
        fetch_u,
        headers={
            "User-Agent": "Wahlkampf-FeedImport/1.0",
            "Accept": (
                "text/calendar, application/calendar+json, "
                "application/rss+xml, application/atom+xml, application/xml, text/xml, */*"
            ),
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

        link_score = _ris_link_detail_score(link_final) if link_final else 0
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
                "match_link": link_final
                if link_final and link_score >= 100
                else None,
            }
        )
    return out


def _fix_malformed_item_url(raw: str) -> str | None:
    """Korrigiert z. B. ``http:'host/...`` aus manchen RIS/RSS-Feeds."""
    s = (raw or "").strip()
    if not s:
        return None
    s = re.sub(r"(?i)^http:'", "http://", s)
    s = re.sub(r"(?i)^https:'", "https://", s)
    p = urlparse(s)
    if p.scheme in ("http", "https") and p.netloc:
        return s[:2000]
    return None


def _rss_element_text(el: ET.Element | None) -> str:
    if el is None:
        return ""
    return "".join(el.itertext()).strip()


def _datetime_from_title_suffix(title: str) -> datetime | None:
    m = re.search(r"(\d{1,2})\.(\d{1,2})\.(\d{4})\s*$", (title or "").strip())
    if not m:
        return None
    day, mon, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
    try:
        return datetime(year, mon, day, 10, 0, 0)
    except ValueError:
        return None


def _parse_ammerland_sitzung_datetime(desc: str, title: str) -> tuple[datetime | None, str]:
    """Datum/Zeit/Ort aus Bürgerinfo/RSS-Beschreibung (Landkreis Ammerland u. ä.)."""
    d = (desc or "").strip()
    m = re.search(
        r"(?is)Datum:\s*(\d{1,2})\.(\d{1,2})\.(\d{4})\s+"
        r"Zeit:\s*(\d{1,2}):(\d{2})(?:\s*Uhr)?\s+"
        r"Ort:\s*(.+)$",
        d,
    )
    if m:
        day, mon, year, th, tm = (int(m.group(i)) for i in range(1, 6))
        ort = m.group(6).strip()
        try:
            return datetime(year, mon, day, th, tm, 0), ort[:600]
        except ValueError:
            pass
    m2 = re.search(r"(?i)Datum:\s*(\d{1,2})\.(\d{1,2})\.(\d{4})", d)
    if m2:
        day, mon, year = int(m2.group(1)), int(m2.group(2)), int(m2.group(3))
        mzeit = re.search(r"(?i)Zeit:\s*(\d{1,2}):(\d{2})", d)
        th, tm = (10, 0)
        if mzeit:
            th, tm = int(mzeit.group(1)), int(mzeit.group(2))
        m_ort = re.search(r"(?is)Ort:\s*(.+)$", d)
        ort = m_ort.group(1).strip() if m_ort else ""
        try:
            return datetime(year, mon, day, th, tm, 0), ort[:600]
        except ValueError:
            pass
    dt_title = _datetime_from_title_suffix(title)
    if dt_title:
        return dt_title, ""
    return None, ""


def _rss_import_key(
    link: str | None, guid: str, title: str, starts_at: datetime
) -> str:
    """Stabile Identität bevorzugt per Detail-URL oder RSS-GUID — nicht bei jedem Titel-/Zeit-Patch neuer Hash."""
    link_s = _norm_url_token((link or "").strip())
    guid_s = (guid or "").strip()[:800]
    if link_s and _ris_link_detail_score(link_s) >= 100:
        base = f"rss|url|{link_s}"
    elif guid_s:
        base = f"rss|guid|{guid_s}"
    elif link_s:
        base = f"rss|urlweak|{link_s}|{title[:240]}|{starts_at.isoformat()}"
    else:
        base = f"rss|fallback|{title[:300]}|{starts_at.isoformat()}"
    return hashlib.sha256(base.encode("utf-8", errors="replace")).hexdigest()


def parse_rss_buergerinfo_items(raw: bytes) -> tuple[list[dict], str | None]:
    """RSS mit Sitzungs-Items (Bürgerinfo Ammerland: Gremium/Datum/Zeit/Ort in description)."""
    try:
        root = ET.fromstring(raw)
    except ET.ParseError as e:
        return [], f"RSS/XML konnte nicht gelesen werden: {e}"
    out: list[dict] = []
    for item in root.findall(".//item"):
        title = _rss_element_text(item.find("title"))[:500]
        desc = _rss_element_text(item.find("description"))
        link_raw = _rss_element_text(item.find("link"))
        link_final = _fix_malformed_item_url(link_raw)
        guid_el = item.find("guid")
        guid_raw = (
            _rss_element_text(guid_el).strip()[:800] if guid_el is not None else ""
        )
        starts_at, location = _parse_ammerland_sitzung_datetime(desc, title)
        if starts_at is None:
            logger.debug("RSS item übersprungen (kein Datum): %s", title[:120] if title else "")
            continue
        # Titel, Ort, Zeit und Link kommen aus strukturierten Feldern — Beschreibung nur Platzhalter.
        description = "Sitzung"
        t_short = (title or "(RSS)").strip()[:200] or "(RSS)"
        import_key = _rss_import_key(link_final, guid_raw, t_short, starts_at)
        link_score = _ris_link_detail_score(link_final) if link_final else 0
        out.append(
            {
                "title": t_short,
                "location": (location or "")[:300],
                "description": description,
                "link_url": link_final,
                "starts_at": starts_at,
                "ends_at": None,
                "import_key": import_key,
                "match_link": link_final
                if link_final and link_score >= 100
                else None,
            }
        )
    if not out:
        return [], (
            "RSS-Feed enthält keine importierbaren Einträge "
            "(erwartet werden Sitzungen mit Datum/Zeit in der Beschreibung oder im Titel)."
        )
    return out, None


def parse_feed_to_events(raw: bytes) -> tuple[list[dict], str | None]:
    """Erkennt ICS oder RSS und liefert eine einheitliche Event-Liste."""
    head = raw[:16000].lstrip(b"\xef\xbb\xbf").decode("utf-8", errors="replace")
    hl = head.lower()
    if "begin:vcalendar" in hl:
        try:
            return parse_vevents_from_ics(raw), None
        except Exception as e:
            logger.warning("ICS parse failed: %s", e)
            return [], f"Kalender konnte nicht gelesen werden: {e}"
    if "<rss" in hl or ("<channel" in hl and "<item" in hl):
        return parse_rss_buergerinfo_items(raw)
    try:
        ev = parse_vevents_from_ics(raw)
        if ev:
            return ev, None
    except Exception:
        pass
    ev, err = parse_rss_buergerinfo_items(raw)
    if ev:
        return ev, None
    return [], err or "Unbekanntes Feed-Format (weder ICS noch unterstütztes RSS)."


def _dt_same(a: datetime | None, b: datetime | None) -> bool:
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    na = a.replace(tzinfo=None, microsecond=0)
    nb = b.replace(tzinfo=None, microsecond=0)
    return na == nb


def _apply_feed_event_to_termin(t: Termin, ev: dict, kat: str) -> bool:
    """Übernimmt Feed-Felder; True bei Änderung (inkl. Kategorie laut Abo)."""
    nk = normalize_termin_kategorie(kat)
    title = (ev.get("title") or "").strip() or "(Feed)"
    title = title[:200]
    desc = (ev.get("description") or "").strip()
    if len(desc) > 20000:
        desc = desc[:20000]
    loc = (ev.get("location") or "").strip()[:300]
    starts_at: datetime = ev["starts_at"]
    ends_at: datetime | None = ev.get("ends_at")
    link = (ev.get("link_url") or "").strip() or None
    if link and len(link) > 2000:
        link = link[:2000]

    changed = False
    if t.title != title:
        t.title = title
        changed = True
    if (t.description or "") != desc:
        t.description = desc
        changed = True
    if (t.location or "") != loc:
        t.location = loc
        changed = True
    if not _dt_same(t.starts_at, starts_at):
        t.starts_at = starts_at
        changed = True
    if not _dt_same(t.ends_at, ends_at):
        t.ends_at = ends_at
        changed = True
    cur_link = (t.link_url or "").strip() or None
    if cur_link != link:
        t.link_url = link
        changed = True
    if normalize_termin_kategorie(getattr(t, "termin_kategorie", None)) != nk:
        changed = True
    apply_kategorie_to_termin_row(t, nk)
    return changed


def _persist_feed_events(
    db: Session,
    mandant_slug: str,
    events: list[dict],
    kat: str,
) -> tuple[int, int]:
    """Legt neue Termine an und aktualisiert bestehende (Import-Key oder eindeutiger Detail-Link)."""
    ms = mandant_slug.strip().lower()
    created = 0
    updated = 0
    for ev in events:
        dedupe = ev["import_key"]
        termin = (
            db.query(Termin)
            .filter(Termin.mandant_slug == ms, Termin.cal_import_key == dedupe)
            .first()
        )
        ml = (ev.get("match_link") or "").strip() or None
        if termin is None and ml:
            cands = (
                db.query(Termin)
                .filter(
                    Termin.mandant_slug == ms,
                    Termin.link_url == ml,
                    Termin.cal_import_key.isnot(None),
                )
                .all()
            )
            if len(cands) == 1:
                termin = cands[0]

        if termin is not None:
            ch = _apply_feed_event_to_termin(termin, ev, kat)
            if termin.cal_import_key != dedupe:
                termin.cal_import_key = dedupe
                ch = True
            if ch:
                try:
                    db.commit()
                    updated += 1
                except IntegrityError:
                    db.rollback()
            continue

        title = ev["title"].strip() or "(Feed)"
        desc = (ev["description"] or "").strip()
        loc = (ev["location"] or "").strip()

        new_t = Termin(
            mandant_slug=ms,
            title=title[:200],
            description=desc[:20000] if len(desc) > 20000 else desc,
            location=loc[:300],
            starts_at=ev["starts_at"],
            ends_at=ev["ends_at"],
            created_by_id=None,
            cal_import_key=dedupe,
            link_url=(ev.get("link_url") or None),
        )
        apply_kategorie_to_termin_row(new_t, kat)
        db.add(new_t)
        try:
            db.commit()
            created += 1
        except IntegrityError:
            db.rollback()
    return created, updated


def import_fraktion_termine_from_calendar(
    db: Session,
    mandant_slug: str,
    cal_url: str,
    *,
    termin_kategorie: str = "verband",
) -> tuple[int, int, str | None]:
    """Import aus ICS/Webcal oder RSS: neue Termine + Aktualisierung bestehender Einträge."""
    ms = mandant_slug.strip().lower()
    url = cal_url.strip()
    if not url:
        return 0, 0, "Keine Feed-URL konfiguriert."

    kat = normalize_termin_kategorie(termin_kategorie)

    try:
        raw = fetch_subscription_bytes(url)
    except Exception as e:
        logger.warning("Feed fetch failed mandant=%s: %s", ms, e)
        return 0, 0, f"Feed konnte nicht geladen werden: {e}"

    events, perr = parse_feed_to_events(raw)
    if perr:
        return 0, 0, perr

    created, updated = _persist_feed_events(db, ms, events, kat)
    return created, updated, None


def run_all_fraktion_cal_subscriptions() -> None:
    """Alle aktiven Plattform-Kalender-Abos (URL + Abo an) → Ziel-Ortsverband."""
    from sqlalchemy.orm import sessionmaker

    from app.platform_database import platform_engine

    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=platform_engine())
    db = SessionLocal()
    try:
        subs = (
            db.query(ExternCalSubscription)
            .filter(
                ExternCalSubscription.feed_url.isnot(None),
                ExternCalSubscription.feed_url != "",
                ExternCalSubscription.abo_active.is_(True),
            )
            .all()
        )
        for sub in subs:
            url = (sub.feed_url or "").strip()
            if not url:
                continue
            ms = sub.mandant_slug.strip().lower()
            n, u, err = import_fraktion_termine_from_calendar(
                db,
                ms,
                url,
                termin_kategorie=sub.termin_kategorie or "verband",
            )
            if err:
                logger.info(
                    "Kalender sub_id=%s mandant=%s created=%s updated=%s msg=%s",
                    sub.id,
                    ms,
                    n,
                    u,
                    err,
                )
            elif n or u:
                logger.info(
                    "Kalender sub_id=%s mandant=%s created=%s updated=%s",
                    sub.id,
                    ms,
                    n,
                    u,
                )
    finally:
        db.close()
