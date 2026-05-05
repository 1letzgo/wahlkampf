"""RSS-Fetch für Fraktionstermine (z. B. RIS „aktuelle Sitzungen“), dedupliziert über rss_import_key."""

from __future__ import annotations

import hashlib
import logging
import re
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime
from email.utils import parsedate_to_datetime
from urllib.parse import urlparse

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.config import RSS_FETCH_TIMEOUT_SECONDS
from app.mandant_features import FEATURE_FRAKTION, is_mandant_feature_enabled
from app.platform_models import Ortsverband, Termin

logger = logging.getLogger(__name__)

_DATUM_UHR_RE = re.compile(
    r"Datum:\s*(\d{1,2})\.(\d{1,2})\.(\d{4}).*?Uhrzeit:\s*(\d{1,2}):(\d{2})",
    re.IGNORECASE | re.DOTALL,
)
_RAUM_RE = re.compile(r"Raum:\s*([^<\n\r]+)", re.IGNORECASE)


def _local_tag(tag: str) -> str:
    if "}" in tag:
        return tag.rsplit("}", 1)[-1]
    return tag


def _children_named(parent: ET.Element, name: str):
    n = name.lower()
    for el in parent:
        if _local_tag(el.tag).lower() == n:
            yield el


def _elem_plain_text(el: ET.Element | None) -> str:
    if el is None:
        return ""
    parts = [el.text or ""]
    for c in el:
        parts.append(ET.tostring(c, encoding="unicode", method="text"))
    parts.append(el.tail or "")
    return "".join(parts).strip()


def _decode_xml_bytes(raw: bytes) -> str:
    head = raw[:400]
    enc_match = re.search(rb"encoding\s*=\s*['\"]([^'\"]+)['\"]", head, re.I)
    enc = "utf-8"
    if enc_match:
        try:
            enc = enc_match.group(1).decode("ascii", errors="ignore").strip().lower() or "utf-8"
        except Exception:
            enc = "utf-8"
    try:
        return raw.decode(enc)
    except UnicodeDecodeError:
        return raw.decode("utf-8", errors="replace")


def _html_to_text(html: str) -> str:
    if not html:
        return ""
    t = re.sub(r"(?i)<br\s*/?>", "\n", html)
    t = re.sub(r"<[^>]+>", " ", t)
    return " ".join(t.split())


def _parse_pub_date(raw: str | None) -> datetime | None:
    if not raw or not raw.strip():
        return None
    try:
        dt = parsedate_to_datetime(raw.strip())
        if dt.tzinfo:
            return dt.replace(tzinfo=None)
        return dt
    except (TypeError, ValueError):
        return None


def _starts_at_from_item(description_html: str, pub_date_raw: str | None) -> datetime | None:
    plain = _html_to_text(description_html)
    m = _DATUM_UHR_RE.search(description_html) or _DATUM_UHR_RE.search(plain)
    if m:
        d, mo, y, hh, mm = (int(m.group(i)) for i in range(1, 6))
        try:
            return datetime(y, mo, d, hh, mm, 0)
        except ValueError:
            pass
    return _parse_pub_date(pub_date_raw)


def _location_from_description(description_html: str) -> str:
    m = _RAUM_RE.search(description_html)
    if not m:
        return ""
    return " ".join(m.group(1).split())[:300]


def _item_dedupe_key(guid: str | None, link: str, title: str) -> str:
    g = (guid or "").strip()
    if g:
        if len(g) > 128:
            return hashlib.sha256(g.encode("utf-8", errors="replace")).hexdigest()
        return g
    base = f"{link}\0{title}".encode("utf-8", errors="replace")
    return hashlib.sha256(base).hexdigest()


def _truncate(s: str, max_len: int) -> str:
    s = s.strip()
    if len(s) <= max_len:
        return s
    return s[: max_len - 1].rstrip() + "…"


def fetch_rss_xml(feed_url: str, *, timeout: int | None = None) -> str:
    timeout = RSS_FETCH_TIMEOUT_SECONDS if timeout is None else timeout
    parsed = urlparse(feed_url.strip())
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise ValueError("Ungültige Feed-URL.")
    req = urllib.request.Request(
        feed_url.strip(),
        headers={
            "User-Agent": "Wahlkampf-Fraktion-RSS/1.0",
            "Accept": "application/rss+xml, application/xml, text/xml, */*",
        },
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
    return _decode_xml_bytes(raw)


def parse_rss_items(xml_text: str) -> list[dict]:
    root = ET.fromstring(xml_text)
    channels: list[ET.Element] = []
    if _local_tag(root.tag).lower() == "rss":
        channels.extend(_children_named(root, "channel"))
    elif _local_tag(root.tag).lower() == "feed":
        channels.append(root)
    out: list[dict] = []
    for channel in channels:
        for item in _children_named(channel, "item"):
            title_el = next(_children_named(item, "title"), None)
            link_el = next(_children_named(item, "link"), None)
            desc_el = next(_children_named(item, "description"), None)
            guid_el = next(_children_named(item, "guid"), None)
            pub_el = next(_children_named(item, "pubDate"), None)
            title = _elem_plain_text(title_el) or "(ohne Titel)"
            link = (_elem_plain_text(link_el) if link_el is not None else "").strip()
            description = _elem_plain_text(desc_el)
            guid = _elem_plain_text(guid_el) if guid_el is not None else ""
            pub_raw = _elem_plain_text(pub_el) if pub_el is not None else ""
            out.append(
                {
                    "title": title,
                    "link": link,
                    "description": description,
                    "guid": guid,
                    "pub_raw": pub_raw,
                }
            )
    return out


def import_fraktion_termine_from_feed(
    db: Session,
    mandant_slug: str,
    feed_url: str,
) -> tuple[int, str | None]:
    """Legt fehlende Fraktionstermine aus dem Feed an. Rückgabe: (Anzahl neu, Fehlertext oder None)."""
    ms = mandant_slug.strip().lower()
    if not is_mandant_feature_enabled(db, ms, FEATURE_FRAKTION):
        return 0, "Fraktion ist für diesen Ortsverband nicht aktiviert."

    url = feed_url.strip()
    if not url:
        return 0, "Keine Feed-URL konfiguriert."

    try:
        xml_text = fetch_rss_xml(url)
    except Exception as e:
        logger.warning("RSS fetch failed mandant=%s: %s", ms, e)
        return 0, f"Feed konnte nicht geladen werden: {e}"

    try:
        items = parse_rss_items(xml_text)
    except ET.ParseError as e:
        logger.warning("RSS parse failed mandant=%s: %s", ms, e)
        return 0, f"XML konnte nicht gelesen werden: {e}"

    created = 0
    for it in items:
        dedupe = _item_dedupe_key(it.get("guid"), it["link"], it["title"])
        exists = (
            db.query(Termin.id)
            .filter(Termin.mandant_slug == ms, Termin.rss_import_key == dedupe)
            .first()
        )
        if exists:
            continue

        starts_at = _starts_at_from_item(it["description"], it["pub_raw"])
        if starts_at is None:
            logger.debug("RSS item skipped (no datetime): %s", it["title"][:80])
            continue

        loc = _location_from_description(it["description"])
        desc_plain = _html_to_text(it["description"])
        body_parts = [desc_plain] if desc_plain else []
        if it["link"]:
            body_parts.append(it["link"])
        description = "\n\n".join(p for p in body_parts if p).strip()

        termin = Termin(
            mandant_slug=ms,
            title=_truncate(it["title"], 200),
            description=description[:20000] if len(description) > 20000 else description,
            location=_truncate(loc, 300),
            starts_at=starts_at,
            ends_at=None,
            created_by_id=None,
            is_fraktion_termin=True,
            fraktion_vertraulich=False,
            rss_import_key=dedupe,
        )
        db.add(termin)
        try:
            db.commit()
            created += 1
        except IntegrityError:
            db.rollback()

    return created, None


def run_all_fraktion_rss_imports() -> None:
    """Alle OVs mit konfigurierter Feed-URL und aktivem FEATURE_FRAKTION."""
    from sqlalchemy.orm import sessionmaker

    from app.platform_database import platform_engine

    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=platform_engine())
    db = SessionLocal()
    try:
        ovs = (
            db.query(Ortsverband)
            .filter(
                Ortsverband.fraktion_rss_feed_url.isnot(None),
                Ortsverband.fraktion_rss_feed_url != "",
            )
            .all()
        )
        for ov in ovs:
            url = (ov.fraktion_rss_feed_url or "").strip()
            if not url:
                continue
            if not is_mandant_feature_enabled(db, ov.slug, FEATURE_FRAKTION):
                continue
            n, err = import_fraktion_termine_from_feed(db, ov.slug, url)
            if err:
                logger.info(
                    "RSS import mandant=%s created=%s msg=%s",
                    ov.slug,
                    n,
                    err,
                )
            elif n:
                logger.info("RSS import mandant=%s created=%s", ov.slug, n)
    finally:
        db.close()
