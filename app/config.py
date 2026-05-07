from __future__ import annotations

import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

# Plattform: Superadmin + Register der Ortsverbände
PLATFORM_DATABASE_PATH = Path(
    os.environ.get("PLATFORM_DATABASE_PATH", str(BASE_DIR / "platform.db"))
)

# Mandanten: je OV eigener Ordner mit SQLite + uploads/
MANDANTEN_ROOT = Path(os.environ.get("MANDANTEN_ROOT", str(BASE_DIR / "mandanten")))
# Plattform-Standard-OV: Migration, Session-Fallback (deps), Kurzpfad /api/v1 → /m/<slug>/api/v1 (Mobile-API).
DEFAULT_MANDANT_SLUG = os.environ.get("DEFAULT_MANDANT_SLUG", "westerstede").strip().lower()

SECRET_KEY = os.environ.get("SECRET_KEY", "dev-change-me-in-production")
SESSION_COOKIE = "wahlkampf_session"
ICS_TOKEN = os.environ.get("ICS_TOKEN", "")
MAX_UPLOAD_MB = int(os.environ.get("MAX_UPLOAD_MB", "8"))

# Fraktions-Kalender-Abo (Webcal/ICS), Hintergrundabruf. 0 = aus.
# Intervall: CAL_FRAKTION_SYNC_INTERVAL_HOURS (Standard 6) oder Sekunden mit CAL_FRAKTION_SYNC_INTERVAL_SECONDS.
# Legacy: RSS_FRAKTION_IMPORT_INTERVAL_SECONDS (Sekunden) wird noch akzeptiert, wenn CAL_* nicht gesetzt.
def _cal_fraktion_sync_interval_seconds() -> int:
    raw_sec = os.environ.get("CAL_FRAKTION_SYNC_INTERVAL_SECONDS", "").strip()
    if raw_sec:
        return max(0, int(raw_sec))
    legacy = os.environ.get("RSS_FRAKTION_IMPORT_INTERVAL_SECONDS", "").strip()
    if legacy:
        return max(0, int(legacy))
    raw_h = os.environ.get("CAL_FRAKTION_SYNC_INTERVAL_HOURS", "6").strip()
    if raw_h in ("0", "0.0"):
        return 0
    try:
        h = float(raw_h)
    except ValueError:
        h = 6.0
    sec = int(h * 3600)
    return max(0, sec)


CAL_FRAKTION_SYNC_INTERVAL_SECONDS = _cal_fraktion_sync_interval_seconds()

CAL_FETCH_TIMEOUT_SECONDS = int(
    os.environ.get(
        "CAL_FETCH_TIMEOUT_SECONDS",
        os.environ.get("RSS_FETCH_TIMEOUT_SECONDS", "25"),
    )
)

# Superadmin: Plattform (/admin/ortsverbaende …). Nur über Env (wie PUBLIC_SITE_*), kein Hardcode.
# SUPERADMIN_USERNAME=einname oder SUPERADMIN_USERNAMES=a,b (Komma/Semikolon).
SUPERADMIN_USERNAME = os.environ.get("SUPERADMIN_USERNAME", "").strip().lower()
_super_raw = os.environ.get("SUPERADMIN_USERNAMES", "").strip()


def superadmin_usernames() -> frozenset[str]:
    names: set[str] = set()
    if SUPERADMIN_USERNAME:
        names.add(SUPERADMIN_USERNAME)
    if _super_raw:
        for part in _super_raw.replace(";", ",").split(","):
            u = part.strip().lower()
            if u:
                names.add(u)
    return frozenset(names)


def is_superadmin_username(username: str) -> bool:
    return username.strip().lower() in superadmin_usernames()

# Mandant per Hostname (optional): z. B. MANDANT_HOST_BASE_DOMAIN=localhost → westerstede.localhost:8000 → Slug westerstede
MANDANT_HOST_BASE_DOMAIN = os.environ.get("MANDANT_HOST_BASE_DOMAIN", "").strip().lower()
# Ohne echte Subdomain: gesamter Hostname = Slug (nur für lokale Tests), z. B. spd-wahlkampf:8000
MANDANT_HOST_IS_RAW_SLUG = os.environ.get("MANDANT_HOST_IS_RAW_SLUG", "").strip().lower() in (
    "1",
    "true",
    "yes",
)


def _parse_public_site_hosts(raw: str) -> frozenset[str]:
    out: set[str] = set()
    for part in raw.replace(";", ",").split(","):
        h = part.strip().lower().split(":")[0]
        if h:
            out.add(h)
    return frozenset(out)


# Optional: feste öffentliche Site — nur Browser-Kurz-URLs (/login statt /m/<slug>/login) auf diesen Hosts.
# Nicht nötig für die Mobile-API; dafür siehe DEFAULT_MANDANT_SLUG.
# PUBLIC_SITE_HOSTS=wahlkampf.spd-wst.de,wahlkamp.spd-wst.de PUBLIC_SITE_MANDANT_SLUG=westerstede
PUBLIC_SITE_HOSTS = _parse_public_site_hosts(
    os.environ.get("PUBLIC_SITE_HOSTS", ""),
)
PUBLIC_SITE_MANDANT_SLUG = os.environ.get("PUBLIC_SITE_MANDANT_SLUG", "").strip().lower()

# Kreis-/überörtlicher OV: Slug für projektweit sichtbare Termine (optional).
# Pro Aufruf aus os.environ lesen (Workers/Reload, gleiche Slugs wie ortsverbaende.slug).


def kreis_ov_slug() -> str | None:
    """Mandanten-Slug des Kreises oder None, wenn nicht konfiguriert."""
    raw = os.environ.get("WAHKAMPF_KREIS_OV_SLUG", "").strip()
    if len(raw) >= 2 and raw[0] == raw[-1] and raw[0] in "\"'":
        raw = raw[1:-1].strip()
    raw = raw.lower()
    return raw if raw else None


def mandant_dir(slug: str) -> Path:
    return MANDANTEN_ROOT / slug.strip().lower()


def sqlite_database_path(slug: str) -> Path:
    return mandant_dir(slug) / "wahlkampf.db"


def upload_dir_for_slug(slug: str) -> Path:
    return mandant_dir(slug) / "uploads"
