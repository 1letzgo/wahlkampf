import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

# Plattform: Superadmin + Register der Ortsverbände
PLATFORM_DATABASE_PATH = Path(
    os.environ.get("PLATFORM_DATABASE_PATH", str(BASE_DIR / "platform.db"))
)

# Mandanten: je OV eigener Ordner mit SQLite + uploads/
MANDANTEN_ROOT = Path(os.environ.get("MANDANTEN_ROOT", str(BASE_DIR / "mandanten")))
# Fallback Slug nur für Migration / ICS ohne Session (ein OV pro öffentlicher ICS-URL)
DEFAULT_MANDANT_SLUG = os.environ.get("DEFAULT_MANDANT_SLUG", "westerstede").strip().lower()

SECRET_KEY = os.environ.get("SECRET_KEY", "dev-change-me-in-production")
SESSION_COOKIE = "wahlkampf_session"
ICS_TOKEN = os.environ.get("ICS_TOKEN", "")
MAX_UPLOAD_MB = int(os.environ.get("MAX_UPLOAD_MB", "8"))

# OV-Benutzer mit diesem Namen darf /admin/ortsverbaende (normale Session, kein separates Superadmin-Login)
SUPERADMIN_USERNAME = os.environ.get("SUPERADMIN_USERNAME", "letzgo").strip().lower()

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


# Öffentliche Domain(n): Host ist eingetragen → fester Mandanten-Slug, Kurz-URLs (/login statt /m/slug/login).
# PUBLIC_SITE_HOSTS=wahlkampf.spd-wst.de,wahlkamp.spd-wst.de PUBLIC_SITE_MANDANT_SLUG=westerstede
PUBLIC_SITE_HOSTS = _parse_public_site_hosts(
    os.environ.get("PUBLIC_SITE_HOSTS", ""),
)
PUBLIC_SITE_MANDANT_SLUG = os.environ.get("PUBLIC_SITE_MANDANT_SLUG", "").strip().lower()


def mandant_dir(slug: str) -> Path:
    return MANDANTEN_ROOT / slug.strip().lower()


def sqlite_database_path(slug: str) -> Path:
    return mandant_dir(slug) / "wahlkampf.db"


def upload_dir_for_slug(slug: str) -> Path:
    return mandant_dir(slug) / "uploads"
