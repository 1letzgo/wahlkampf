import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

DATABASE_URL = os.environ.get(
    "DATABASE_URL", f"sqlite:///{BASE_DIR / 'wahlkampf.db'}"
)
PLAKATE_DATABASE_URL = os.environ.get(
    "PLAKATE_DATABASE_URL",
    f"sqlite:///{BASE_DIR / 'plakate.db'}",
)
SECRET_KEY = os.environ.get("SECRET_KEY", "dev-change-me-in-production")
SESSION_COOKIE = "wahlkampf_session"
ICS_TOKEN = os.environ.get("ICS_TOKEN", "")
UPLOAD_DIR = Path(os.environ.get("UPLOAD_DIR", str(BASE_DIR / "uploads")))
MAX_UPLOAD_MB = int(os.environ.get("MAX_UPLOAD_MB", "8"))
