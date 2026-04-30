"""Mandant aus Hostname ableiten (Subdomain oder Raw-Host für lokale Tests)."""

from __future__ import annotations

from starlette.requests import Request

from app.config import MANDANT_HOST_BASE_DOMAIN, MANDANT_HOST_IS_RAW_SLUG
from app.ov_services import validate_ov_slug

_RESERVED_SUBDOMAINS = frozenset(
    {"www", "admin", "api", "static", "mail", "ftp", "cdn"},
)


def _first_forwarded_rfc7239(forwarded_header: str) -> str | None:
    """Erster Forwarded-Eintrag, Feld host=… (ohne Port)."""
    first = forwarded_header.split(",")[0].strip()
    for part in first.split(";"):
        part = part.strip()
        if part.lower().startswith("host="):
            val = part.split("=", 1)[1].strip()
            if len(val) >= 2 and val[0] == val[-1] == '"':
                val = val[1:-1]
            return val.split(":")[0].strip().lower() or None
    return None


def incoming_hostname(request: Request) -> str:
    """Öffentlicher Hostname (Proxy: Forwarded, dann X-Forwarded-Host, dann Host)."""
    fwd = (request.headers.get("forwarded") or "").strip()
    if fwd:
        h = _first_forwarded_rfc7239(fwd)
        if h:
            return h
    xfh = (request.headers.get("x-forwarded-host") or "").strip()
    if xfh:
        return xfh.split(",")[0].strip().split(":")[0].lower()
    return (request.headers.get("host") or "").strip().split(":")[0].lower()


def _host_without_port(host_header: str) -> str:
    return host_header.strip().lower().split(":")[0]


def mandant_slug_from_host(host_header: str | None) -> str | None:
    if not host_header or not (
        MANDANT_HOST_BASE_DOMAIN or MANDANT_HOST_IS_RAW_SLUG
    ):
        return None
    host = _host_without_port(host_header)
    if not host:
        return None

    base = MANDANT_HOST_BASE_DOMAIN
    if base:
        base = base.lstrip(".").lower()
        suffix = "." + base
        if host == base:
            return None
        if not host.endswith(suffix):
            return None
        sub = host[: -len(suffix)].rstrip(".")
        if not sub or "." in sub:
            return None
        if sub in _RESERVED_SUBDOMAINS:
            return None
        cand = sub.strip().lower()
        if validate_ov_slug(cand):
            return None
        return cand

    if MANDANT_HOST_IS_RAW_SLUG:
        if "." in host:
            return None
        cand = host.strip().lower()
        if not cand or validate_ov_slug(cand):
            return None
        return cand

    return None


def _decode_header_value(v: bytes) -> str:
    try:
        return v.decode("latin-1")
    except UnicodeDecodeError:
        return v.decode("utf-8", errors="replace")


def effective_forwarded_host(scope: dict) -> str | None:
    """Öffentlicher Host: Forwarded (RFC 7239), dann X-Forwarded-Host, dann Host."""
    fwd_val: str | None = None
    xfh: str | None = None
    host: str | None = None
    for k, v in scope.get("headers") or []:
        lk = k.lower()
        if lk == b"forwarded":
            fwd_val = _decode_header_value(v).strip()
        elif lk == b"x-forwarded-host":
            xfh = _decode_header_value(v).strip()
        elif lk == b"host":
            host = _decode_header_value(v).strip()
    if fwd_val:
        h = _first_forwarded_rfc7239(fwd_val)
        if h:
            return h
    if xfh:
        return xfh.split(",")[0].strip()
    return host


def _rewrite_rel_path(rel: str, slug: str) -> str:
    suffix = "/" if rel == "/" else rel
    return f"/m/{slug}{suffix}"


def should_skip_host_rewrite(rel: str) -> bool:
    if rel.startswith("/m/"):
        return True
    if rel.startswith("/admin"):
        return True
    if rel.startswith("/static"):
        return True
    if rel.startswith("/docs") or rel.startswith("/redoc"):
        return True
    if rel in ("/openapi.json", "/manifest.webmanifest"):
        return True
    if rel.startswith("/.well-known"):
        return True
    return False


def apply_mandant_host_path_rewrite(scope: dict) -> None:
    if not (MANDANT_HOST_BASE_DOMAIN or MANDANT_HOST_IS_RAW_SLUG):
        return
    slug = mandant_slug_from_host(effective_forwarded_host(scope))
    if not slug:
        return

    path = scope.get("path") or "/"
    rp = (scope.get("root_path") or "").rstrip("/")
    rel = path
    if rp and path.startswith(rp):
        rel = path[len(rp) :] or "/"

    if should_skip_host_rewrite(rel):
        return

    new_rel = _rewrite_rel_path(rel, slug)
    new_path = (rp + new_rel) if rp else new_rel
    scope["path"] = new_path
    try:
        scope["raw_path"] = new_path.encode("latin-1")
    except UnicodeEncodeError:
        scope["raw_path"] = new_path.encode("utf-8")
