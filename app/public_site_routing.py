"""Kurze öffentliche URLs: Browser ohne /m/<slug>, Routing intern unverändert."""

from __future__ import annotations

from starlette.requests import Request
from starlette.responses import RedirectResponse

from app.config import PUBLIC_SITE_HOSTS, PUBLIC_SITE_MANDANT_SLUG
from app.mandant_host import incoming_hostname
from app.platform_admin_paths import is_platform_superadmin_scope_path


def strip_root_path(scope_path: str, root_path: str) -> str:
    rp = root_path.rstrip("/")
    if rp and scope_path.startswith(rp):
        return scope_path[len(rp) :] or "/"
    return scope_path


def hide_mandant_prefix_for_request(request: Request) -> bool:
    return bool(
        PUBLIC_SITE_HOSTS
        and PUBLIC_SITE_MANDANT_SLUG
        and incoming_hostname(request) in PUBLIC_SITE_HOSTS
    )


def redirect_strip_m_prefix_if_public(request: Request) -> RedirectResponse | None:
    """Alte URLs /m/<slug>/… → im Browser nur /…"""
    if not hide_mandant_prefix_for_request(request):
        return None
    slug = PUBLIC_SITE_MANDANT_SLUG
    path = request.scope.get("path") or "/"
    rp = (request.scope.get("root_path") or "").rstrip("/")
    rel = strip_root_path(path, rp)
    pfx = f"/m/{slug}"
    if not (rel == pfx or rel.startswith(pfx + "/")):
        return None
    tail = rel[len(pfx) :] or "/"
    new_full = (rp + tail) if rp else tail
    qs = request.scope.get("query_string", b"")
    if qs:
        try:
            new_full += "?" + qs.decode("latin-1")
        except UnicodeDecodeError:
            new_full += "?" + qs.decode("utf-8", errors="replace")
    return RedirectResponse(new_full, status_code=307)


def rewrite_scope_to_internal_m_path(request: Request) -> None:
    """Kurze URL /login → intern /m/<slug>/login (nur öffentlicher Host)."""
    if not getattr(request.state, "hide_mandant_path_prefix", False):
        return
    slug = PUBLIC_SITE_MANDANT_SLUG
    scope = request.scope
    path = scope.get("path") or "/"
    rp = (scope.get("root_path") or "").rstrip("/")
    rel = strip_root_path(path, rp)

    if rel.startswith("/m/"):
        return

    if is_platform_superadmin_scope_path(rel):
        return

    exempt_prefixes = (
        "/static",
        "/docs",
        "/redoc",
        "/openapi.json",
        "/manifest.webmanifest",
        "/.well-known",
    )
    if rel in ("/openapi.json", "/manifest.webmanifest"):
        return
    for ex in exempt_prefixes:
        if rel == ex or rel.startswith(ex + "/"):
            return

    new_rel = f"/m/{slug}/" if rel == "/" else f"/m/{slug}{rel}"
    new_path = (rp + new_rel) if rp else new_rel
    scope["path"] = new_path
    try:
        scope["raw_path"] = new_path.encode("latin-1")
    except UnicodeEncodeError:
        scope["raw_path"] = new_path.encode("utf-8")
