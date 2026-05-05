from __future__ import annotations

import asyncio
import json
import logging
import re
import uuid
from contextlib import asynccontextmanager
from urllib.parse import urlencode, urlparse
from datetime import date, datetime, time
from pathlib import Path
from types import SimpleNamespace
from typing import Annotated, Any, List, Optional, Union

from fastapi import APIRouter, Depends, FastAPI, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.exception_handlers import http_exception_handler
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from starlette.templating import Jinja2Templates
from sqlalchemy import and_, func, or_
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, selectinload
from starlette.middleware.sessions import SessionMiddleware
from pydantic import BaseModel, ConfigDict, Field

from app.platform_models import (
    TEILNAHME_STATUS_ABGESAGT,
    TEILNAHME_STATUS_ZUGESAGT,
    MandantAppSetting,
    MandantPlakat,
    Ortsverband,
    OvMembership,
    PlatformUser,
    Termin,
    TerminKommentar,
    TerminTeilnahme,
)
from app.auth import hash_password, verify_password
from app.config import (
    CAL_FRAKTION_SYNC_INTERVAL_SECONDS,
    ICS_TOKEN,
    MAX_UPLOAD_MB,
    PUBLIC_SITE_MANDANT_SLUG,
    SECRET_KEY,
    SESSION_COOKIE,
    is_superadmin_username,
    kreis_ov_slug,
    superadmin_usernames,
    upload_dir_for_slug,
)
from app.deps import AdminUser, AuthenticatedUser, CurrentUser
from app.fraktion_visibility import (
    termin_fraktion_sichtbar_fuer_user,
    user_is_fraktionsmitglied,
)
from app.ics_service import (
    all_termine_for_feed,
    all_termine_multi_mandanten,
    build_ics_calendar,
    termine_for_user_teilnahmen,
    termine_zugesagt_multi_mandanten,
)
from app.mandant_features import (
    FEATURE_FRAKTION,
    FEATURE_PLAKATE,
    FEATURE_SHAREPIC,
    is_mandant_feature_enabled,
)
from app.mandant_host import apply_mandant_host_path_rewrite
from app.platform_bootstrap import bootstrap_platform
from app.platform_database import get_platform_db
from app.cal_fraktion_import import run_all_fraktion_cal_subscriptions
from app.settings_store import (
    ensure_ics_token_for_ui,
    ensure_user_calendar_token,
    verify_ics_token,
)
from app.superadmin_web import router as superadmin_router
from app.tenant_assets import sharepic_mask_url
from app.termin_attachments import (
    attachments_decode,
    attachments_encode,
    MAX_TERMIN_ATTACHMENT_BYTES,
    save_attachment_upload,
)
from app.termin_extern import (
    EXTERNE_TEILNEHMER_KEYS,
    EXTERNE_TEILNEHMER_OPTIONS,
    externe_teilnehmer_decode,
    externe_teilnehmer_encode,
    externe_teilnehmer_labels,
)

TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"
STATIC_DIR = Path(__file__).resolve().parent / "static"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

STATIC_DIR.mkdir(parents=True, exist_ok=True)

_cal_log = logging.getLogger("wahlkampf.fraktion_cal")


@asynccontextmanager
async def lifespan(app: FastAPI):
    bootstrap_platform()
    interval = CAL_FRAKTION_SYNC_INTERVAL_SECONDS
    task: asyncio.Task | None = None
    if interval > 0:

        async def _cal_background():
            await asyncio.sleep(45)
            while True:
                try:
                    await asyncio.to_thread(run_all_fraktion_cal_subscriptions)
                except Exception:
                    _cal_log.exception("Kalender-Abo Fraktionstermine (Hintergrund)")
                await asyncio.sleep(interval)

        task = asyncio.create_task(_cal_background())
    yield
    if task:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


ALLOWED_IMAGE = {"image/jpeg", "image/png", "image/webp"}
EXT_MAP = {".jpg": ".jpg", ".jpeg": ".jpg", ".png": ".png", ".webp": ".webp"}
USERNAME_PATTERN = re.compile(r"^[\w.-]+$", re.UNICODE)


class TerminKommentarPayload(BaseModel):
    body: str = Field("", max_length=4000)


class MenuOvCardOpenBody(BaseModel):
    model_config = ConfigDict(extra="forbid")

    slug: str = Field(..., max_length=80)
    open: bool


tenant_router = APIRouter(prefix="/m/{mandant_slug}")

app = FastAPI(title="Wahlkampf", lifespan=lifespan)
app.add_middleware(SessionMiddleware, secret_key=SECRET_KEY, session_cookie=SESSION_COOKIE)


@app.middleware("http")
async def mandanten_kontext(request: Request, call_next):
    apply_mandant_host_path_rewrite(request.scope)

    from app.public_site_routing import (
        hide_mandant_prefix_for_request,
        redirect_strip_m_prefix_if_public,
        rewrite_scope_to_internal_m_path,
    )

    request.state.hide_mandant_path_prefix = hide_mandant_prefix_for_request(request)
    redir = redirect_strip_m_prefix_if_public(request)
    if redir:
        return redir
    rewrite_scope_to_internal_m_path(request)

    request.state.mandanten_prefix = ""
    request.state.mandant_slug = ""
    request.state.ortsverband_name = ""
    path = request.scope.get("path") or "/"
    rp = (request.scope.get("root_path") or "").rstrip("/")
    if rp and path.startswith(rp):
        path = path[len(rp) :] or "/"
    parts = [p for p in path.strip("/").split("/") if p]
    if len(parts) >= 2 and parts[0] == "m":
        slug = parts[1].lower()
        request.state.mandant_slug = slug
        if getattr(request.state, "hide_mandant_path_prefix", False):
            request.state.mandanten_prefix = ""
        else:
            request.state.mandanten_prefix = f"/m/{slug}"
        from sqlalchemy.orm import sessionmaker

        from app.platform_database import platform_engine
        from app.platform_models import Ortsverband

        SessionP = sessionmaker(autocommit=False, autoflush=False, bind=platform_engine())
        pdb = SessionP()
        try:
            ov = pdb.get(Ortsverband, slug)
            if ov:
                request.state.ortsverband_name = (ov.display_name or "").strip() or slug
        finally:
            pdb.close()
    response = await call_next(request)
    return response


def _browser_login_url(request: Request, slug: str, *, pending: bool = False) -> str:
    s = slug.strip().lower()
    if getattr(request.state, "hide_mandant_path_prefix", False):
        pfx = _app_path_prefix(request)
        login_base = f"{pfx}/login" if pfx else "/login"
        return f"{login_base}?pending=1" if pending else login_base
    params = [("ov", s)]
    if pending:
        params.append(("pending", "1"))
    return _app_home_with_query(request, params)


def _safe_login_next_path(raw: str | None) -> str | None:
    """Nur gleiche Origin, Pfad — gegen Open-Redirect."""
    if not raw:
        return None
    s = raw.strip()
    if not s.startswith("/") or s.startswith("//"):
        return None
    if "\x00" in s or "\r" in s or "\n" in s:
        return None
    return s


@app.exception_handler(HTTPException)
async def http_exc(request: Request, exc: HTTPException):
    accept = request.headers.get("accept") or ""
    wants_html = "text/html" in accept or accept.startswith("*/*")
    if exc.status_code == 401 and wants_html:
        path = request.url.path
        rp = (request.scope.get("root_path") or "").rstrip("/")
        if rp and path.startswith(rp):
            path = path[len(rp) :] or "/"
        if path.startswith("/admin"):
            slug = request.session.get("mandant_slug")
            if slug:
                return RedirectResponse(_browser_login_url(request, slug), status_code=302)
            return RedirectResponse("/", status_code=302)
        if exc.detail == "Konto noch nicht freigegeben.":
            slug = request.session.get("mandant_slug")
            if slug:
                return RedirectResponse(
                    _browser_login_url(request, slug, pending=True),
                    status_code=302,
                )
            return RedirectResponse("/", status_code=302)
        slug = request.session.get("mandant_slug")
        if slug:
            return RedirectResponse(_browser_login_url(request, slug), status_code=302)
        return RedirectResponse("/", status_code=302)
    if exc.status_code == 403 and wants_html:
        msg = exc.detail if isinstance(exc.detail, str) else "Keine Berechtigung."
        return templates.TemplateResponse(
            request,
            "forbidden.html",
            {"message": msg},
            status_code=403,
        )
    return await http_exception_handler(request, exc)


app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


def _mp(request: Request) -> str:
    if getattr(request.state, "hide_mandant_path_prefix", False):
        return ""
    slug = request.path_params.get("mandant_slug")
    if slug:
        return f"/m/{slug.strip().lower()}"
    return request.state.mandanten_prefix or ""


def _upload_root(request: Request) -> Path:
    slug = request.path_params["mandant_slug"].strip().lower()
    return upload_dir_for_slug(slug)


def _app_path_prefix(request: Request) -> str:
    """Pfad-Präfix hinter Reverse-Proxy (uvicorn --root-path) für PWA scope/start_url."""
    return (request.scope.get("root_path") or "").rstrip("/")


def _app_home_with_query(request: Request, params: list[tuple[str, str]]) -> str:
    """Start-URL der App inkl. root_path; params werden zu ?a=b …"""
    pfx = _app_path_prefix(request)
    base = f"{pfx}/" if pfx else "/"
    qs = urlencode(params)
    return f"{base}?{qs}" if qs else base


@app.get("/manifest.webmanifest", include_in_schema=False)
def web_app_manifest(request: Request):
    prefix = _app_path_prefix(request)
    start_url = f"{prefix}/" if prefix else "/"
    scope = f"{prefix}/" if prefix else "/"
    icon_path = f"{prefix}/static/icon.svg" if prefix else "/static/icon.svg"
    body = {
        "name": "SPD vor Ort — Wahlkampf",
        "short_name": "SPD vor Ort",
        "description": "Termine, SPD vor Ort und Organisation im Wahlkampf.",
        "id": start_url,
        "start_url": start_url,
        "scope": scope,
        "display": "standalone",
        "display_override": ["standalone", "browser"],
        "background_color": "#e8e4dc",
        "theme_color": "#e8e4dc",
        "icons": [
            {
                "src": icon_path,
                "sizes": "any",
                "type": "image/svg+xml",
                "purpose": "any",
            },
        ],
        "prefer_related_applications": False,
    }
    return JSONResponse(
        content=body,
        media_type="application/manifest+json",
        headers={"Cache-Control": "public, max-age=3600"},
    )


def _can_manage_termin(user: AuthenticatedUser, termin: Termin) -> bool:
    if termin.mandant_slug != user.mandant_slug:
        return False
    return bool(user.is_admin or termin.created_by_id == user.id)


def termin_is_promoted(t: Termin) -> bool:
    return bool(getattr(t, "promoted_all_ovs", False))


def ist_kreis_admin(pdb: Session, user: AuthenticatedUser) -> bool:
    ks = kreis_ov_slug()
    if not ks:
        return False
    mem = (
        pdb.query(OvMembership)
        .filter(
            OvMembership.user_id == user.id,
            OvMembership.ov_slug == ks,
            OvMembership.is_approved.is_(True),
            OvMembership.is_admin.is_(True),
        )
        .first()
    )
    return mem is not None


def _termin_visible_base(
    pdb: Session,
    t: Termin,
    viewing_ms: str,
    user: AuthenticatedUser,
) -> bool:
    """Mandanten-/Kreis-Sichtbarkeit ohne Fraktions-Regeln."""
    viewing_ms = viewing_ms.strip().lower()
    ms_t = t.mandant_slug.strip().lower()
    ks = kreis_ov_slug()

    if ms_t == viewing_ms:
        if ks and ms_t == ks:
            if termin_is_promoted(t):
                if is_superadmin_username(user.username):
                    return True
                mem_any = (
                    pdb.query(OvMembership)
                    .filter(
                        OvMembership.user_id == user.id,
                        OvMembership.is_approved.is_(True),
                    )
                    .first()
                )
                return mem_any is not None
            mem_here = (
                pdb.query(OvMembership)
                .filter(
                    OvMembership.user_id == user.id,
                    OvMembership.ov_slug == viewing_ms,
                    OvMembership.is_approved.is_(True),
                )
                .first()
            )
            return mem_here is not None
        return True

    if not ks or ms_t != ks or not termin_is_promoted(t):
        return False
    if is_superadmin_username(user.username):
        return True
    mem = (
        pdb.query(OvMembership)
        .filter(
            OvMembership.user_id == user.id,
            OvMembership.ov_slug == viewing_ms,
            OvMembership.is_approved.is_(True),
        )
        .first()
    )
    return mem is not None


def termin_sichtbar_instance(
    pdb: Session,
    t: Termin,
    viewing_ms: str,
    user: AuthenticatedUser,
) -> bool:
    if not _termin_visible_base(pdb, t, viewing_ms, user):
        return False
    if getattr(t, "is_fraktion_termin", False):
        if not is_mandant_feature_enabled(pdb, t.mandant_slug, FEATURE_FRAKTION):
            return False
    return termin_fraktion_sichtbar_fuer_user(
        pdb,
        t,
        user_id=user.id,
        username=user.username,
    )


def termin_sichtbar_in_mandant(
    pdb: Session,
    termin_id: int,
    viewing_ms: str,
    user: AuthenticatedUser,
) -> Termin | None:
    """Termin im Kontext viewing_ms (URL-Mandant), inkl. Kreis-promoted und Fraktion."""
    viewing_ms = viewing_ms.strip().lower()
    t = (
        pdb.query(Termin)
        .options(selectinload(Termin.teilnahmen))
        .filter(Termin.id == termin_id)
        .first()
    )
    if not t:
        return None
    if not termin_sichtbar_instance(pdb, t, viewing_ms, user):
        return None
    return t


def _termin_path_segment_for_instance(t: Termin) -> str:
    return "fraktion/termine" if getattr(t, "is_fraktion_termin", False) else "termine"


def _termin_path_segment_from_request(request: Request) -> str:
    path = request.scope.get("path") or ""
    if "/fraktion/termine" in path:
        return "fraktion/termine"
    return "termine"


def _menu_show_alle_termine(pdb: Session, user: AuthenticatedUser) -> bool:
    slugs = _approved_ov_slugs_for_user_feeds(pdb, user)
    if len(slugs) > 1:
        return True
    if len(slugs) == 1:
        sole = slugs[0]
        return user_is_fraktionsmitglied(pdb, user.id, sole) and is_mandant_feature_enabled(
            pdb,
            sole,
            FEATURE_FRAKTION,
        )
    return False


def _termin_row_for_viewing_ov(
    pdb: Session,
    t: Termin,
    user: AuthenticatedUser,
    *,
    viewing_ms: str,
    kommentar_count: int,
    ov_labels: dict[str, str],
    always_show_ov_display_name: bool = False,
) -> dict:
    row = _termin_row_from_instance(pdb, t, user, kommentar_count=kommentar_count)
    ms_t = t.mandant_slug.strip().lower()
    vw = viewing_ms.strip().lower()
    row["owner_mandanten_prefix"] = f"/m/{ms_t}"
    row["viewing_mandanten_prefix"] = f"/m/{vw}"
    row["mandanten_prefix"] = f"/m/{vw}"
    if always_show_ov_display_name:
        row["ov_display_name"] = ov_labels.get(ms_t, ms_t)
    else:
        row["ov_display_name"] = (
            ov_labels.get(ms_t, ms_t) if ms_t != vw else ""
        )
    row["kann_verwalten"] = _can_manage_termin_cross_ov(pdb, user, t)
    row["termin_web_prefix"] = _termin_path_segment_for_instance(t)
    return row


def _unlink_upload(rel: Optional[str], upload_root: Path) -> None:
    if not rel:
        return
    p = upload_root / rel
    try:
        p.unlink(missing_ok=True)
    except OSError:
        pass


def _as_upload_file_list(v: UploadFile | List[UploadFile] | None) -> List[UploadFile]:
    if v is None:
        return []
    if isinstance(v, list):
        return v
    return [v]


def _form_str_list(v: List[str] | str | None) -> List[str]:
    if v is None:
        return []
    if isinstance(v, str):
        return [v] if v else []
    return [str(x) for x in v]


async def _termin_append_attachments(
    uploads: List[UploadFile],
    *,
    termin_id: int,
    upload_root: Path,
    existing: List[dict[str, str]],
) -> tuple[List[dict[str, str]], Optional[str]]:
    items = list(existing)
    added: List[dict[str, str]] = []
    try:
        for uf in uploads:
            if not uf.filename:
                continue
            it = await save_attachment_upload(
                uf,
                termin_id=termin_id,
                upload_root=upload_root,
            )
            added.append(it)
            items.append(it)
    except ValueError as e:
        for it in added:
            _unlink_upload(it["path"], upload_root)
        return existing, str(e)
    return items, None


def _safe_ext(filename: Optional[str], content_type: Optional[str]) -> str:
    if filename:
        suf = Path(filename).suffix.lower()
        if suf in EXT_MAP:
            return EXT_MAP[suf]
    if content_type == "image/jpeg":
        return ".jpg"
    if content_type == "image/png":
        return ".png"
    if content_type == "image/webp":
        return ".webp"
    return ""


def _pending_approval_count(pdb: Session, mandant_slug: str, user: AuthenticatedUser) -> int:
    if not user.is_admin:
        return 0
    ms = mandant_slug.strip().lower()
    return (
        pdb.query(OvMembership)
        .filter(
            OvMembership.ov_slug == ms,
            OvMembership.is_approved.is_(False),
        )
        .count()
    )


def _decode_menu_ov_card_open(raw: str | None) -> dict[str, bool]:
    try:
        data = json.loads((raw or "").strip() or "{}")
    except json.JSONDecodeError:
        return {}
    if not isinstance(data, dict):
        return {}
    out: dict[str, bool] = {}
    for k, v in data.items():
        sk = str(k).strip().lower()
        if not sk:
            continue
        if isinstance(v, bool):
            out[sk] = v
    return out


def _menu_ov_open_map_for_user(pdb: Session, user: AuthenticatedUser) -> dict[str, bool]:
    pu = pdb.get(PlatformUser, user.id)
    if not pu:
        return {}
    return _decode_menu_ov_card_open(getattr(pu, "menu_ov_card_open_json", None))


def _my_ovs_menu_items(
    pdb: Session,
    _mandant_slug: str,
    user_id: int,
    username: str,
) -> list[dict[str, Any]]:
    """OVs für Menü / Wechsel: nur freigegebene Mitgliedschaften — auch für Plattform-Superadmins."""
    sup = is_superadmin_username(username)
    rows = (
        pdb.query(OvMembership, Ortsverband)
        .join(Ortsverband, OvMembership.ov_slug == Ortsverband.slug)
        .filter(
            OvMembership.user_id == user_id,
            OvMembership.is_approved.is_(True),
        )
        .order_by(func.lower(Ortsverband.display_name), OvMembership.ov_slug.asc())
        .all()
    )
    out_members: list[dict[str, Any]] = []
    for m, ov in rows:
        slug = ov.slug.strip().lower()
        dn = (ov.display_name or "").strip() or slug.replace("-", " ").replace("_", " ").title()
        is_adm = bool(m.is_admin or sup)
        pend = 0
        if is_adm:
            pend = (
                pdb.query(OvMembership)
                .filter(
                    OvMembership.ov_slug == slug,
                    OvMembership.is_approved.is_(False),
                )
                .count()
            )
        out_members.append(
            {
                "slug": slug,
                "display_name": dn,
                "is_admin": is_adm,
                "admin_pending_count": pend,
                "feature_plakate": is_mandant_feature_enabled(pdb, slug, FEATURE_PLAKATE),
                "feature_sharepic": is_mandant_feature_enabled(pdb, slug, FEATURE_SHAREPIC),
                "feature_fraktion": is_mandant_feature_enabled(pdb, slug, FEATURE_FRAKTION),
            },
        )
    return out_members


def _query_ortsverbaende_sorted(pdb: Session) -> list[Ortsverband]:
    return (
        pdb.query(Ortsverband)
        .order_by(func.lower(Ortsverband.display_name), Ortsverband.slug.asc())
        .all()
    )


def _login_shell_response(
    request: Request,
    pdb: Session,
    *,
    mandant_slug_for_select: str,
    error: str | None,
    info: str | None,
    status_code: int = 200,
):
    """Login-Oberfläche: Startseite mit OV-Dropdown oder login.html auf Kurz-URL-/Ein-Mandanten-Host."""
    if getattr(request.state, "hide_mandant_path_prefix", False):
        return templates.TemplateResponse(
            request,
            "login.html",
            {"error": error, "info": info},
            status_code=status_code,
        )
    ovs = _query_ortsverbaende_sorted(pdb)
    valid = {o.slug.strip().lower() for o in ovs}
    ms = mandant_slug_for_select.strip().lower()
    preselect = ms if ms in valid else ""
    return templates.TemplateResponse(
        request,
        "home.html",
        {
            "ovs": ovs,
            "preselect_ov_slug": preselect,
            "login_error": error,
            "login_info": info,
        },
        status_code=status_code,
    )


def _registrierung_shell_ctx(
    pdb: Session,
    mandant_slug_for_select: str,
    *,
    error: str | None,
    name_value: str,
    username_value: str,
) -> dict:
    ovs = _query_ortsverbaende_sorted(pdb)
    valid = {o.slug.strip().lower() for o in ovs}
    ms = mandant_slug_for_select.strip().lower()
    preselect = ms if ms in valid else ""
    return {
        "error": error,
        "name_value": name_value,
        "username_value": username_value,
        "ovs": ovs,
        "preselect_ov_slug": preselect,
    }


def _redirect_after_registrierung(request: Request, ov_slug: str, *, first_user: bool) -> RedirectResponse:
    """Nach Registrierung zur gemeinsamen Start-/Login-Ansicht (Multi-Mandant: /?ov=…)."""
    ms = ov_slug.strip().lower()
    if getattr(request.state, "hide_mandant_path_prefix", False):
        q = "registered=first" if first_user else "registered=1"
        return RedirectResponse(f"{_mp(request)}/login?{q}", status_code=302)
    reg_val = "first" if first_user else "1"
    return RedirectResponse(
        _app_home_with_query(request, [("ov", ms), ("registered", reg_val)]),
        status_code=302,
    )


def _user_display_names(pdb: Session, user_ids: set[int]) -> dict[int, str]:
    if not user_ids:
        return {}
    rows = pdb.query(PlatformUser).filter(PlatformUser.id.in_(user_ids)).all()
    return {
        u.id: ((u.display_name or u.username).strip() or u.username) for u in rows
    }


def _termin_kommentare_public(
    pdb: Session,
    termin_id: int,
    user: AuthenticatedUser,
    termin: Termin | None = None,
) -> list[dict]:
    rows = (
        pdb.query(TerminKommentar)
        .filter(TerminKommentar.termin_id == termin_id)
        .order_by(TerminKommentar.created_at.asc())
        .all()
    )
    tref = termin if termin is not None else pdb.get(Termin, termin_id)
    ids = {r.user_id for r in rows}
    names = _user_display_names(pdb, ids)
    out: list[dict] = []
    for r in rows:
        dt = r.created_at
        au = names.get(r.user_id, "Unbekannt")
        may_manage = bool(r.user_id == user.id) or (
            tref is not None and _can_manage_termin_cross_ov(pdb, user, tref)
        )
        out.append(
            {
                "id": r.id,
                "author_name": au,
                "body": r.body or "",
                "created_display": dt.strftime("%d.%m.%Y · %H:%M"),
                "can_edit": may_manage,
                "can_delete": may_manage,
            },
        )
    return out


def _plakate_list_payload(
    pdb: Session, mandant_slug: str, request: Request
) -> list[dict]:
    mp = _mp(request)
    ms = mandant_slug.strip().lower()
    rows = (
        pdb.query(MandantPlakat)
        .filter(MandantPlakat.mandant_slug == ms)
        .order_by(MandantPlakat.hung_at.desc())
        .all()
    )
    ids: set[int] = set()
    for r in rows:
        ids.add(r.hung_by_user_id)
        if r.removed_by_user_id:
            ids.add(r.removed_by_user_id)
    names = _user_display_names(pdb, ids)
    out: list[dict] = []
    for r in rows:
        out.append(
            {
                "id": r.id,
                "lat": r.latitude,
                "lng": r.longitude,
                "active": r.removed_at is None,
                "hung_by_id": r.hung_by_user_id,
                "hung_by_name": names.get(r.hung_by_user_id, "Unbekannt"),
                "hung_at": r.hung_at.isoformat(),
                "image_url": f"{mp}/media/{r.image_path}" if r.image_path else None,
                "note": (r.note or "").strip(),
                "removed_by_id": r.removed_by_user_id,
                "removed_by_name": names.get(r.removed_by_user_id)
                if r.removed_by_user_id
                else None,
                "removed_at": r.removed_at.isoformat() if r.removed_at else None,
            },
        )
    return out




@tenant_router.get("/media/{resource_path:path}", include_in_schema=False)
def serve_tenant_media(mandant_slug: str, resource_path: str):
    root = upload_dir_for_slug(mandant_slug).resolve()
    candidate = (root / resource_path).resolve()
    try:
        candidate.relative_to(root)
    except ValueError:
        raise HTTPException(status_code=404, detail="Not found")
    if not candidate.is_file():
        raise HTTPException(status_code=404, detail="Not found")
    return FileResponse(candidate)



@tenant_router.get("/", include_in_schema=False)
def tenant_root(request: Request, mandant_slug: str):
    ms = mandant_slug.strip().lower()
    if request.session.get("user_id") and request.session.get("mandant_slug") == ms:
        return RedirectResponse(f"{_mp(request)}/menu", status_code=302)
    if getattr(request.state, "hide_mandant_path_prefix", False):
        return RedirectResponse(f"{_mp(request)}/login", status_code=302)
    return RedirectResponse(_app_home_with_query(request, [("ov", ms)]), status_code=302)


@tenant_router.get("/login", response_class=HTMLResponse)
def login_form(request: Request, mandant_slug: str):
    ms = mandant_slug.strip().lower()
    if not getattr(request.state, "hide_mandant_path_prefix", False):
        params = [("ov", ms)]
        for k, v in request.query_params.multi_items():
            if k != "ov":
                params.append((k, v))
        return RedirectResponse(_app_home_with_query(request, params), status_code=302)
    info = None
    if request.query_params.get("pending") == "1":
        info = "Dein Konto ist noch nicht freigegeben. Bitte warte auf einen Administrator."
    if request.query_params.get("registered") == "first":
        info = (
            "Als erster Nutzer bist du automatisch Administrator und freigeschaltet — "
            "du kannst dich jetzt anmelden."
        )
    elif request.query_params.get("registered") == "1":
        info = (
            "Registrierung gespeichert. Sobald ein Administrator dich freischaltet, "
            "kannst du dich anmelden."
        )
    return templates.TemplateResponse(
        request,
        "login.html",
        {"error": None, "info": info},
    )


def _login_submit_response(
    pdb: Session,
    request: Request,
    mandant_slug: str,
    username_raw: str,
    password: str,
    login_next: str | None = None,
):
    ms = mandant_slug.strip().lower()
    if pdb.get(Ortsverband, ms) is None:
        return _login_shell_response(
            request,
            pdb,
            mandant_slug_for_select=ms,
            error="Dieser Ortsverband ist nicht bekannt.",
            info=None,
            status_code=404,
        )
    uname = username_raw.strip().lower()
    pu = (
        pdb.query(PlatformUser)
        .filter(func.lower(PlatformUser.username) == uname)
        .first()
    )
    if not pu or not verify_password(password, pu.password_hash):
        return _login_shell_response(
            request,
            pdb,
            mandant_slug_for_select=ms,
            error="Benutzername oder Passwort falsch.",
            info=None,
            status_code=401,
        )
    mem = (
        pdb.query(OvMembership)
        .filter(OvMembership.user_id == pu.id, OvMembership.ov_slug == ms)
        .first()
    )

    if is_superadmin_username(pu.username):
        pass
    elif mem is None:
        pdb.add(
            OvMembership(
                user_id=pu.id,
                ov_slug=ms,
                is_admin=False,
                is_approved=False,
            ),
        )
        try:
            pdb.commit()
        except IntegrityError:
            pdb.rollback()
        else:
            return _login_shell_response(
                request,
                pdb,
                mandant_slug_for_select=ms,
                error=None,
                info=(
                    "Beitritt zu diesem Ortsverband wurde angefragt. Du nutzt bereits denselben "
                    "Benutzernamen wie bei einem anderen OV — das ist vorgesehen. Sobald hier eine "
                    "Administratorin oder ein Administrator dich freischaltet, meldest du dich "
                    "wie gewohnt mit Benutzername und Passwort an."
                ),
            )
        mem = (
            pdb.query(OvMembership)
            .filter(OvMembership.user_id == pu.id, OvMembership.ov_slug == ms)
            .first()
        )
        if mem is None:
            return _login_shell_response(
                request,
                pdb,
                mandant_slug_for_select=ms,
                error="Der Beitritt konnte nicht gespeichert werden. Bitte später erneut versuchen.",
                info=None,
                status_code=500,
            )

    if not is_superadmin_username(pu.username):
        assert mem is not None
        if not mem.is_approved:
            has_active_admin = (
                pdb.query(OvMembership)
                .filter(
                    OvMembership.ov_slug == ms,
                    OvMembership.is_admin.is_(True),
                    OvMembership.is_approved.is_(True),
                )
                .first()
            )
            if not has_active_admin:
                mem.is_approved = True
                mem.is_admin = True
                pdb.merge(MandantAppSetting(mandant_slug=ms, key="founder_done", value="1"))
                pdb.commit()
            else:
                return _login_shell_response(
                    request,
                    pdb,
                    mandant_slug_for_select=ms,
                    error=None,
                    info="Dein Konto ist noch nicht freigegeben. Bitte warte auf einen Administrator.",
                )

    request.session["user_id"] = pu.id
    request.session["mandant_slug"] = ms
    safe_next = _safe_login_next_path(login_next)
    if safe_next:
        return RedirectResponse(safe_next, status_code=302)
    return RedirectResponse(f"{_mp(request)}/menu", status_code=302)


@tenant_router.post("/login", response_class=HTMLResponse)
def login_submit(
    mandant_slug: str,
    request: Request,
    pdb: Annotated[Session, Depends(get_platform_db)],
    username: Annotated[str, Form()],
    password: Annotated[str, Form()],
    login_next: Annotated[str | None, Form()] = None,
):
    return _login_submit_response(
        pdb,
        request,
        mandant_slug,
        username,
        password,
        login_next=login_next,
    )


def _require_mandant_feature(pdb: Session, mandant_slug: str, feature_key: str) -> None:
    if not is_mandant_feature_enabled(pdb, mandant_slug, feature_key):
        raise HTTPException(status_code=404, detail="Not found")


def _request_has_fraktion_termine_path(request: Request) -> bool:
    path = request.scope.get("path") or ""
    rp = (request.scope.get("root_path") or "").rstrip("/")
    if rp and path.startswith(rp):
        path = path[len(rp) :] or "/"
    return "/fraktion/termine" in path


def _require_fraktion_feature_for_request_path(
    request: Request,
    pdb: Session,
    mandant_slug: str,
) -> None:
    if _request_has_fraktion_termine_path(request):
        _require_mandant_feature(pdb, mandant_slug, FEATURE_FRAKTION)


@tenant_router.get("/menu", response_class=HTMLResponse)
def app_menu(
    mandant_slug: str,
    request: Request,
    pdb: Annotated[Session, Depends(get_platform_db)],
    user: CurrentUser,
):
    return templates.TemplateResponse(
        request,
        "menu.html",
        {
            "user": user,
            "show_superadmin_link": is_superadmin_username(user.username),
            "my_ovs": _my_ovs_menu_items(pdb, mandant_slug, user.id, user.username),
            "show_alle_termine": _menu_show_alle_termine(pdb, user),
            "menu_ov_open": _menu_ov_open_map_for_user(pdb, user),
            "menu_ov_card_save_url": f"{_mp(request)}/menu/ov-card-open",
        },
    )


@tenant_router.post("/menu/ov-card-open")
def app_menu_ov_card_open(
    mandant_slug: str,
    body: MenuOvCardOpenBody,
    pdb: Annotated[Session, Depends(get_platform_db)],
    user: CurrentUser,
):
    ms = body.slug.strip().lower()
    mem_ok = (
        pdb.query(OvMembership)
        .filter(
            OvMembership.user_id == user.id,
            OvMembership.ov_slug == ms,
            OvMembership.is_approved.is_(True),
        )
        .first()
    )
    if not mem_ok:
        raise HTTPException(status_code=403, detail="Kein Zugriff auf diesen Verband")
    pu = pdb.get(PlatformUser, user.id)
    if not pu:
        raise HTTPException(status_code=401, detail="Ungültige Sitzung")
    data = _decode_menu_ov_card_open(getattr(pu, "menu_ov_card_open_json", None))
    data[ms] = body.open
    pu.menu_ov_card_open_json = json.dumps(data, sort_keys=True, separators=(",", ":"))
    pdb.add(pu)
    pdb.commit()
    return JSONResponse({"ok": True})


def _profil_template_response(
    request: Request,
    *,
    user: AuthenticatedUser,
    pu: PlatformUser,
    error: str | None,
    saved: bool,
    display_name_prefill: str,
    status_code: int = 200,
):
    return templates.TemplateResponse(
        request,
        "profil.html",
        {
            "user": user,
            "platform_user": pu,
            "error": error,
            "saved": saved,
            "display_name_prefill": display_name_prefill,
        },
        status_code=status_code,
    )


@tenant_router.get("/profil", response_class=HTMLResponse)
def profil_anzeigen(
    mandant_slug: str,
    request: Request,
    pdb: Annotated[Session, Depends(get_platform_db)],
    user: CurrentUser,
    gespeichert: Annotated[int | None, Query()] = None,
):
    pu = pdb.get(PlatformUser, user.id)
    if not pu:
        raise HTTPException(status_code=404, detail="Nutzer nicht gefunden")
    dn = (pu.display_name or "").strip()
    return _profil_template_response(
        request,
        user=user,
        pu=pu,
        error=None,
        saved=bool(gespeichert),
        display_name_prefill=dn or user.display_name or user.username,
    )


@tenant_router.post("/profil", response_class=HTMLResponse)
def profil_speichern(
    mandant_slug: str,
    request: Request,
    pdb: Annotated[Session, Depends(get_platform_db)],
    user: CurrentUser,
    display_name: Annotated[str, Form()],
    password_current: Annotated[str, Form()] = "",
    password_new: Annotated[str, Form()] = "",
    password_new2: Annotated[str, Form()] = "",
):
    pu = pdb.get(PlatformUser, user.id)
    if not pu:
        raise HTTPException(status_code=404, detail="Nutzer nicht gefunden")
    dn = " ".join(display_name.split()).strip()
    err: str | None = None
    if len(dn) < 2:
        err = "Anzeigename mindestens 2 Zeichen."
    elif len(dn) > 120:
        err = "Anzeigename höchstens 120 Zeichen."

    pw_cur = (password_current or "").strip()
    pw1 = (password_new or "").strip()
    pw2 = (password_new2 or "").strip()
    wants_pw_change = bool(pw_cur or pw1 or pw2)

    if err is None and wants_pw_change:
        if not pw1 or not pw2:
            err = "Neues Passwort bitte zweimal eingeben."
        elif len(pw1) < 8:
            err = "Neues Passwort mindestens 8 Zeichen."
        elif pw1 != pw2:
            err = "Die neuen Passwörter stimmen nicht überein."
        elif not pw_cur:
            err = "Bitte das aktuelle Passwort eingeben."
        elif not verify_password(pw_cur, pu.password_hash):
            err = "Aktuelles Passwort ist falsch."

    if err:
        return _profil_template_response(
            request,
            user=user,
            pu=pu,
            error=err,
            saved=False,
            display_name_prefill=dn,
            status_code=400,
        )

    pu.display_name = dn
    if pw1:
        pu.password_hash = hash_password(pw1)
    pdb.add(pu)
    pdb.commit()
    return RedirectResponse(f"{_mp(request)}/profil?gespeichert=1", status_code=302)


@tenant_router.get("/sharepic", response_class=HTMLResponse)
def sharepic_creator(
    mandant_slug: str,
    request: Request,
    pdb: Annotated[Session, Depends(get_platform_db)],
    user: CurrentUser,
):
    _require_mandant_feature(pdb, mandant_slug, FEATURE_SHAREPIC)
    ov_display = (getattr(request.state, "ortsverband_name", None) or "").strip()
    if not ov_display:
        ov_display = mandant_slug.strip().lower()
    return templates.TemplateResponse(
        request,
        "sharepic.html",
        {
            "user": user,
            "path_prefix": _app_path_prefix(request),
            "mask_src_suffix": sharepic_mask_url(),
            "sharepic_ov_display_name": ov_display,
            "sharepic_slogan_default": f"Für {ov_display}.\nFür Dich.",
        },
    )


@tenant_router.get("/plakate", response_class=HTMLResponse)
def plakate_view(
    mandant_slug: str,
    request: Request,
    pdb: Annotated[Session, Depends(get_platform_db)],
    user: CurrentUser,
):
    _require_mandant_feature(pdb, mandant_slug, FEATURE_PLAKATE)
    return templates.TemplateResponse(
        request,
        "plakate.html",
        {
            "user": user,
            "plakate_initial": _plakate_list_payload(pdb, mandant_slug, request),
            "max_mb": MAX_UPLOAD_MB,
        },
    )


@tenant_router.get("/plakate/api/list")
def plakate_api_list(
    mandant_slug: str,
    request: Request,
    pdb: Annotated[Session, Depends(get_platform_db)],
    _: CurrentUser,
):
    _require_mandant_feature(pdb, mandant_slug, FEATURE_PLAKATE)
    return JSONResponse(_plakate_list_payload(pdb, mandant_slug, request))


@tenant_router.post("/plakate/api/hinzufuegen")
async def plakate_hinzufuegen(
    mandant_slug: str,
    request: Request,
    pdb: Annotated[Session, Depends(get_platform_db)],
    user: CurrentUser,
    lat: Annotated[str, Form()],
    lng: Annotated[str, Form()],
    note: Annotated[str, Form()] = "",
    foto: Annotated[Optional[UploadFile], File()] = None,
):
    _require_mandant_feature(pdb, mandant_slug, FEATURE_PLAKATE)
    try:
        lat_f = float(lat.replace(",", "."))
        lng_f = float(lng.replace(",", "."))
    except ValueError:
        raise HTTPException(status_code=400, detail="Koordinaten ungültig.")
    if not (-90 <= lat_f <= 90 and -180 <= lng_f <= 180):
        raise HTTPException(status_code=400, detail="Koordinaten außerhalb des gültigen Bereichs.")
    ms = mandant_slug.strip().lower()
    p = MandantPlakat(
        mandant_slug=ms,
        latitude=lat_f,
        longitude=lng_f,
        hung_by_user_id=user.id,
        note=note.strip(),
    )
    pdb.add(p)
    pdb.flush()
    if foto and foto.filename:
        ext = _safe_ext(foto.filename, foto.content_type)
        if ext and foto.content_type in ALLOWED_IMAGE:
            max_b = MAX_UPLOAD_MB * 1024 * 1024
            dest_name = f"{p.id}_{uuid.uuid4().hex}{ext}"
            rel = f"plakate/{dest_name}"
            dest = _upload_root(request) / rel
            dest.parent.mkdir(parents=True, exist_ok=True)
            size = 0
            with dest.open("wb") as f:
                while chunk := await foto.read(1024 * 1024):
                    size += len(chunk)
                    if size > max_b:
                        dest.unlink(missing_ok=True)
                        pdb.rollback()
                        raise HTTPException(
                            status_code=400,
                            detail=f"Bild zu groß (max. {MAX_UPLOAD_MB} MB).",
                        )
                    f.write(chunk)
            p.image_path = rel
            pdb.add(p)
        elif foto.filename:
            pdb.rollback()
            raise HTTPException(
                status_code=400,
                detail="Nur JPEG-, PNG- oder WebP-Bilder erlaubt.",
            )
    pdb.commit()
    return JSONResponse({"ok": True, "plakate": _plakate_list_payload(pdb, mandant_slug, request)})


@tenant_router.post("/plakate/api/abhaengen/{plakat_id}")
def plakate_abhaengen(
    mandant_slug: str,
    plakat_id: int,
    pdb: Annotated[Session, Depends(get_platform_db)],
    user: CurrentUser,
):
    _require_mandant_feature(pdb, mandant_slug, FEATURE_PLAKATE)
    ms = mandant_slug.strip().lower()
    p = (
        pdb.query(MandantPlakat)
        .filter(
            MandantPlakat.id == plakat_id,
            MandantPlakat.mandant_slug == ms,
        )
        .first()
    )
    if not p or p.removed_at is not None:
        raise HTTPException(
            status_code=404,
            detail="Plakat nicht gefunden oder bereits abgehängt.",
        )
    p.removed_by_user_id = user.id
    p.removed_at = datetime.utcnow()
    pdb.add(p)
    pdb.commit()
    return JSONResponse({"ok": True, "plakate": _plakate_list_payload(pdb, mandant_slug, request)})


@tenant_router.get("/registrierung", response_class=HTMLResponse)
def registrierung_form(
    mandant_slug: str,
    request: Request,
    pdb: Annotated[Session, Depends(get_platform_db)],
):
    ctx = _registrierung_shell_ctx(
        pdb,
        mandant_slug,
        error=None,
        name_value="",
        username_value="",
    )
    return templates.TemplateResponse(request, "registrierung.html", ctx)


@tenant_router.post("/registrierung", response_class=HTMLResponse)
def registrierung_submit(
    mandant_slug: str,
    request: Request,
    pdb: Annotated[Session, Depends(get_platform_db)],
    name: Annotated[str, Form()],
    benutzername: Annotated[str, Form()],
    password: Annotated[str, Form()],
    password2: Annotated[str, Form()],
):
    ms = mandant_slug.strip().lower()
    if pdb.get(Ortsverband, ms) is None:
        ctx = _registrierung_shell_ctx(
            pdb,
            mandant_slug,
            error="Dieser Ortsverband ist nicht bekannt — bitte einen gültigen Verband wählen.",
            name_value=" ".join(name.split()).strip(),
            username_value=benutzername.strip(),
        )
        return templates.TemplateResponse(
            request,
            "registrierung.html",
            ctx,
            status_code=404,
        )
    display_name = " ".join(name.split()).strip()
    username_raw = benutzername.strip()
    username_norm = username_raw.lower()
    err = None
    if len(display_name) < 2:
        err = "Bitte einen Namen mit mindestens 2 Zeichen angeben."
    elif len(display_name) > 120:
        err = "Name ist zu lang (max. 120 Zeichen)."
    elif len(username_norm) < 2:
        err = "Benutzername mindestens 2 Zeichen."
    elif len(username_norm) > 80:
        err = "Benutzername ist zu lang (max. 80 Zeichen)."
    elif not USERNAME_PATTERN.match(username_norm):
        err = (
            "Benutzername: nur Buchstaben, Ziffern, Punkt, Unterstrich und Bindestrich "
            "(keine Leerzeichen)."
        )
    elif len(password) < 8:
        err = "Passwort mindestens 8 Zeichen."
    elif password != password2:
        err = "Passwörter stimmen nicht überein."
    ctx = _registrierung_shell_ctx(
        pdb,
        mandant_slug,
        error=err,
        name_value=display_name,
        username_value=username_raw,
    )
    if err:
        return templates.TemplateResponse(
            request,
            "registrierung.html",
            ctx,
            status_code=400,
        )
    if (
        pdb.query(PlatformUser)
        .filter(func.lower(PlatformUser.username) == username_norm)
        .first()
    ):
        ctx = _registrierung_shell_ctx(
            pdb,
            mandant_slug,
            error=(
                "Dieser Benutzername ist bereits auf der Plattform vergeben — ein zweites Konto "
                "gibt es nicht. Hast du dich schon woanders registriert und willst diesem "
                "Ortsverband beitreten? Dann hier nicht erneut registrieren, sondern mit "
                "Benutzername und Passwort anmelden; danach wird ein Beitritt beantragt oder "
                "du wirst freigeschaltet."
            ),
            name_value=display_name,
            username_value=username_raw,
        )
        return templates.TemplateResponse(
            request,
            "registrierung.html",
            ctx,
            status_code=400,
        )
    founder_done = pdb.get(MandantAppSetting, (ms, "founder_done"))
    is_first_user = founder_done is None
    pu = PlatformUser(
        username=username_norm,
        password_hash=hash_password(password),
        display_name=display_name,
    )
    pdb.add(pu)
    pdb.flush()
    pdb.add(
        OvMembership(
            user_id=pu.id,
            ov_slug=ms,
            is_admin=is_first_user,
            is_approved=is_first_user,
        )
    )
    if is_first_user:
        pdb.merge(MandantAppSetting(mandant_slug=ms, key="founder_done", value="1"))
    pdb.commit()
    return _redirect_after_registrierung(request, ms, first_user=is_first_user)


def _admin_count(pdb: Session, mandant_slug: str) -> int:
    ms = mandant_slug.strip().lower()
    return (
        pdb.query(OvMembership)
        .filter(OvMembership.ov_slug == ms, OvMembership.is_admin.is_(True))
        .count()
    )


def _ov_user_rows_for_admin(pdb: Session, mandant_slug: str) -> list:
    ms = mandant_slug.strip().lower()
    memberships = (
        pdb.query(OvMembership)
        .filter(OvMembership.ov_slug == ms)
        .order_by(OvMembership.id.asc())
        .all()
    )
    seen_ids: set[int] = set()
    out: list = []
    for m in memberships:
        pu = pdb.get(PlatformUser, m.user_id)
        if not pu:
            continue
        seen_ids.add(pu.id)
        sup = is_superadmin_username(pu.username)
        out.append(
            SimpleNamespace(
                id=pu.id,
                username=pu.username,
                display_name=pu.display_name,
                created_at=pu.created_at,
                is_admin=bool(m.is_admin or sup),
                is_approved=m.is_approved,
                fraktion_member=bool(getattr(m, "fraktion_member", False)),
                shadow_superadmin=False,
                platform_superadmin=sup,
            ),
        )
    names = superadmin_usernames()
    if names:
        shadow_candidates = (
            pdb.query(PlatformUser)
            .filter(PlatformUser.username.in_(tuple(names)))
            .order_by(func.lower(PlatformUser.display_name), PlatformUser.username.asc())
            .all()
        )
        for pu in shadow_candidates:
            if pu.id in seen_ids:
                continue
            out.append(
                SimpleNamespace(
                    id=pu.id,
                    username=pu.username,
                    display_name=pu.display_name,
                    created_at=pu.created_at,
                    is_admin=True,
                    is_approved=True,
                    shadow_superadmin=True,
                    platform_superadmin=True,
                    fraktion_member=False,
                ),
            )
    regular = [r for r in out if not r.shadow_superadmin]
    shadows = [r for r in out if r.shadow_superadmin]
    regular.sort(key=lambda r: r.created_at)
    shadows.sort(
        key=lambda r: ((r.display_name or r.username).lower(), r.username.lower()),
    )
    return regular + shadows


@tenant_router.get("/admin/benutzer", response_class=HTMLResponse)
def admin_benutzer_list(
    mandant_slug: str,
    request: Request,
    pdb: Annotated[Session, Depends(get_platform_db)],
    user: AdminUser,
):
    all_users = _ov_user_rows_for_admin(pdb, mandant_slug)
    ms = mandant_slug.strip().lower()
    return templates.TemplateResponse(
        request,
        "admin_benutzer.html",
        {
            "user": user,
            "users": all_users,
            "admin_count": _admin_count(pdb, mandant_slug),
            "feature_fraktion": is_mandant_feature_enabled(pdb, ms, FEATURE_FRAKTION),
        },
    )


@tenant_router.post("/admin/benutzer/{uid}/freigeben")
def admin_benutzer_freigeben(
    mandant_slug: str,
    uid: int,
    request: Request,
    pdb: Annotated[Session, Depends(get_platform_db)],
    _: AdminUser,
):
    ms = mandant_slug.strip().lower()
    m = (
        pdb.query(OvMembership)
        .filter(OvMembership.user_id == uid, OvMembership.ov_slug == ms)
        .first()
    )
    if m and not m.is_approved:
        m.is_approved = True
        pdb.commit()
    return RedirectResponse(f"{_mp(request)}/admin/benutzer", status_code=302)


@tenant_router.post("/admin/benutzer/{uid}/fraktion-mitglied")
def admin_benutzer_fraktion_mitglied(
    mandant_slug: str,
    uid: int,
    request: Request,
    pdb: Annotated[Session, Depends(get_platform_db)],
    _: AdminUser,
):
    _require_mandant_feature(pdb, mandant_slug, FEATURE_FRAKTION)
    ms = mandant_slug.strip().lower()
    m = (
        pdb.query(OvMembership)
        .filter(OvMembership.user_id == uid, OvMembership.ov_slug == ms)
        .first()
    )
    if m and m.is_approved:
        m.fraktion_member = True
        pdb.commit()
    return RedirectResponse(f"{_mp(request)}/admin/benutzer", status_code=302)


@tenant_router.post("/admin/benutzer/{uid}/fraktion-kein-mitglied")
def admin_benutzer_fraktion_kein_mitglied(
    mandant_slug: str,
    uid: int,
    request: Request,
    pdb: Annotated[Session, Depends(get_platform_db)],
    _: AdminUser,
):
    _require_mandant_feature(pdb, mandant_slug, FEATURE_FRAKTION)
    ms = mandant_slug.strip().lower()
    m = (
        pdb.query(OvMembership)
        .filter(OvMembership.user_id == uid, OvMembership.ov_slug == ms)
        .first()
    )
    if m and m.is_approved:
        m.fraktion_member = False
        pdb.commit()
    return RedirectResponse(f"{_mp(request)}/admin/benutzer", status_code=302)


@tenant_router.post("/admin/benutzer/{uid}/admin-ernennen")
def admin_benutzer_admin_ernennen(
    mandant_slug: str,
    uid: int,
    request: Request,
    pdb: Annotated[Session, Depends(get_platform_db)],
    _: AdminUser,
):
    ms = mandant_slug.strip().lower()
    m = (
        pdb.query(OvMembership)
        .filter(OvMembership.user_id == uid, OvMembership.ov_slug == ms)
        .first()
    )
    if m:
        m.is_admin = True
        m.is_approved = True
        pdb.commit()
    return RedirectResponse(f"{_mp(request)}/admin/benutzer", status_code=302)


@tenant_router.post("/admin/benutzer/{uid}/admin-entfernen")
def admin_benutzer_admin_entfernen(
    mandant_slug: str,
    uid: int,
    request: Request,
    pdb: Annotated[Session, Depends(get_platform_db)],
    _: AdminUser,
):
    ms = mandant_slug.strip().lower()
    m = (
        pdb.query(OvMembership)
        .filter(OvMembership.user_id == uid, OvMembership.ov_slug == ms)
        .first()
    )
    if not m or not m.is_admin:
        return RedirectResponse(f"{_mp(request)}/admin/benutzer", status_code=302)
    if _admin_count(pdb, mandant_slug) <= 1:
        raise HTTPException(
            status_code=403,
            detail="Es muss mindestens ein Administrator bleiben.",
        )
    m.is_admin = False
    pdb.commit()
    return RedirectResponse(f"{_mp(request)}/admin/benutzer", status_code=302)


@tenant_router.post("/admin/benutzer/{uid}/zugriff-entziehen")
def admin_benutzer_zugriff_entziehen(
    mandant_slug: str,
    uid: int,
    request: Request,
    pdb: Annotated[Session, Depends(get_platform_db)],
    actor: AdminUser,
):
    """Nur Mitgliedschaft in diesem OV entfernen — globales PlatformUser-Konto bleibt bestehen."""
    ms = mandant_slug.strip().lower()
    if uid == actor.id:
        raise HTTPException(
            status_code=403,
            detail="Du kannst dir nicht selbst den Zugriff auf dieses OV entziehen.",
        )
    mem = (
        pdb.query(OvMembership)
        .filter(OvMembership.user_id == uid, OvMembership.ov_slug == ms)
        .first()
    )
    if not mem:
        return RedirectResponse(f"{_mp(request)}/admin/benutzer", status_code=302)
    if mem.is_admin and _admin_count(pdb, mandant_slug) <= 1:
        raise HTTPException(
            status_code=403,
            detail="Der letzte Administrator kann nicht entfernt werden.",
        )

    pdb.query(Termin).filter(
        Termin.mandant_slug == ms,
        Termin.created_by_id == uid,
    ).update(
        {Termin.created_by_id: actor.id},
        synchronize_session=False,
    )
    pdb.delete(mem)
    pdb.commit()
    return RedirectResponse(f"{_mp(request)}/admin/benutzer", status_code=302)


@tenant_router.post("/admin/benutzer/{uid}/loeschen")
def admin_benutzer_loeschen_compat(
    mandant_slug: str,
    uid: int,
    request: Request,
    pdb: Annotated[Session, Depends(get_platform_db)],
    actor: AdminUser,
):
    """Frühere Route: entfernt nur die OV-Mitgliedschaft (kein PlatformUser-Löschen)."""
    return admin_benutzer_zugriff_entziehen(
        mandant_slug, uid, request, pdb, actor
    )


@tenant_router.get("/logout")
def logout(request: Request, mandant_slug: str):
    slug = mandant_slug.strip().lower()
    request.session.pop("user_id", None)
    request.session.pop("mandant_slug", None)
    if getattr(request.state, "hide_mandant_path_prefix", False):
        return RedirectResponse(f"{_mp(request)}/login", status_code=302)
    return RedirectResponse(_app_home_with_query(request, [("ov", slug)]), status_code=302)


def _termin_kommentar_counts_by_termin(pdb: Session, termin_ids: list[int]) -> dict[int, int]:
    if not termin_ids:
        return {}
    q = (
        pdb.query(
            TerminKommentar.termin_id,
            func.count(TerminKommentar.id),
        )
        .filter(TerminKommentar.termin_id.in_(termin_ids))
        .group_by(TerminKommentar.termin_id)
        .all()
    )
    return {int(tid): int(c) for tid, c in q}


def _teilnahme_status_val(tn: TerminTeilnahme) -> str:
    s = getattr(tn, "teilnahme_status", None) or TEILNAHME_STATUS_ZUGESAGT
    return s if s in (TEILNAHME_STATUS_ZUGESAGT, TEILNAHME_STATUS_ABGESAGT) else TEILNAHME_STATUS_ZUGESAGT


def _termin_row_from_instance(
    pdb: Session,
    t: Termin,
    user: AuthenticatedUser,
    *,
    kommentar_count: int = 0,
) -> dict:
    zugesagt = [
        tn for tn in t.teilnahmen if _teilnahme_status_val(tn) == TEILNAHME_STATUS_ZUGESAGT
    ]
    abgesagt = [
        tn for tn in t.teilnahmen if _teilnahme_status_val(tn) == TEILNAHME_STATUS_ABGESAGT
    ]
    uids_z = {tn.user_id for tn in zugesagt}
    uids_a = {tn.user_id for tn in abgesagt}
    names_map = _user_display_names(pdb, uids_z | uids_a)
    names = sorted(
        {(names_map.get(tn.user_id, "Unbekannt")).strip() for tn in zugesagt},
        key=str.lower,
    )
    names_absagen = sorted(
        {(names_map.get(tn.user_id, "Unbekannt")).strip() for tn in abgesagt},
        key=str.lower,
    )
    ich = user.id in uids_z
    ich_abgesagt = user.id in uids_a
    kann = _can_manage_termin(user, t)
    extern_labels = externe_teilnehmer_labels(
        externe_teilnehmer_decode(t.externe_teilnehmer_json),
    )
    teilnehmer_extern = sorted(extern_labels, key=str.lower)
    return {
        "termin": t,
        "teilnehmer": names,
        "teilnehmer_abgesagt": names_absagen,
        "teilnehmer_extern": teilnehmer_extern,
        "ich_teilnehme": ich,
        "ich_abgesagt": ich_abgesagt,
        "kann_verwalten": kann,
        "kommentar_count": kommentar_count,
    }


def _teilnahme_wants_live_json(request: Request) -> bool:
    return "application/json" in (request.headers.get("accept") or "").lower()


def _termin_teilnahme_live_payload(
    request: Request,
    pdb: Session,
    *,
    mandant_slug: str,
    termin_id: int,
    user: AuthenticatedUser,
    return_to_list: bool,
) -> dict:
    ms = mandant_slug.strip().lower()
    t = termin_sichtbar_in_mandant(pdb, termin_id, ms, user)
    if not t:
        raise HTTPException(status_code=404, detail="Termin nicht gefunden")
    counts = _termin_kommentar_counts_by_termin(pdb, [termin_id])
    row = _termin_row_from_instance(pdb, t, user, kommentar_count=counts.get(termin_id, 0))
    termin_vergangen = t.starts_at < datetime.utcnow()
    mp = f"{_app_path_prefix(request)}{request.state.mandanten_prefix or ''}"
    ctx = {
        "request": request,
        "mp": mp,
        "termin_id": termin_id,
        "row": row,
        "termin_vergangen": termin_vergangen,
        "return_to_list": return_to_list,
        "teilnehmer": row["teilnehmer"],
        "teilnehmer_extern": row["teilnehmer_extern"],
        "teilnehmer_abgesagt": row["teilnehmer_abgesagt"],
        "termin_web_prefix": _termin_path_segment_for_instance(t),
    }
    footer_inner = templates.env.get_template("_termin_live_footer_inner.html").render(**ctx)
    teilnehmer_inner = templates.env.get_template("_termin_live_teilnehmer_inner.html").render(**ctx)
    absagen_section = ""
    if row["teilnehmer_abgesagt"]:
        absagen_section = templates.env.get_template("_termin_live_absagen_section.html").render(**ctx)
    return {
        "ok": True,
        "footer_inner_html": footer_inner,
        "teilnehmer_inner_html": teilnehmer_inner,
        "absagen_section_html": absagen_section,
    }


def _termin_link_url_normalized(raw: str) -> tuple[str | None, str | None]:
    """Leer oder gültige http(s)-URL (max. 2000 Zeichen)."""
    s = (raw or "").strip()
    if not s:
        return None, None
    if len(s) > 2000:
        return None, "Link darf höchstens 2000 Zeichen haben."
    p = urlparse(s)
    if p.scheme not in ("http", "https") or not p.netloc:
        return None, "Link muss mit http:// oder https:// beginnen und eine gültige Adresse haben."
    return s, None


def _filter_extern_gast_keys(extern_gast: Optional[List[str]]) -> list[str]:
    if not extern_gast:
        return []
    return sorted({str(x) for x in extern_gast if str(x) in EXTERNE_TEILNEHMER_KEYS})


def _termin_form_context(
    *,
    user: AuthenticatedUser,
    termin: Optional[Termin],
    error: Optional[str],
    extern_gast: Optional[List[str]] = None,
    pdb: Optional[Session] = None,
    mandant_slug: Optional[str] = None,
    termin_form_segment: str = "termine",
) -> dict:
    if extern_gast is not None:
        auswahl = _filter_extern_gast_keys(extern_gast)
    elif termin is not None:
        auswahl = externe_teilnehmer_decode(termin.externe_teilnehmer_json)
    else:
        auswahl = []
    ks = kreis_ov_slug()
    ms_ctx = (mandant_slug or (termin.mandant_slug if termin else "") or "").strip().lower()
    show_promoted = bool(
        pdb is not None and ks and ms_ctx == ks and ist_kreis_admin(pdb, user)
    )
    termin_anhaenge = (
        attachments_decode(termin.attachments_json) if termin is not None else []
    )
    show_fraktion_vertraulich = termin_form_segment == "fraktion/termine" or (
        termin is not None and getattr(termin, "is_fraktion_termin", False)
    )
    tseg_norm = termin_form_segment.strip().strip("/")
    show_externe_gaeste = tseg_norm != "fraktion/termine"
    return {
        "user": user,
        "termin": termin,
        "error": error,
        "max_mb": MAX_UPLOAD_MB,
        "max_attachment_mb": MAX_TERMIN_ATTACHMENT_BYTES // (1024 * 1024),
        "termin_anhaenge": termin_anhaenge,
        "externe_optionen": EXTERNE_TEILNEHMER_OPTIONS,
        "externe_auswahl": auswahl,
        "show_promoted_all_ovs_checkbox": show_promoted,
        "promoted_all_ovs_checked": bool(termin and termin_is_promoted(termin)),
        "termin_form_segment": termin_form_segment.strip().strip("/"),
        "show_fraktion_vertraulich_checkbox": show_fraktion_vertraulich,
        "fraktion_vertraulich_checked": bool(
            termin and getattr(termin, "fraktion_vertraulich", False)
        ),
        "show_externe_gaeste": show_externe_gaeste,
    }


def _termin_list_rows(pdb: Session, mandant_slug: str, user: AuthenticatedUser) -> list[dict]:
    ms = mandant_slug.strip().lower()
    ks = kreis_ov_slug()
    q = pdb.query(Termin).options(selectinload(Termin.teilnahmen))
    if ks and ms != ks:
        q = q.filter(
            or_(
                func.lower(Termin.mandant_slug) == ms,
                and_(
                    Termin.promoted_all_ovs == True,  # noqa: E712
                    func.lower(Termin.mandant_slug) == ks,
                ),
            ),
        )
    else:
        q = q.filter(func.lower(Termin.mandant_slug) == ms)
    q = q.filter(Termin.is_fraktion_termin.is_(False))
    rows = q.order_by(Termin.starts_at.asc()).all()
    ids = [t.id for t in rows]
    counts = _termin_kommentar_counts_by_termin(pdb, ids)
    label_slugs = sorted({t.mandant_slug.strip().lower() for t in rows})
    labels = _ov_display_labels_for_slugs(pdb, label_slugs)
    return [
        _termin_row_for_viewing_ov(
            pdb,
            t,
            user,
            viewing_ms=ms,
            kommentar_count=counts.get(t.id, 0),
            ov_labels=labels,
        )
        for t in rows
    ]


def _termin_list_rows_fraktion(pdb: Session, mandant_slug: str, user: AuthenticatedUser) -> list[dict]:
    ms = mandant_slug.strip().lower()
    ks = kreis_ov_slug()
    q = pdb.query(Termin).options(selectinload(Termin.teilnahmen)).filter(
        Termin.is_fraktion_termin.is_(True),
    )
    if ks and ms != ks:
        q = q.filter(
            or_(
                func.lower(Termin.mandant_slug) == ms,
                and_(
                    Termin.promoted_all_ovs == True,  # noqa: E712
                    func.lower(Termin.mandant_slug) == ks,
                ),
            ),
        )
    else:
        q = q.filter(func.lower(Termin.mandant_slug) == ms)
    rows_raw = q.order_by(Termin.starts_at.asc()).all()
    rows = [t for t in rows_raw if termin_sichtbar_instance(pdb, t, ms, user)]
    ids = [t.id for t in rows]
    counts = _termin_kommentar_counts_by_termin(pdb, ids)
    label_slugs = sorted({t.mandant_slug.strip().lower() for t in rows})
    labels = _ov_display_labels_for_slugs(pdb, label_slugs)
    return [
        _termin_row_for_viewing_ov(
            pdb,
            t,
            user,
            viewing_ms=ms,
            kommentar_count=counts.get(t.id, 0),
            ov_labels=labels,
        )
        for t in rows
    ]


def _approved_ov_slugs_for_user_feeds(pdb: Session, user: AuthenticatedUser) -> list[str]:
    rows = (
        pdb.query(OvMembership.ov_slug)
        .filter(
            OvMembership.user_id == user.id,
            OvMembership.is_approved.is_(True),
        )
        .all()
    )
    return sorted({str(r[0]).strip().lower() for r in rows})


def _ov_display_labels_for_slugs(pdb: Session, slugs: list[str]) -> dict[str, str]:
    if not slugs:
        return {}
    ovs = pdb.query(Ortsverband).filter(Ortsverband.slug.in_(slugs)).all()
    return {
        o.slug.strip().lower(): ((o.display_name or "").strip() or o.slug)
        for o in ovs
    }


def _can_manage_termin_cross_ov(pdb: Session, user: AuthenticatedUser, termin: Termin) -> bool:
    if is_superadmin_username(user.username):
        return True
    ms_t = termin.mandant_slug.strip().lower()
    mem = (
        pdb.query(OvMembership)
        .filter(
            OvMembership.user_id == user.id,
            OvMembership.ov_slug == ms_t,
            OvMembership.is_approved.is_(True),
        )
        .first()
    )
    if not mem:
        return False
    if termin.created_by_id == user.id:
        return True
    return bool(mem.is_admin)


def _can_anlegen_fraktionstermin(
    pdb: Session,
    user: AuthenticatedUser,
    mandant_slug: str,
) -> bool:
    """Neue Fraktionstermine nur für Fraktionsmitglieder (OV-weit); Superadmin ausnahme."""
    if is_superadmin_username(user.username):
        return True
    return user_is_fraktionsmitglied(pdb, user.id, mandant_slug.strip().lower())


def _termin_row_cross_ov(
    pdb: Session,
    t: Termin,
    user: AuthenticatedUser,
    *,
    kommentar_count: int,
    ov_labels: dict[str, str],
) -> dict:
    ms = t.mandant_slug.strip().lower()
    mp_row = f"/m/{ms}"
    dn = ov_labels.get(ms, ms)
    row = _termin_row_from_instance(pdb, t, user, kommentar_count=kommentar_count)
    row["mandanten_prefix"] = mp_row
    row["ov_display_name"] = dn
    row["kann_verwalten"] = _can_manage_termin_cross_ov(pdb, user, t)
    row["termin_web_prefix"] = _termin_path_segment_for_instance(t)
    return row


def _termin_list_rows_multi(
    pdb: Session,
    mandant_slugs: list[str],
    user: AuthenticatedUser,
    *,
    viewing_ms: str,
) -> list[dict]:
    if not mandant_slugs:
        return []
    sl = sorted({s.strip().lower() for s in mandant_slugs})
    ks = kreis_ov_slug()
    vm = viewing_ms.strip().lower()
    q = pdb.query(Termin).options(selectinload(Termin.teilnahmen))
    if ks:
        q = q.filter(
            or_(
                Termin.mandant_slug.in_(sl),
                and_(
                    Termin.promoted_all_ovs == True,  # noqa: E712
                    func.lower(Termin.mandant_slug) == ks,
                ),
            ),
        )
    else:
        q = q.filter(Termin.mandant_slug.in_(sl))
    rows_ordered = q.order_by(Termin.starts_at.asc()).all()
    seen: set[int] = set()
    rows: list[Termin] = []
    for t in rows_ordered:
        if t.id in seen:
            continue
        owner_ms = t.mandant_slug.strip().lower()
        if not termin_sichtbar_instance(pdb, t, owner_ms, user):
            continue
        seen.add(t.id)
        rows.append(t)
    label_slugs = sorted({t.mandant_slug.strip().lower() for t in rows})
    labels = _ov_display_labels_for_slugs(pdb, label_slugs + sl)
    ids = [t.id for t in rows]
    counts = _termin_kommentar_counts_by_termin(pdb, ids)
    return [
        _termin_row_for_viewing_ov(
            pdb,
            t,
            user,
            viewing_ms=vm,
            kommentar_count=counts.get(t.id, 0),
            ov_labels=labels,
            always_show_ov_display_name=True,
        )
        for t in rows
    ]


def _termin_detail_row(
    pdb: Session, mandant_slug: str, user: AuthenticatedUser, termin_id: int
) -> dict | None:
    ms = mandant_slug.strip().lower()
    t = termin_sichtbar_in_mandant(pdb, termin_id, ms, user)
    if not t:
        return None
    counts = _termin_kommentar_counts_by_termin(pdb, [t.id])
    labels = _ov_display_labels_for_slugs(pdb, [t.mandant_slug.strip().lower()])
    return _termin_row_for_viewing_ov(
        pdb,
        t,
        user,
        viewing_ms=ms,
        kommentar_count=counts.get(t.id, 0),
        ov_labels=labels,
    )


def _split_termine_upcoming_past(rows: list[dict]) -> tuple[list[dict], list[dict]]:
    now = datetime.utcnow()
    upcoming = [r for r in rows if r["termin"].starts_at >= now]
    past = [r for r in rows if r["termin"].starts_at < now]
    past.sort(key=lambda r: r["termin"].starts_at, reverse=True)
    return upcoming, past


@tenant_router.get("/termine", response_class=HTMLResponse)
def termine_list(
    mandant_slug: str,
    request: Request,
    pdb: Annotated[Session, Depends(get_platform_db)],
    user: CurrentUser,
):
    termin_rows = _termin_list_rows(pdb, mandant_slug, user)
    termin_upcoming, termin_past = _split_termine_upcoming_past(termin_rows)
    token = ensure_ics_token_for_ui(pdb, mandant_slug, ICS_TOKEN)
    base = str(request.base_url).rstrip("/")
    my_token = ensure_user_calendar_token(pdb, user.platform_user)
    mp = _mp(request)
    feed_url_my = f"{base}{mp}/calendar/me.ics?t={my_token}"
    feed_url_all = f"{base}{mp}/calendar.ics?t={token}"
    neuer_termin_href = f"{mp}/termine/neu"
    return templates.TemplateResponse(
        request,
        "termine_list.html",
        {
            "user": user,
            "termin_upcoming": termin_upcoming,
            "termin_past": termin_past,
            "feed_url_my": feed_url_my,
            "feed_url_all": feed_url_all,
            "page_title": "Termine",
            "show_neuer_termin_button": True,
            "ics_my_label": "Meine Zusagen",
            "ics_all_label": "Alle Termine",
            "neuer_termin_button_label": "Neuer Termin",
            "neuer_termin_href": neuer_termin_href,
        },
    )


@tenant_router.get("/termine/alle", response_class=HTMLResponse)
def termine_list_alle(
    mandant_slug: str,
    request: Request,
    pdb: Annotated[Session, Depends(get_platform_db)],
    user: CurrentUser,
):
    slugs = _approved_ov_slugs_for_user_feeds(pdb, user)
    if not slugs:
        return RedirectResponse(f"{_mp(request)}/termine", status_code=302)
    if len(slugs) <= 1:
        sole = slugs[0]
        if not (
            user_is_fraktionsmitglied(pdb, user.id, sole)
            and is_mandant_feature_enabled(pdb, sole, FEATURE_FRAKTION)
        ):
            return RedirectResponse(f"{_mp(request)}/termine", status_code=302)
    termin_rows = _termin_list_rows_multi(pdb, slugs, user, viewing_ms=mandant_slug.strip().lower())
    termin_upcoming, termin_past = _split_termine_upcoming_past(termin_rows)
    my_token = ensure_user_calendar_token(pdb, user.platform_user)
    base = str(request.base_url).rstrip("/")
    mp = _mp(request)
    feed_url_my = f"{base}{mp}/calendar/zusagen-alle.ics?t={my_token}"
    feed_url_all = f"{base}{mp}/calendar/termine-alle.ics?t={my_token}"
    return templates.TemplateResponse(
        request,
        "termine_list.html",
        {
            "user": user,
            "termin_upcoming": termin_upcoming,
            "termin_past": termin_past,
            "feed_url_my": feed_url_my,
            "feed_url_all": feed_url_all,
            "page_title": "Alle Termine",
            "show_neuer_termin_button": False,
            "ics_my_label": "Meine Zusagen (alle Verbände)",
            "ics_all_label": "Alle Termine (alle Verbände)",
        },
    )


@tenant_router.get("/termine/neu", response_class=HTMLResponse)
def termin_new_form(
    mandant_slug: str,
    request: Request,
    pdb: Annotated[Session, Depends(get_platform_db)],
    user: CurrentUser,
):
    return templates.TemplateResponse(
        request,
        "termin_form.html",
        _termin_form_context(
            user=user,
            termin=None,
            error=None,
            pdb=pdb,
            mandant_slug=mandant_slug,
            termin_form_segment="termine",
        ),
    )


@tenant_router.post("/termine/neu", response_class=HTMLResponse)
async def termin_create(
    mandant_slug: str,
    request: Request,
    pdb: Annotated[Session, Depends(get_platform_db)],
    user: CurrentUser,
    title: Annotated[str, Form()],
    datum: Annotated[date, Form()],
    start_uhrzeit: Annotated[str, Form()],
    description: Annotated[str, Form()] = "",
    location: Annotated[str, Form()] = "",
    link_url: Annotated[str, Form()] = "",
    end_uhrzeit: Annotated[str, Form()] = "",
    extern_gast: Annotated[Optional[List[str]], Form()] = None,
    promoted_all_ovs: Annotated[str | None, Form()] = None,
    bild: Annotated[Optional[UploadFile], File()] = None,
    anhaenge: Annotated[Optional[List[UploadFile]], File()] = None,
):
    err = _parse_times(start_uhrzeit, end_uhrzeit)
    if err:
        return templates.TemplateResponse(
            request,
            "termin_form.html",
            _termin_form_context(
                user=user,
                termin=None,
                error=err,
                extern_gast=extern_gast,
                pdb=pdb,
                mandant_slug=mandant_slug,
                termin_form_segment="termine",
            ),
            status_code=400,
        )
    link_u, link_err = _termin_link_url_normalized(link_url)
    if link_err:
        return templates.TemplateResponse(
            request,
            "termin_form.html",
            _termin_form_context(
                user=user,
                termin=None,
                error=link_err,
                extern_gast=extern_gast,
                pdb=pdb,
                mandant_slug=mandant_slug,
                termin_form_segment="termine",
            ),
            status_code=400,
        )
    st = _combine(datum, start_uhrzeit)
    en = _combine(datum, end_uhrzeit) if end_uhrzeit.strip() else None
    if en and en <= st:
        en = None

    ms_low = mandant_slug.strip().lower()
    ks = kreis_ov_slug()
    promoted = False
    if ks and ms_low == ks and ist_kreis_admin(pdb, user):
        promoted = (promoted_all_ovs or "").strip() == "1"

    t = Termin(
        mandant_slug=ms_low,
        title=title.strip(),
        description=description.strip(),
        location=location.strip(),
        starts_at=st,
        ends_at=en,
        externe_teilnehmer_json=externe_teilnehmer_encode(
            _filter_extern_gast_keys(extern_gast),
        ),
        created_by_id=user.id,
        promoted_all_ovs=promoted,
        is_fraktion_termin=False,
        fraktion_vertraulich=False,
        link_url=link_u,
    )
    pdb.add(t)
    pdb.flush()

    if bild and bild.filename:
        ext = _safe_ext(bild.filename, bild.content_type)
        if ext and bild.content_type in ALLOWED_IMAGE:
            max_b = MAX_UPLOAD_MB * 1024 * 1024
            dest_name = f"{t.id}_{uuid.uuid4().hex}{ext}"
            dest = _upload_root(request) / dest_name
            size = 0
            with dest.open("wb") as f:
                while chunk := await bild.read(1024 * 1024):
                    size += len(chunk)
                    if size > max_b:
                        dest.unlink(missing_ok=True)
                        return templates.TemplateResponse(
                            request,
                            "termin_form.html",
                            _termin_form_context(
                                user=user,
                                termin=None,
                                error=f"Bild zu groß (max. {MAX_UPLOAD_MB} MB).",
                                extern_gast=extern_gast,
                                pdb=pdb,
                                mandant_slug=mandant_slug,
                                termin_form_segment="termine",
                            ),
                            status_code=400,
                        )
                    f.write(chunk)
            t.image_path = dest_name
            pdb.add(t)

    uploads = _as_upload_file_list(anhaenge)
    if uploads:
        items, aerr = await _termin_append_attachments(
            uploads,
            termin_id=t.id,
            upload_root=_upload_root(request),
            existing=attachments_decode(t.attachments_json),
        )
        if aerr:
            return templates.TemplateResponse(
                request,
                "termin_form.html",
                _termin_form_context(
                    user=user,
                    termin=None,
                    error=aerr,
                    extern_gast=extern_gast,
                    pdb=pdb,
                    mandant_slug=mandant_slug,
                    termin_form_segment="termine",
                ),
                status_code=400,
            )
        t.attachments_json = attachments_encode(items)
        pdb.add(t)

    pdb.commit()
    return RedirectResponse(f"{_mp(request)}/termine/{t.id}", status_code=302)


@tenant_router.get("/fraktion", include_in_schema=False)
def fraktion_hub_redirect(
    mandant_slug: str,
    request: Request,
    pdb: Annotated[Session, Depends(get_platform_db)],
    user: CurrentUser,
):
    _require_mandant_feature(pdb, mandant_slug, FEATURE_FRAKTION)
    return RedirectResponse(f"{_mp(request)}/fraktion/termine", status_code=302)


@tenant_router.get("/fraktion/termine", response_class=HTMLResponse)
def fraktion_termine_list(
    mandant_slug: str,
    request: Request,
    pdb: Annotated[Session, Depends(get_platform_db)],
    user: CurrentUser,
):
    _require_mandant_feature(pdb, mandant_slug, FEATURE_FRAKTION)
    termin_rows = _termin_list_rows_fraktion(pdb, mandant_slug, user)
    termin_upcoming, termin_past = _split_termine_upcoming_past(termin_rows)
    token = ensure_ics_token_for_ui(pdb, mandant_slug, ICS_TOKEN)
    base = str(request.base_url).rstrip("/")
    my_token = ensure_user_calendar_token(pdb, user.platform_user)
    mp = _mp(request)
    feed_url_my = f"{base}{mp}/calendar/me.ics?t={my_token}"
    feed_url_all = f"{base}{mp}/calendar.ics?t={token}"
    neuer_termin_href = f"{mp}/fraktion/termine/neu"
    kann_fraktionstermin_anlegen = _can_anlegen_fraktionstermin(pdb, user, mandant_slug)
    return templates.TemplateResponse(
        request,
        "termine_list.html",
        {
            "user": user,
            "termin_upcoming": termin_upcoming,
            "termin_past": termin_past,
            "feed_url_my": feed_url_my,
            "feed_url_all": feed_url_all,
            "page_title": "Fraktion — Termine",
            "show_neuer_termin_button": kann_fraktionstermin_anlegen,
            "ics_my_label": "Meine Zusagen",
            "ics_all_label": "Alle Termine",
            "neuer_termin_button_label": "Neuer Fraktionstermin",
            "neuer_termin_href": neuer_termin_href,
        },
    )


@tenant_router.get("/fraktion/termine/neu", response_class=HTMLResponse)
def fraktion_termin_new_form(
    mandant_slug: str,
    request: Request,
    pdb: Annotated[Session, Depends(get_platform_db)],
    user: CurrentUser,
):
    _require_mandant_feature(pdb, mandant_slug, FEATURE_FRAKTION)
    if not _can_anlegen_fraktionstermin(pdb, user, mandant_slug):
        raise HTTPException(
            status_code=403,
            detail="Nur Fraktionsmitglieder können neue Fraktionstermine anlegen.",
        )
    return templates.TemplateResponse(
        request,
        "termin_form.html",
        _termin_form_context(
            user=user,
            termin=None,
            error=None,
            pdb=pdb,
            mandant_slug=mandant_slug,
            termin_form_segment="fraktion/termine",
        ),
    )


@tenant_router.post("/fraktion/termine/neu", response_class=HTMLResponse)
async def fraktion_termin_create(
    mandant_slug: str,
    request: Request,
    pdb: Annotated[Session, Depends(get_platform_db)],
    user: CurrentUser,
    title: Annotated[str, Form()],
    datum: Annotated[date, Form()],
    start_uhrzeit: Annotated[str, Form()],
    description: Annotated[str, Form()] = "",
    location: Annotated[str, Form()] = "",
    link_url: Annotated[str, Form()] = "",
    end_uhrzeit: Annotated[str, Form()] = "",
    promoted_all_ovs: Annotated[str | None, Form()] = None,
    fraktion_vertraulich: Annotated[str | None, Form()] = None,
    bild: Annotated[Optional[UploadFile], File()] = None,
    anhaenge: Annotated[Optional[List[UploadFile]], File()] = None,
):
    _require_mandant_feature(pdb, mandant_slug, FEATURE_FRAKTION)
    if not _can_anlegen_fraktionstermin(pdb, user, mandant_slug):
        raise HTTPException(
            status_code=403,
            detail="Nur Fraktionsmitglieder können neue Fraktionstermine anlegen.",
        )
    err = _parse_times(start_uhrzeit, end_uhrzeit)
    if err:
        return templates.TemplateResponse(
            request,
            "termin_form.html",
            _termin_form_context(
                user=user,
                termin=None,
                error=err,
                pdb=pdb,
                mandant_slug=mandant_slug,
                termin_form_segment="fraktion/termine",
            ),
            status_code=400,
        )
    link_u, link_err = _termin_link_url_normalized(link_url)
    if link_err:
        return templates.TemplateResponse(
            request,
            "termin_form.html",
            _termin_form_context(
                user=user,
                termin=None,
                error=link_err,
                pdb=pdb,
                mandant_slug=mandant_slug,
                termin_form_segment="fraktion/termine",
            ),
            status_code=400,
        )
    st = _combine(datum, start_uhrzeit)
    en = _combine(datum, end_uhrzeit) if end_uhrzeit.strip() else None
    if en and en <= st:
        en = None

    ms_low = mandant_slug.strip().lower()
    ks = kreis_ov_slug()
    promoted = False
    if ks and ms_low == ks and ist_kreis_admin(pdb, user):
        promoted = (promoted_all_ovs or "").strip() == "1"

    confidential = (fraktion_vertraulich or "").strip() == "1"

    t = Termin(
        mandant_slug=ms_low,
        title=title.strip(),
        description=description.strip(),
        location=location.strip(),
        starts_at=st,
        ends_at=en,
        externe_teilnehmer_json=externe_teilnehmer_encode([]),
        created_by_id=user.id,
        promoted_all_ovs=promoted,
        is_fraktion_termin=True,
        fraktion_vertraulich=confidential,
        link_url=link_u,
    )
    pdb.add(t)
    pdb.flush()

    if bild and bild.filename:
        ext = _safe_ext(bild.filename, bild.content_type)
        if ext and bild.content_type in ALLOWED_IMAGE:
            max_b = MAX_UPLOAD_MB * 1024 * 1024
            dest_name = f"{t.id}_{uuid.uuid4().hex}{ext}"
            dest = _upload_root(request) / dest_name
            size = 0
            with dest.open("wb") as f:
                while chunk := await bild.read(1024 * 1024):
                    size += len(chunk)
                    if size > max_b:
                        dest.unlink(missing_ok=True)
                        return templates.TemplateResponse(
                            request,
                            "termin_form.html",
                            _termin_form_context(
                                user=user,
                                termin=None,
                                error=f"Bild zu groß (max. {MAX_UPLOAD_MB} MB).",
                                pdb=pdb,
                                mandant_slug=mandant_slug,
                                termin_form_segment="fraktion/termine",
                            ),
                            status_code=400,
                        )
                    f.write(chunk)
            t.image_path = dest_name
            pdb.add(t)

    uploads = _as_upload_file_list(anhaenge)
    if uploads:
        items, aerr = await _termin_append_attachments(
            uploads,
            termin_id=t.id,
            upload_root=_upload_root(request),
            existing=attachments_decode(t.attachments_json),
        )
        if aerr:
            return templates.TemplateResponse(
                request,
                "termin_form.html",
                _termin_form_context(
                    user=user,
                    termin=None,
                    error=aerr,
                    pdb=pdb,
                    mandant_slug=mandant_slug,
                    termin_form_segment="fraktion/termine",
                ),
                status_code=400,
            )
        t.attachments_json = attachments_encode(items)
        pdb.add(t)

    pdb.commit()
    return RedirectResponse(f"{_mp(request)}/fraktion/termine/{t.id}", status_code=302)


@tenant_router.get("/termine/{termin_id}", response_class=HTMLResponse)
@tenant_router.get("/fraktion/termine/{termin_id}", response_class=HTMLResponse)
def termin_detail(
    mandant_slug: str,
    termin_id: int,
    request: Request,
    pdb: Annotated[Session, Depends(get_platform_db)],
    user: CurrentUser,
):
    _require_fraktion_feature_for_request_path(request, pdb, mandant_slug)
    row = _termin_detail_row(pdb, mandant_slug, user, termin_id)
    if not row:
        raise HTTPException(status_code=404, detail="Termin nicht gefunden")
    termin_vergangen = row["termin"].starts_at < datetime.utcnow()
    kommentare = _termin_kommentare_public(pdb, termin_id, user, termin=row["termin"])
    return templates.TemplateResponse(
        request,
        "termin_detail.html",
        {
            "user": user,
            "row": row,
            "termin_vergangen": termin_vergangen,
            "termin_kommentare": kommentare,
            "termin_anhaenge": attachments_decode(row["termin"].attachments_json),
        },
    )


@tenant_router.post("/termine/{termin_id}/kommentare")
@tenant_router.post("/fraktion/termine/{termin_id}/kommentare")
def termin_kommentar_create(
    mandant_slug: str,
    termin_id: int,
    request: Request,
    payload: TerminKommentarPayload,
    pdb: Annotated[Session, Depends(get_platform_db)],
    user: CurrentUser,
):
    _require_fraktion_feature_for_request_path(request, pdb, mandant_slug)
    ms = mandant_slug.strip().lower()
    body_txt = payload.body.strip()
    if not body_txt:
        raise HTTPException(status_code=400, detail="Kommentar darf nicht leer sein.")
    t = termin_sichtbar_in_mandant(pdb, termin_id, ms, user)
    if not t:
        raise HTTPException(status_code=404, detail="Termin nicht gefunden.")
    km = TerminKommentar(
        termin_id=termin_id,
        user_id=user.id,
        body=body_txt[:4000],
    )
    pdb.add(km)
    pdb.commit()
    return JSONResponse(
        {
            "ok": True,
            "kommentare": _termin_kommentare_public(pdb, termin_id, user, termin=t),
        },
    )


@tenant_router.patch("/termine/{termin_id}/kommentare/{kommentar_id}")
@tenant_router.patch("/fraktion/termine/{termin_id}/kommentare/{kommentar_id}")
def termin_kommentar_update(
    mandant_slug: str,
    termin_id: int,
    kommentar_id: int,
    request: Request,
    payload: TerminKommentarPayload,
    pdb: Annotated[Session, Depends(get_platform_db)],
    user: CurrentUser,
):
    _require_fraktion_feature_for_request_path(request, pdb, mandant_slug)
    body_txt = payload.body.strip()
    if not body_txt:
        raise HTTPException(status_code=400, detail="Kommentar darf nicht leer sein.")
    ms = mandant_slug.strip().lower()
    t = termin_sichtbar_in_mandant(pdb, termin_id, ms, user)
    if not t:
        raise HTTPException(status_code=404, detail="Termin nicht gefunden.")
    km = (
        pdb.query(TerminKommentar)
        .filter(
            TerminKommentar.id == kommentar_id,
            TerminKommentar.termin_id == termin_id,
        )
        .first()
    )
    if not km:
        raise HTTPException(status_code=404, detail="Kommentar nicht gefunden.")
    if not (
        km.user_id == user.id or _can_manage_termin_cross_ov(pdb, user, t)
    ):
        raise HTTPException(status_code=403, detail="Keine Berechtigung.")
    km.body = body_txt[:4000]
    pdb.add(km)
    pdb.commit()
    return JSONResponse(
        {
            "ok": True,
            "kommentare": _termin_kommentare_public(pdb, termin_id, user, termin=t),
        },
    )


@tenant_router.delete("/termine/{termin_id}/kommentare/{kommentar_id}")
@tenant_router.delete("/fraktion/termine/{termin_id}/kommentare/{kommentar_id}")
def termin_kommentar_delete(
    mandant_slug: str,
    termin_id: int,
    kommentar_id: int,
    request: Request,
    pdb: Annotated[Session, Depends(get_platform_db)],
    user: CurrentUser,
):
    _require_fraktion_feature_for_request_path(request, pdb, mandant_slug)
    ms = mandant_slug.strip().lower()
    t = termin_sichtbar_in_mandant(pdb, termin_id, ms, user)
    if not t:
        raise HTTPException(status_code=404, detail="Termin nicht gefunden.")
    km = (
        pdb.query(TerminKommentar)
        .filter(
            TerminKommentar.id == kommentar_id,
            TerminKommentar.termin_id == termin_id,
        )
        .first()
    )
    if not km:
        raise HTTPException(status_code=404, detail="Kommentar nicht gefunden.")
    if not (
        km.user_id == user.id or _can_manage_termin_cross_ov(pdb, user, t)
    ):
        raise HTTPException(status_code=403, detail="Keine Berechtigung.")
    pdb.delete(km)
    pdb.commit()
    return JSONResponse(
        {
            "ok": True,
            "kommentare": _termin_kommentare_public(pdb, termin_id, user, termin=t),
        },
    )


@tenant_router.post("/termine/{termin_id}/teilnehmen")
@tenant_router.post("/fraktion/termine/{termin_id}/teilnehmen")
def termin_teilnehmen(
    mandant_slug: str,
    termin_id: int,
    request: Request,
    pdb: Annotated[Session, Depends(get_platform_db)],
    user: CurrentUser,
    return_to: Annotated[str | None, Form()] = None,
):
    _require_fraktion_feature_for_request_path(request, pdb, mandant_slug)
    ms = mandant_slug.strip().lower()
    t = termin_sichtbar_in_mandant(pdb, termin_id, ms, user)
    if not t:
        raise HTTPException(status_code=404, detail="Termin nicht gefunden")
    exists = (
        pdb.query(TerminTeilnahme)
        .filter_by(termin_id=termin_id, user_id=user.id)
        .first()
    )
    if not exists:
        pdb.add(
            TerminTeilnahme(
                termin_id=termin_id,
                user_id=user.id,
                teilnahme_status=TEILNAHME_STATUS_ZUGESAGT,
            ),
        )
    else:
        exists.teilnahme_status = TEILNAHME_STATUS_ZUGESAGT
        pdb.add(exists)
    pdb.commit()
    return_to_list = (return_to or "").strip() == "list"
    seg = _termin_path_segment_for_instance(t)
    if _teilnahme_wants_live_json(request):
        return JSONResponse(
            _termin_teilnahme_live_payload(
                request,
                pdb,
                mandant_slug=mandant_slug,
                termin_id=termin_id,
                user=user,
                return_to_list=return_to_list,
            ),
        )
    if return_to_list:
        return RedirectResponse(f"{_mp(request)}/{seg}", status_code=302)
    return RedirectResponse(f"{_mp(request)}/{seg}/{termin_id}", status_code=302)


@tenant_router.post("/termine/{termin_id}/abmelden")
@tenant_router.post("/termine/{termin_id}/absagen")
@tenant_router.post("/fraktion/termine/{termin_id}/abmelden")
@tenant_router.post("/fraktion/termine/{termin_id}/absagen")
def termin_teilnahme_absagen(
    mandant_slug: str,
    termin_id: int,
    request: Request,
    pdb: Annotated[Session, Depends(get_platform_db)],
    user: CurrentUser,
    return_to: Annotated[str | None, Form()] = None,
):
    """Absage bzw. Zusage zurücknehmen — gleiche Logik für `/absagen` und `/abmelden` (Legacy)."""
    _require_fraktion_feature_for_request_path(request, pdb, mandant_slug)
    ms = mandant_slug.strip().lower()
    t = termin_sichtbar_in_mandant(pdb, termin_id, ms, user)
    if not t:
        raise HTTPException(status_code=404, detail="Termin nicht gefunden")
    row = (
        pdb.query(TerminTeilnahme)
        .filter_by(termin_id=termin_id, user_id=user.id)
        .first()
    )
    if row:
        row.teilnahme_status = TEILNAHME_STATUS_ABGESAGT
        pdb.add(row)
    else:
        pdb.add(
            TerminTeilnahme(
                termin_id=termin_id,
                user_id=user.id,
                teilnahme_status=TEILNAHME_STATUS_ABGESAGT,
            ),
        )
    pdb.commit()
    return_to_list = (return_to or "").strip() == "list"
    seg = _termin_path_segment_for_instance(t)
    if _teilnahme_wants_live_json(request):
        return JSONResponse(
            _termin_teilnahme_live_payload(
                request,
                pdb,
                mandant_slug=mandant_slug,
                termin_id=termin_id,
                user=user,
                return_to_list=return_to_list,
            ),
        )
    if return_to_list:
        return RedirectResponse(f"{_mp(request)}/{seg}", status_code=302)
    return RedirectResponse(f"{_mp(request)}/{seg}/{termin_id}", status_code=302)


@tenant_router.get("/termine/{termin_id}/bearbeiten", response_class=HTMLResponse)
@tenant_router.get("/fraktion/termine/{termin_id}/bearbeiten", response_class=HTMLResponse)
def termin_edit_form(
    mandant_slug: str,
    termin_id: int,
    request: Request,
    pdb: Annotated[Session, Depends(get_platform_db)],
    user: CurrentUser,
):
    _require_fraktion_feature_for_request_path(request, pdb, mandant_slug)
    ms = mandant_slug.strip().lower()
    t = termin_sichtbar_in_mandant(pdb, termin_id, ms, user)
    if not t:
        raise HTTPException(status_code=404, detail="Termin nicht gefunden")
    owner = t.mandant_slug.strip().lower()
    seg = _termin_path_segment_for_instance(t)
    if owner != ms:
        rp = _app_path_prefix(request).rstrip("/")
        dest = f"{rp}/m/{owner}/{seg}/{termin_id}/bearbeiten"
        return RedirectResponse(dest, status_code=302)
    if not _can_manage_termin_cross_ov(pdb, user, t):
        raise HTTPException(
            status_code=403,
            detail="Du darfst diesen Termin nicht bearbeiten.",
        )
    return templates.TemplateResponse(
        request,
        "termin_form.html",
        _termin_form_context(
            user=user,
            termin=t,
            error=None,
            pdb=pdb,
            mandant_slug=mandant_slug,
            termin_form_segment=_termin_path_segment_from_request(request),
        ),
    )


@tenant_router.post("/termine/{termin_id}/bearbeiten", response_class=HTMLResponse)
@tenant_router.post("/fraktion/termine/{termin_id}/bearbeiten", response_class=HTMLResponse)
async def termin_edit_save(
    mandant_slug: str,
    termin_id: int,
    request: Request,
    pdb: Annotated[Session, Depends(get_platform_db)],
    user: CurrentUser,
    title: Annotated[str, Form()],
    datum: Annotated[date, Form()],
    start_uhrzeit: Annotated[str, Form()],
    description: Annotated[str, Form()] = "",
    location: Annotated[str, Form()] = "",
    link_url: Annotated[str, Form()] = "",
    end_uhrzeit: Annotated[str, Form()] = "",
    bild_entfernen: Annotated[str, Form()] = "",
    extern_gast: Annotated[Optional[List[str]], Form()] = None,
    anhang_entfernen: Annotated[
        Union[str, List[str], None],
        Form(),
    ] = None,
    promoted_all_ovs: Annotated[str | None, Form()] = None,
    fraktion_vertraulich: Annotated[str | None, Form()] = None,
    bild: Annotated[Optional[UploadFile], File()] = None,
    anhaenge: Annotated[Optional[List[UploadFile]], File()] = None,
):
    _require_fraktion_feature_for_request_path(request, pdb, mandant_slug)
    ms = mandant_slug.strip().lower()
    t = termin_sichtbar_in_mandant(pdb, termin_id, ms, user)
    if not t:
        raise HTTPException(status_code=404, detail="Termin nicht gefunden")
    if t.mandant_slug.strip().lower() != ms:
        raise HTTPException(status_code=404, detail="Termin nicht gefunden")
    if not _can_manage_termin_cross_ov(pdb, user, t):
        raise HTTPException(
            status_code=403,
            detail="Du darfst diesen Termin nicht bearbeiten.",
        )
    upload_root = upload_dir_for_slug(t.mandant_slug.strip().lower())

    err = _parse_times(start_uhrzeit, end_uhrzeit)
    if err:
        return templates.TemplateResponse(
            request,
            "termin_form.html",
            _termin_form_context(
                user=user,
                termin=t,
                error=err,
                extern_gast=extern_gast,
                pdb=pdb,
                mandant_slug=mandant_slug,
                termin_form_segment=_termin_path_segment_for_instance(t),
            ),
            status_code=400,
        )
    link_u, link_err = _termin_link_url_normalized(link_url)
    if link_err:
        return templates.TemplateResponse(
            request,
            "termin_form.html",
            _termin_form_context(
                user=user,
                termin=t,
                error=link_err,
                extern_gast=extern_gast,
                pdb=pdb,
                mandant_slug=mandant_slug,
                termin_form_segment=_termin_path_segment_for_instance(t),
            ),
            status_code=400,
        )
    st = _combine(datum, start_uhrzeit)
    en = _combine(datum, end_uhrzeit) if end_uhrzeit.strip() else None
    if en and en <= st:
        en = None

    t.title = title.strip()
    t.description = description.strip()
    t.location = location.strip()
    t.link_url = link_u
    t.starts_at = st
    t.ends_at = en
    if getattr(t, "is_fraktion_termin", False):
        t.externe_teilnehmer_json = externe_teilnehmer_encode([])
    else:
        t.externe_teilnehmer_json = externe_teilnehmer_encode(
            _filter_extern_gast_keys(extern_gast),
        )

    ks = kreis_ov_slug()
    can_prom = bool(
        ks
        and ms == ks
        and t.mandant_slug.strip().lower() == ks
        and ist_kreis_admin(pdb, user),
    )
    if can_prom:
        t.promoted_all_ovs = (promoted_all_ovs or "").strip() == "1"

    if getattr(t, "is_fraktion_termin", False):
        t.fraktion_vertraulich = (fraktion_vertraulich or "").strip() == "1"

    if bild_entfernen == "1":
        _unlink_upload(t.image_path, upload_root)
        t.image_path = None

    if bild and bild.filename:
        ext = _safe_ext(bild.filename, bild.content_type)
        if ext and bild.content_type in ALLOWED_IMAGE:
            max_b = MAX_UPLOAD_MB * 1024 * 1024
            dest_name = f"{t.id}_{uuid.uuid4().hex}{ext}"
            dest = upload_root / dest_name
            size = 0
            with dest.open("wb") as f:
                while chunk := await bild.read(1024 * 1024):
                    size += len(chunk)
                    if size > max_b:
                        dest.unlink(missing_ok=True)
                        pdb.rollback()
                        pdb.refresh(t)
                        return templates.TemplateResponse(
                            request,
                            "termin_form.html",
                            _termin_form_context(
                                user=user,
                                termin=t,
                                error=f"Bild zu groß (max. {MAX_UPLOAD_MB} MB).",
                                extern_gast=extern_gast,
                                pdb=pdb,
                                mandant_slug=mandant_slug,
                                termin_form_segment=_termin_path_segment_for_instance(t),
                            ),
                            status_code=400,
                        )
                    f.write(chunk)
            _unlink_upload(t.image_path, upload_root)
            t.image_path = dest_name

    raw_attach = attachments_decode(t.attachments_json)
    remove_set = {
        int(x)
        for x in _form_str_list(anhang_entfernen)
        if str(x).strip().isdigit()
    }
    kept_attach: List[dict[str, str]] = []
    for idx, it in enumerate(raw_attach):
        if idx in remove_set:
            _unlink_upload(it["path"], upload_root)
        else:
            kept_attach.append(it)

    uploads_a = _as_upload_file_list(anhaenge)
    if uploads_a:
        kept_attach2, aerr = await _termin_append_attachments(
            uploads_a,
            termin_id=t.id,
            upload_root=upload_root,
            existing=kept_attach,
        )
        if aerr:
            pdb.rollback()
            pdb.refresh(t)
            return templates.TemplateResponse(
                request,
                "termin_form.html",
                _termin_form_context(
                    user=user,
                    termin=t,
                    error=aerr,
                    extern_gast=extern_gast,
                    pdb=pdb,
                    mandant_slug=mandant_slug,
                    termin_form_segment=_termin_path_segment_for_instance(t),
                ),
                status_code=400,
            )
        kept_attach = kept_attach2

    t.attachments_json = attachments_encode(kept_attach)

    pdb.add(t)
    pdb.commit()
    seg = _termin_path_segment_for_instance(t)
    return RedirectResponse(f"{_mp(request)}/{seg}/{termin_id}", status_code=302)


@tenant_router.get("/termine/{termin_id}/loeschen", response_class=HTMLResponse)
@tenant_router.get("/fraktion/termine/{termin_id}/loeschen", response_class=HTMLResponse)
def termin_delete_confirm(
    mandant_slug: str,
    termin_id: int,
    request: Request,
    pdb: Annotated[Session, Depends(get_platform_db)],
    user: CurrentUser,
):
    _require_fraktion_feature_for_request_path(request, pdb, mandant_slug)
    ms = mandant_slug.strip().lower()
    t = termin_sichtbar_in_mandant(pdb, termin_id, ms, user)
    if not t:
        raise HTTPException(status_code=404, detail="Termin nicht gefunden")
    owner = t.mandant_slug.strip().lower()
    if owner != ms:
        rp = _app_path_prefix(request).rstrip("/")
        seg = _termin_path_segment_for_instance(t)
        return RedirectResponse(f"{rp}/m/{owner}/{seg}/{termin_id}/loeschen", status_code=302)
    if not _can_manage_termin_cross_ov(pdb, user, t):
        raise HTTPException(
            status_code=403,
            detail="Du darfst diesen Termin nicht löschen.",
        )
    return templates.TemplateResponse(
        request,
        "termin_loeschen.html",
        {
            "user": user,
            "termin": t,
            "termin_web_prefix": _termin_path_segment_for_instance(t),
        },
    )


@tenant_router.post("/termine/{termin_id}/loeschen")
@tenant_router.post("/fraktion/termine/{termin_id}/loeschen")
def termin_delete_do(
    mandant_slug: str,
    termin_id: int,
    request: Request,
    pdb: Annotated[Session, Depends(get_platform_db)],
    user: CurrentUser,
):
    _require_fraktion_feature_for_request_path(request, pdb, mandant_slug)
    ms = mandant_slug.strip().lower()
    t = termin_sichtbar_in_mandant(pdb, termin_id, ms, user)
    if not t:
        raise HTTPException(status_code=404, detail="Termin nicht gefunden")
    if t.mandant_slug.strip().lower() != ms:
        raise HTTPException(status_code=404, detail="Termin nicht gefunden")
    if not _can_manage_termin_cross_ov(pdb, user, t):
        raise HTTPException(
            status_code=403,
            detail="Du darfst diesen Termin nicht löschen.",
        )
    upload_root = upload_dir_for_slug(t.mandant_slug.strip().lower())
    _unlink_upload(t.image_path, upload_root)
    for it in attachments_decode(t.attachments_json):
        _unlink_upload(it.get("path"), upload_root)
    pdb.delete(t)
    pdb.commit()
    seg = _termin_path_segment_for_instance(t)
    return RedirectResponse(f"{_mp(request)}/{seg}", status_code=302)


_TIME_RE = re.compile(r"^\s*(\d{1,2}):(\d{2})\s*$")


def _parse_times(start_s: str, end_s: str) -> str | None:
    if not _TIME_RE.match(start_s or ""):
        return "Start-Uhrzeit bitte als HH:MM angeben."
    if end_s.strip() and not _TIME_RE.match(end_s):
        return "End-Uhrzeit bitte als HH:MM angeben oder leer lassen."
    return None


def _combine(d: date, hhmm: str) -> datetime:
    m = _TIME_RE.match(hhmm.strip())
    assert m
    h, mi = int(m.group(1)), int(m.group(2))
    return datetime(d.year, d.month, d.day, h, mi, 0)


@tenant_router.get("/calendar.ics")
def calendar_ics(
    mandant_slug: str,
    request: Request,
    pdb: Annotated[Session, Depends(get_platform_db)],
    t: Optional[str] = None,
):
    if not verify_ics_token(pdb, mandant_slug, ICS_TOKEN, t):
        raise HTTPException(status_code=404, detail="Not found")
    termine = all_termine_for_feed(pdb, mandant_slug)
    ks = kreis_ov_slug()
    labels = None
    if ks and termine:
        slugs = sorted({x.mandant_slug.strip().lower() for x in termine})
        labels = _ov_display_labels_for_slugs(pdb, slugs)
    body = build_ics_calendar(termine, ov_labels_for_mandant_slug=labels)
    return Response(
        content=body,
        media_type="text/calendar; charset=utf-8",
        headers={
            "Content-Disposition": 'attachment; filename="wahlkampf.ics"',
            "Cache-Control": "no-store",
        },
    )


@tenant_router.get("/calendar/me.ics")
def calendar_ics_me(
    mandant_slug: str,
    pdb: Annotated[Session, Depends(get_platform_db)],
    t: Optional[str] = None,
):
    """Persönlicher Feed: nur Termine mit Zusage (Teilnahme) für den zugehörigen Account."""
    if not t:
        raise HTTPException(status_code=404, detail="Not found")
    owner = (
        pdb.query(PlatformUser)
        .filter(PlatformUser.calendar_token == t)
        .first()
    )
    if not owner:
        raise HTTPException(status_code=404, detail="Not found")
    termine = termine_for_user_teilnahmen(pdb, owner.id, mandant_slug)
    ks = kreis_ov_slug()
    labels = None
    if ks and termine:
        slugs = sorted({x.mandant_slug.strip().lower() for x in termine})
        labels = _ov_display_labels_for_slugs(pdb, slugs)
    body = build_ics_calendar(termine, cal_name="Meine Zusagen — Wahlkampf", ov_labels_for_mandant_slug=labels)
    return Response(
        content=body,
        media_type="text/calendar; charset=utf-8",
        headers={
            "Content-Disposition": 'attachment; filename="meine-termine.ics"',
            "Cache-Control": "no-store",
        },
    )


@tenant_router.get("/calendar/zusagen-alle.ics")
def calendar_ics_zusagen_alle(
    mandant_slug: str,
    pdb: Annotated[Session, Depends(get_platform_db)],
    t: Optional[str] = None,
):
    """Persönlicher Feed: Zusagen über alle freigegebenen Verbände des Nutzers."""
    if not t:
        raise HTTPException(status_code=404, detail="Not found")
    owner = (
        pdb.query(PlatformUser)
        .filter(PlatformUser.calendar_token == t)
        .first()
    )
    if not owner:
        raise HTTPException(status_code=404, detail="Not found")
    au = AuthenticatedUser(owner, mandant_slug, None)
    slugs = _approved_ov_slugs_for_user_feeds(pdb, au)
    termine = termine_zugesagt_multi_mandanten(pdb, owner.id, slugs)
    ks = kreis_ov_slug()
    label_slugs = list(slugs)
    if ks and ks not in label_slugs:
        label_slugs.append(ks)
    labels = _ov_display_labels_for_slugs(pdb, label_slugs)
    body = build_ics_calendar(
        termine,
        cal_name="Meine Zusagen — alle Verbände",
        ov_labels_for_mandant_slug=labels,
    )
    return Response(
        content=body,
        media_type="text/calendar; charset=utf-8",
        headers={
            "Content-Disposition": 'attachment; filename="meine-zusagen-alle.ics"',
            "Cache-Control": "no-store",
        },
    )


@tenant_router.get("/calendar/termine-alle.ics")
def calendar_ics_termine_alle(
    mandant_slug: str,
    pdb: Annotated[Session, Depends(get_platform_db)],
    t: Optional[str] = None,
):
    """Persönlicher Feed: alle Termine in allen freigegebenen Verbänden des Nutzers."""
    if not t:
        raise HTTPException(status_code=404, detail="Not found")
    owner = (
        pdb.query(PlatformUser)
        .filter(PlatformUser.calendar_token == t)
        .first()
    )
    if not owner:
        raise HTTPException(status_code=404, detail="Not found")
    au = AuthenticatedUser(owner, mandant_slug, None)
    slugs = _approved_ov_slugs_for_user_feeds(pdb, au)
    termine = all_termine_multi_mandanten(pdb, slugs, calendar_owner_user_id=owner.id)
    ks = kreis_ov_slug()
    label_slugs = list(slugs)
    if ks and ks not in label_slugs:
        label_slugs.append(ks)
    labels = _ov_display_labels_for_slugs(pdb, label_slugs)
    body = build_ics_calendar(
        termine,
        cal_name="Alle Termine — meine Verbände",
        ov_labels_for_mandant_slug=labels,
    )
    return Response(
        content=body,
        media_type="text/calendar; charset=utf-8",
        headers={
            "Content-Disposition": 'attachment; filename="alle-termine-alle.ics"',
            "Cache-Control": "no-store",
        },
    )


app.include_router(tenant_router)
app.include_router(superadmin_router)


@app.get("/m/{mandant_slug}", include_in_schema=False)
def mandant_redirect_add_slash(mandant_slug: str, request: Request):
    """Coolify/nginx liefern oft /m/westerstede ohne Slash — Tenant-Routen hängen an …/."""
    ms = mandant_slug.strip().lower()
    if getattr(request.state, "hide_mandant_path_prefix", False) and (
        ms == PUBLIC_SITE_MANDANT_SLUG
    ):
        dest = "/"
    else:
        dest = f"/m/{ms}/"
    if request.url.query:
        dest = f"{dest}?{request.url.query}"
    return RedirectResponse(dest, status_code=307)


@app.get("/", response_class=HTMLResponse)
def root(request: Request):
    if request.session.get("user_id") and request.session.get("mandant_slug"):
        slug = request.session["mandant_slug"]
        if getattr(request.state, "hide_mandant_path_prefix", False):
            return RedirectResponse("/menu", status_code=302)
        return RedirectResponse(f"/m/{slug}/menu", status_code=302)
    from sqlalchemy.orm import sessionmaker

    from app.platform_database import platform_engine
    from app.platform_models import Ortsverband

    SessionP = sessionmaker(autocommit=False, autoflush=False, bind=platform_engine())
    pdb = SessionP()
    try:
        ovs = _query_ortsverbaende_sorted(pdb)
    finally:
        pdb.close()
    valid = {o.slug.strip().lower() for o in ovs}
    raw_ov = (request.query_params.get("ov") or "").strip().lower()
    preselect_ov_slug = raw_ov if raw_ov in valid else ""
    login_info = None
    if request.query_params.get("pending") == "1":
        login_info = "Dein Konto ist noch nicht freigegeben. Bitte warte auf einen Administrator."
    if request.query_params.get("registered") == "first":
        login_info = (
            "Als erster Nutzer bist du automatisch Administrator und freigeschaltet — "
            "du kannst dich jetzt anmelden."
        )
    elif request.query_params.get("registered") == "1":
        login_info = (
            "Registrierung gespeichert. Sobald ein Administrator dich freischaltet, "
            "kannst du dich anmelden."
        )
    return templates.TemplateResponse(
        request,
        "home.html",
        {
            "ovs": ovs,
            "preselect_ov_slug": preselect_ov_slug,
            "login_error": None,
            "login_info": login_info,
        },
    )
