from __future__ import annotations

import re
import uuid
from contextlib import asynccontextmanager
from datetime import date, datetime, time
from pathlib import Path
from types import SimpleNamespace
from typing import Annotated, List, Optional

from fastapi import APIRouter, Depends, FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.exception_handlers import http_exception_handler
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from starlette.templating import Jinja2Templates
from sqlalchemy import func
from sqlalchemy.orm import Session, selectinload
from starlette.middleware.sessions import SessionMiddleware
from pydantic import BaseModel, Field

from app.platform_models import (
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
    ICS_TOKEN,
    MAX_UPLOAD_MB,
    PUBLIC_SITE_MANDANT_SLUG,
    SECRET_KEY,
    SESSION_COOKIE,
    is_superadmin_username,
    upload_dir_for_slug,
)
from app.deps import AdminUser, AuthenticatedUser, CurrentUser
from app.ics_service import (
    all_termine_for_feed,
    build_ics_calendar,
    termine_for_user_teilnahmen,
)
from app.mandant_host import apply_mandant_host_path_rewrite
from app.platform_bootstrap import bootstrap_platform
from app.platform_database import get_platform_db
from app.settings_store import (
    ensure_ics_token_for_ui,
    ensure_user_calendar_token,
    verify_ics_token,
)
from app.superadmin_web import router as superadmin_router
from app.tenant_assets import sharepic_mask_url
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

ALLOWED_IMAGE = {"image/jpeg", "image/png", "image/webp"}
EXT_MAP = {".jpg": ".jpg", ".jpeg": ".jpg", ".png": ".png", ".webp": ".webp"}
USERNAME_PATTERN = re.compile(r"^[\w.-]+$", re.UNICODE)

tenant_router = APIRouter(prefix="/m/{mandant_slug}")


class TerminKommentarPayload(BaseModel):
    body: str = Field("", max_length=4000)


@asynccontextmanager
async def lifespan(app: FastAPI):
    bootstrap_platform()
    yield


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
    q = "?pending=1" if pending else ""
    s = slug.strip().lower()
    if getattr(request.state, "hide_mandant_path_prefix", False):
        return f"/login{q}"
    return f"/m/{s}/login{q}"


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


def _unlink_upload(rel: Optional[str], upload_root: Path) -> None:
    if not rel:
        return
    p = upload_root / rel
    try:
        p.unlink(missing_ok=True)
    except OSError:
        pass


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


def _approved_user_ovs_menu_items(
    pdb: Session,
    mandant_slug: str,
    user_id: int,
) -> list[dict[str, str | bool]]:
    """Freigegebene OV-Mitgliedschaften mit Anzeigenamen, sortiert für Menü / Wechsel."""
    ms = mandant_slug.strip().lower()
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
    out: list[dict[str, str | bool]] = []
    for m, ov in rows:
        slug = ov.slug.strip().lower()
        dn = (ov.display_name or "").strip() or slug.replace("-", " ").replace("_", " ").title()
        out.append(
            {
                "slug": slug,
                "display_name": dn,
                "href": f"/m/{slug}/menu",
                "current": slug == ms,
                "is_admin": bool(m.is_admin),
            },
        )
    return out


def _user_display_names(pdb: Session, user_ids: set[int]) -> dict[int, str]:
    if not user_ids:
        return {}
    rows = pdb.query(PlatformUser).filter(PlatformUser.id.in_(user_ids)).all()
    return {
        u.id: ((u.display_name or u.username).strip() or u.username) for u in rows
    }


def _termin_kommentare_public(
    pdb: Session, termin_id: int, user: AuthenticatedUser
) -> list[dict]:
    rows = (
        pdb.query(TerminKommentar)
        .filter(TerminKommentar.termin_id == termin_id)
        .order_by(TerminKommentar.created_at.asc())
        .all()
    )
    ids = {r.user_id for r in rows}
    names = _user_display_names(pdb, ids)
    out: list[dict] = []
    for r in rows:
        dt = r.created_at
        au = names.get(r.user_id, "Unbekannt")
        may_manage = bool(user.is_admin or r.user_id == user.id)
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
    return RedirectResponse(f"{_mp(request)}/login", status_code=302)


@tenant_router.get("/login", response_class=HTMLResponse)
def login_form(request: Request):
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


@tenant_router.post("/login", response_class=HTMLResponse)
def login_submit(
    mandant_slug: str,
    request: Request,
    pdb: Annotated[Session, Depends(get_platform_db)],
    username: Annotated[str, Form()],
    password: Annotated[str, Form()],
):
    uname = username.strip().lower()
    pu = (
        pdb.query(PlatformUser)
        .filter(func.lower(PlatformUser.username) == uname)
        .first()
    )
    if not pu or not verify_password(password, pu.password_hash):
        return templates.TemplateResponse(
            request,
            "login.html",
            {"error": "Benutzername oder Passwort falsch.", "info": None},
            status_code=401,
        )
    ms = mandant_slug.strip().lower()
    mem = (
        pdb.query(OvMembership)
        .filter(OvMembership.user_id == pu.id, OvMembership.ov_slug == ms)
        .first()
    )

    if is_superadmin_username(pu.username):
        pass
    elif mem is None:
        return templates.TemplateResponse(
            request,
            "login.html",
            {
                "error": None,
                "info": "Für diesen Ortsverband gibt es kein Konto. Bitte zuerst registrieren.",
            },
        )
    elif not mem.is_approved:
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
            return templates.TemplateResponse(
                request,
                "login.html",
                {
                    "error": None,
                    "info": "Dein Konto ist noch nicht freigegeben. Bitte warte auf einen Administrator.",
                },
            )

    request.session["user_id"] = pu.id
    request.session["mandant_slug"] = ms
    return RedirectResponse(f"{_mp(request)}/menu", status_code=302)


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
            "pending_count": _pending_approval_count(pdb, mandant_slug, user),
            "show_superadmin_link": is_superadmin_username(user.username),
            "my_ovs": _approved_user_ovs_menu_items(pdb, mandant_slug, user.id),
        },
    )


@tenant_router.get("/sharepic", response_class=HTMLResponse)
def sharepic_creator(mandant_slug: str, request: Request, user: CurrentUser):
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
def registrierung_form(request: Request):
    return templates.TemplateResponse(
        request,
        "registrierung.html",
        {
            "error": None,
            "name_value": "",
            "username_value": "",
        },
    )


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
    ctx = {
        "name_value": display_name,
        "username_value": username_raw,
    }
    if err:
        return templates.TemplateResponse(
            request,
            "registrierung.html",
            {"error": err, **ctx},
            status_code=400,
        )
    ms = mandant_slug.strip().lower()
    if (
        pdb.query(PlatformUser)
        .filter(func.lower(PlatformUser.username) == username_norm)
        .first()
    ):
        return templates.TemplateResponse(
            request,
            "registrierung.html",
            {
                "error": "Dieser Benutzername ist bereits vergeben.",
                **ctx,
            },
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
    if is_first_user:
        return RedirectResponse(f"{_mp(request)}/login?registered=first", status_code=302)
    return RedirectResponse(f"{_mp(request)}/login?registered=1", status_code=302)


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
    out: list = []
    for m in memberships:
        pu = pdb.get(PlatformUser, m.user_id)
        if not pu:
            continue
        out.append(
            SimpleNamespace(
                id=pu.id,
                username=pu.username,
                display_name=pu.display_name,
                created_at=pu.created_at,
                is_admin=m.is_admin,
                is_approved=m.is_approved,
            )
        )
    out.sort(key=lambda r: r.created_at)
    return out


@tenant_router.get("/admin/benutzer", response_class=HTMLResponse)
def admin_benutzer_list(
    mandant_slug: str,
    request: Request,
    pdb: Annotated[Session, Depends(get_platform_db)],
    user: AdminUser,
):
    all_users = _ov_user_rows_for_admin(pdb, mandant_slug)
    return templates.TemplateResponse(
        request,
        "admin_benutzer.html",
        {
            "user": user,
            "users": all_users,
            "admin_count": _admin_count(pdb, mandant_slug),
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
def logout(request: Request):
    request.session.pop("user_id", None)
    request.session.pop("mandant_slug", None)
    return RedirectResponse(f"{_mp(request)}/login", status_code=302)


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


def _termin_row_from_instance(
    pdb: Session,
    t: Termin,
    user: AuthenticatedUser,
    *,
    kommentar_count: int = 0,
) -> dict:
    uids = {tn.user_id for tn in t.teilnahmen}
    names_map = _user_display_names(pdb, uids)
    names = sorted(
        {(names_map.get(tn.user_id, "Unbekannt")).strip() for tn in t.teilnahmen},
        key=str.lower,
    )
    ich = any(tn.user_id == user.id for tn in t.teilnahmen)
    kann = _can_manage_termin(user, t)
    extern_labels = externe_teilnehmer_labels(
        externe_teilnehmer_decode(t.externe_teilnehmer_json),
    )
    teilnehmer_extern = sorted(extern_labels, key=str.lower)
    return {
        "termin": t,
        "teilnehmer": names,
        "teilnehmer_extern": teilnehmer_extern,
        "ich_teilnehme": ich,
        "kann_verwalten": kann,
        "kommentar_count": kommentar_count,
    }


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
) -> dict:
    if extern_gast is not None:
        auswahl = _filter_extern_gast_keys(extern_gast)
    elif termin is not None:
        auswahl = externe_teilnehmer_decode(termin.externe_teilnehmer_json)
    else:
        auswahl = []
    return {
        "user": user,
        "termin": termin,
        "error": error,
        "max_mb": MAX_UPLOAD_MB,
        "externe_optionen": EXTERNE_TEILNEHMER_OPTIONS,
        "externe_auswahl": auswahl,
    }


def _termin_list_rows(pdb: Session, mandant_slug: str, user: AuthenticatedUser) -> list[dict]:
    ms = mandant_slug.strip().lower()
    rows = (
        pdb.query(Termin)
        .options(selectinload(Termin.teilnahmen))
        .filter(Termin.mandant_slug == ms)
        .order_by(Termin.starts_at.asc())
        .all()
    )
    ids = [t.id for t in rows]
    counts = _termin_kommentar_counts_by_termin(pdb, ids)
    return [
        _termin_row_from_instance(pdb, t, user, kommentar_count=counts.get(t.id, 0))
        for t in rows
    ]


def _termin_detail_row(
    pdb: Session, mandant_slug: str, user: AuthenticatedUser, termin_id: int
) -> dict | None:
    ms = mandant_slug.strip().lower()
    t = (
        pdb.query(Termin)
        .options(selectinload(Termin.teilnahmen))
        .filter(Termin.id == termin_id, Termin.mandant_slug == ms)
        .first()
    )
    if not t:
        return None
    counts = _termin_kommentar_counts_by_termin(pdb, [t.id])
    return _termin_row_from_instance(
        pdb,
        t,
        user,
        kommentar_count=counts.get(t.id, 0),
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
    return templates.TemplateResponse(
        request,
        "termine_list.html",
        {
            "user": user,
            "termin_upcoming": termin_upcoming,
            "termin_past": termin_past,
            "feed_url_my": feed_url_my,
            "feed_url_all": feed_url_all,
        },
    )


@tenant_router.get("/termine/neu", response_class=HTMLResponse)
def termin_new_form(request: Request, user: CurrentUser):
    return templates.TemplateResponse(
        request,
        "termin_form.html",
        _termin_form_context(user=user, termin=None, error=None),
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
    vorbereitung: Annotated[str, Form()] = "",
    nachbereitung: Annotated[str, Form()] = "",
    location: Annotated[str, Form()] = "",
    end_uhrzeit: Annotated[str, Form()] = "",
    extern_gast: Annotated[Optional[List[str]], Form()] = None,
    bild: Annotated[Optional[UploadFile], File()] = None,
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
            ),
            status_code=400,
        )
    st = _combine(datum, start_uhrzeit)
    en = _combine(datum, end_uhrzeit) if end_uhrzeit.strip() else None
    if en and en <= st:
        en = None

    t = Termin(
        mandant_slug=mandant_slug.strip().lower(),
        title=title.strip(),
        description=description.strip(),
        vorbereitung=vorbereitung.strip(),
        nachbereitung=nachbereitung.strip(),
        location=location.strip(),
        starts_at=st,
        ends_at=en,
        externe_teilnehmer_json=externe_teilnehmer_encode(
            _filter_extern_gast_keys(extern_gast),
        ),
        created_by_id=user.id,
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
                            ),
                            status_code=400,
                        )
                    f.write(chunk)
            t.image_path = dest_name
            pdb.add(t)

    pdb.commit()
    return RedirectResponse(f"{_mp(request)}/termine/{t.id}", status_code=302)


@tenant_router.get("/termine/{termin_id}", response_class=HTMLResponse)
def termin_detail(
    mandant_slug: str,
    termin_id: int,
    request: Request,
    pdb: Annotated[Session, Depends(get_platform_db)],
    user: CurrentUser,
):
    row = _termin_detail_row(pdb, mandant_slug, user, termin_id)
    if not row:
        raise HTTPException(status_code=404, detail="Termin nicht gefunden")
    termin_vergangen = row["termin"].starts_at < datetime.utcnow()
    kommentare = _termin_kommentare_public(pdb, termin_id, user)
    return templates.TemplateResponse(
        request,
        "termin_detail.html",
        {
            "user": user,
            "row": row,
            "termin_vergangen": termin_vergangen,
            "termin_kommentare": kommentare,
        },
    )


@tenant_router.post("/termine/{termin_id}/kommentare")
def termin_kommentar_create(
    mandant_slug: str,
    termin_id: int,
    payload: TerminKommentarPayload,
    pdb: Annotated[Session, Depends(get_platform_db)],
    user: CurrentUser,
):
    ms = mandant_slug.strip().lower()
    body_txt = payload.body.strip()
    if not body_txt:
        raise HTTPException(status_code=400, detail="Kommentar darf nicht leer sein.")
    t = (
        pdb.query(Termin)
        .filter(Termin.id == termin_id, Termin.mandant_slug == ms)
        .first()
    )
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
            "kommentare": _termin_kommentare_public(pdb, termin_id, user),
        },
    )


@tenant_router.patch("/termine/{termin_id}/kommentare/{kommentar_id}")
def termin_kommentar_update(
    mandant_slug: str,
    termin_id: int,
    kommentar_id: int,
    payload: TerminKommentarPayload,
    pdb: Annotated[Session, Depends(get_platform_db)],
    user: CurrentUser,
):
    body_txt = payload.body.strip()
    if not body_txt:
        raise HTTPException(status_code=400, detail="Kommentar darf nicht leer sein.")
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
    if not (user.is_admin or km.user_id == user.id):
        raise HTTPException(status_code=403, detail="Keine Berechtigung.")
    km.body = body_txt[:4000]
    pdb.add(km)
    pdb.commit()
    return JSONResponse(
        {
            "ok": True,
            "kommentare": _termin_kommentare_public(pdb, termin_id, user),
        },
    )


@tenant_router.delete("/termine/{termin_id}/kommentare/{kommentar_id}")
def termin_kommentar_delete(
    mandant_slug: str,
    termin_id: int,
    kommentar_id: int,
    pdb: Annotated[Session, Depends(get_platform_db)],
    user: CurrentUser,
):
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
    if not (user.is_admin or km.user_id == user.id):
        raise HTTPException(status_code=403, detail="Keine Berechtigung.")
    pdb.delete(km)
    pdb.commit()
    return JSONResponse(
        {
            "ok": True,
            "kommentare": _termin_kommentare_public(pdb, termin_id, user),
        },
    )


@tenant_router.post("/termine/{termin_id}/teilnehmen")
def termin_teilnehmen(
    mandant_slug: str,
    termin_id: int,
    request: Request,
    pdb: Annotated[Session, Depends(get_platform_db)],
    user: CurrentUser,
    return_to: Annotated[str | None, Form()] = None,
):
    ms = mandant_slug.strip().lower()
    t = (
        pdb.query(Termin)
        .filter(Termin.id == termin_id, Termin.mandant_slug == ms)
        .first()
    )
    if not t:
        raise HTTPException(status_code=404, detail="Termin nicht gefunden")
    exists = (
        pdb.query(TerminTeilnahme)
        .filter_by(termin_id=termin_id, user_id=user.id)
        .first()
    )
    if not exists:
        pdb.add(
            TerminTeilnahme(termin_id=termin_id, user_id=user.id),
        )
        pdb.commit()
    if return_to == "list":
        return RedirectResponse(f"{_mp(request)}/termine", status_code=302)
    return RedirectResponse(f"{_mp(request)}/termine/{termin_id}", status_code=302)


@tenant_router.post("/termine/{termin_id}/abmelden")
@tenant_router.post("/termine/{termin_id}/absagen")
def termin_abmelden(
    mandant_slug: str,
    termin_id: int,
    request: Request,
    pdb: Annotated[Session, Depends(get_platform_db)],
    user: CurrentUser,
    return_to: Annotated[str | None, Form()] = None,
):
    row = (
        pdb.query(TerminTeilnahme)
        .filter_by(termin_id=termin_id, user_id=user.id)
        .first()
    )
    if row:
        pdb.delete(row)
        pdb.commit()
    if return_to == "list":
        return RedirectResponse(f"{_mp(request)}/termine", status_code=302)
    return RedirectResponse(f"{_mp(request)}/termine/{termin_id}", status_code=302)


@tenant_router.get("/termine/{termin_id}/bearbeiten", response_class=HTMLResponse)
def termin_edit_form(
    mandant_slug: str,
    termin_id: int,
    request: Request,
    pdb: Annotated[Session, Depends(get_platform_db)],
    user: CurrentUser,
):
    ms = mandant_slug.strip().lower()
    t = (
        pdb.query(Termin)
        .filter(Termin.id == termin_id, Termin.mandant_slug == ms)
        .first()
    )
    if not t:
        raise HTTPException(status_code=404, detail="Termin nicht gefunden")
    if not _can_manage_termin(user, t):
        raise HTTPException(
            status_code=403,
            detail="Du darfst diesen Termin nicht bearbeiten.",
        )
    return templates.TemplateResponse(
        request,
        "termin_form.html",
        _termin_form_context(user=user, termin=t, error=None),
    )


@tenant_router.post("/termine/{termin_id}/bearbeiten", response_class=HTMLResponse)
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
    vorbereitung: Annotated[str, Form()] = "",
    nachbereitung: Annotated[str, Form()] = "",
    location: Annotated[str, Form()] = "",
    end_uhrzeit: Annotated[str, Form()] = "",
    bild_entfernen: Annotated[str, Form()] = "",
    extern_gast: Annotated[Optional[List[str]], Form()] = None,
    bild: Annotated[Optional[UploadFile], File()] = None,
):
    ms = mandant_slug.strip().lower()
    t = (
        pdb.query(Termin)
        .filter(Termin.id == termin_id, Termin.mandant_slug == ms)
        .first()
    )
    if not t:
        raise HTTPException(status_code=404, detail="Termin nicht gefunden")
    if not _can_manage_termin(user, t):
        raise HTTPException(
            status_code=403,
            detail="Du darfst diesen Termin nicht bearbeiten.",
        )

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
            ),
            status_code=400,
        )
    st = _combine(datum, start_uhrzeit)
    en = _combine(datum, end_uhrzeit) if end_uhrzeit.strip() else None
    if en and en <= st:
        en = None

    t.title = title.strip()
    t.description = description.strip()
    t.vorbereitung = vorbereitung.strip()
    t.nachbereitung = nachbereitung.strip()
    t.location = location.strip()
    t.starts_at = st
    t.ends_at = en
    t.externe_teilnehmer_json = externe_teilnehmer_encode(
        _filter_extern_gast_keys(extern_gast),
    )

    if bild_entfernen == "1":
        _unlink_upload(t.image_path, _upload_root(request))
        t.image_path = None

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
                            ),
                            status_code=400,
                        )
                    f.write(chunk)
            _unlink_upload(t.image_path, _upload_root(request))
            t.image_path = dest_name

    pdb.add(t)
    pdb.commit()
    return RedirectResponse(f"{_mp(request)}/termine/{termin_id}", status_code=302)


@tenant_router.get("/termine/{termin_id}/loeschen", response_class=HTMLResponse)
def termin_delete_confirm(
    mandant_slug: str,
    termin_id: int,
    request: Request,
    pdb: Annotated[Session, Depends(get_platform_db)],
    user: CurrentUser,
):
    ms = mandant_slug.strip().lower()
    t = (
        pdb.query(Termin)
        .filter(Termin.id == termin_id, Termin.mandant_slug == ms)
        .first()
    )
    if not t:
        raise HTTPException(status_code=404, detail="Termin nicht gefunden")
    if not _can_manage_termin(user, t):
        raise HTTPException(
            status_code=403,
            detail="Du darfst diesen Termin nicht löschen.",
        )
    return templates.TemplateResponse(
        request,
        "termin_loeschen.html",
        {"user": user, "termin": t},
    )


@tenant_router.post("/termine/{termin_id}/loeschen")
def termin_delete_do(
    mandant_slug: str,
    termin_id: int,
    request: Request,
    pdb: Annotated[Session, Depends(get_platform_db)],
    user: CurrentUser,
):
    ms = mandant_slug.strip().lower()
    t = (
        pdb.query(Termin)
        .filter(Termin.id == termin_id, Termin.mandant_slug == ms)
        .first()
    )
    if not t:
        raise HTTPException(status_code=404, detail="Termin nicht gefunden")
    if not _can_manage_termin(user, t):
        raise HTTPException(
            status_code=403,
            detail="Du darfst diesen Termin nicht löschen.",
        )
    _unlink_upload(t.image_path, _upload_root(request))
    pdb.delete(t)
    pdb.commit()
    return RedirectResponse(f"{_mp(request)}/termine", status_code=302)


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
    body = build_ics_calendar(termine)
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
    body = build_ics_calendar(termine, cal_name="Meine Zusagen — Wahlkampf")
    return Response(
        content=body,
        media_type="text/calendar; charset=utf-8",
        headers={
            "Content-Disposition": 'attachment; filename="meine-termine.ics"',
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
        ovs = pdb.query(Ortsverband).order_by(Ortsverband.slug.asc()).all()
    finally:
        pdb.close()
    return templates.TemplateResponse(request, "home.html", {"ovs": ovs})
