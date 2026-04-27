from __future__ import annotations

import re
import uuid
from contextlib import asynccontextmanager
from datetime import date, datetime, time
from pathlib import Path
from typing import Annotated, List, Optional

from fastapi import Depends, FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.exception_handlers import http_exception_handler
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from starlette.templating import Jinja2Templates
from sqlalchemy import func
from sqlalchemy.orm import Session, selectinload
from starlette.middleware.sessions import SessionMiddleware

from app import models
from app.auth import hash_password, verify_password
from app.config import (
    ICS_TOKEN,
    MAX_UPLOAD_MB,
    SECRET_KEY,
    SESSION_COOKIE,
    UPLOAD_DIR,
)
from app.database import engine, get_db
from app.db_migrate import run_sqlite_migrations
from app.deps import AdminUser, CurrentUser
from app.ics_service import all_termine_for_feed, build_ics_calendar
from app.plakate_db import PlakatBase, get_plakate_db, plakate_engine
from app.plakate_models import Plakat
from app.settings_store import ensure_ics_token_for_ui, verify_ics_token
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

UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
STATIC_DIR.mkdir(parents=True, exist_ok=True)

ALLOWED_IMAGE = {"image/jpeg", "image/png", "image/webp"}
EXT_MAP = {".jpg": ".jpg", ".jpeg": ".jpg", ".png": ".png", ".webp": ".webp"}
USERNAME_PATTERN = re.compile(r"^[\w.-]+$", re.UNICODE)

PlakatDB = Annotated[Session, Depends(get_plakate_db)]


@asynccontextmanager
async def lifespan(app: FastAPI):
    models.Base.metadata.create_all(bind=engine)
    run_sqlite_migrations(engine)
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    (UPLOAD_DIR / "plakate").mkdir(parents=True, exist_ok=True)
    PlakatBase.metadata.create_all(bind=plakate_engine)
    yield


app = FastAPI(title="Wahlkampf", lifespan=lifespan)
app.add_middleware(SessionMiddleware, secret_key=SECRET_KEY, session_cookie=SESSION_COOKIE)


@app.exception_handler(HTTPException)
async def http_exc(request: Request, exc: HTTPException):
    accept = request.headers.get("accept") or ""
    wants_html = "text/html" in accept or accept.startswith("*/*")
    if exc.status_code == 401 and wants_html:
        if exc.detail == "Konto noch nicht freigegeben.":
            return RedirectResponse("/login?pending=1", status_code=302)
        return RedirectResponse("/login", status_code=302)
    if exc.status_code == 403 and wants_html:
        msg = exc.detail if isinstance(exc.detail, str) else "Keine Berechtigung."
        return templates.TemplateResponse(
            request,
            "forbidden.html",
            {"message": msg},
            status_code=403,
        )
    return await http_exception_handler(request, exc)


app.mount("/media", StaticFiles(directory=str(UPLOAD_DIR)), name="media")
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


def _app_path_prefix(request: Request) -> str:
    """Pfad-Präfix hinter Reverse-Proxy (uvicorn --root-path) für PWA scope/start_url."""
    return (request.scope.get("root_path") or "").rstrip("/")


@app.get("/manifest.webmanifest", include_in_schema=False)
def web_app_manifest(request: Request):
    prefix = _app_path_prefix(request)
    start_url = f"{prefix}/menu" if prefix else "/menu"
    scope = f"{prefix}/" if prefix else "/"
    icon_path = f"{prefix}/static/icon.svg" if prefix else "/static/icon.svg"
    body = {
        "name": "Wahlkampf",
        "short_name": "Wahlkampf",
        "description": "Termine, Menü und Organisation im Wahlkampf.",
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


def _can_manage_termin(user: models.User, termin: models.Termin) -> bool:
    return bool(user.is_admin or termin.created_by_id == user.id)


def _unlink_upload(rel: Optional[str]) -> None:
    if not rel:
        return
    p = UPLOAD_DIR / rel
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


def _pending_approval_count(db: Session, user: models.User) -> int:
    if not user.is_admin:
        return 0
    return (
        db.query(models.User)
        .filter(models.User.is_approved.is_(False))
        .count()
    )


def _user_display_names(db: Session, user_ids: set[int]) -> dict[int, str]:
    if not user_ids:
        return {}
    rows = (
        db.query(models.User)
        .filter(models.User.id.in_(user_ids))
        .all()
    )
    return {
        u.id: ((u.display_name or u.username).strip() or u.username) for u in rows
    }


def _plakate_list_payload(db: Session, pdb: Session) -> list[dict]:
    rows = pdb.query(Plakat).order_by(Plakat.hung_at.desc()).all()
    ids: set[int] = set()
    for r in rows:
        ids.add(r.hung_by_user_id)
        if r.removed_by_user_id:
            ids.add(r.removed_by_user_id)
    names = _user_display_names(db, ids)
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
                "image_url": f"/media/{r.image_path}" if r.image_path else None,
                "note": (r.note or "").strip(),
                "removed_by_id": r.removed_by_user_id,
                "removed_by_name": names.get(r.removed_by_user_id)
                if r.removed_by_user_id
                else None,
                "removed_at": r.removed_at.isoformat() if r.removed_at else None,
            },
        )
    return out


@app.get("/", response_class=HTMLResponse)
def root(request: Request):
    if request.session.get("user_id"):
        return RedirectResponse("/menu", status_code=302)
    return templates.TemplateResponse(request, "home.html", {})


@app.get("/login", response_class=HTMLResponse)
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


@app.post("/login", response_class=HTMLResponse)
def login_submit(
    request: Request,
    db: Annotated[Session, Depends(get_db)],
    username: Annotated[str, Form()],
    password: Annotated[str, Form()],
):
    uname = username.strip()
    user = (
        db.query(models.User)
        .filter(func.lower(models.User.username) == uname.lower())
        .first()
    )
    if not user or not verify_password(password, user.password_hash):
        return templates.TemplateResponse(
            request,
            "login.html",
            {"error": "Benutzername oder Passwort falsch.", "info": None},
            status_code=401,
        )
    if not user.is_approved:
        # Kein aktiver Administrator → Notfall: einloggender Nutzer wird freigeschaltet
        # und Admin (z. B. nach fehlerhaftem Gründer-Flag oder leerer Verwaltung).
        has_active_admin = (
            db.query(models.User)
            .filter(
                models.User.is_admin.is_(True),
                models.User.is_approved.is_(True),
            )
            .first()
        )
        if not has_active_admin:
            user.is_approved = True
            user.is_admin = True
            db.merge(models.AppSetting(key="founder_done", value="1"))
            db.commit()
        else:
            return templates.TemplateResponse(
                request,
                "login.html",
                {
                    "error": None,
                    "info": "Dein Konto ist noch nicht freigegeben. Bitte warte auf einen Administrator.",
                },
            )
    request.session["user_id"] = user.id
    return RedirectResponse("/menu", status_code=302)


@app.get("/menu", response_class=HTMLResponse)
def app_menu(
    request: Request,
    db: Annotated[Session, Depends(get_db)],
    user: CurrentUser,
):
    return templates.TemplateResponse(
        request,
        "menu.html",
        {
            "user": user,
            "pending_count": _pending_approval_count(db, user),
        },
    )


@app.get("/sharepic", response_class=HTMLResponse)
def sharepic_creator(request: Request, user: CurrentUser):
    return templates.TemplateResponse(
        request,
        "sharepic.html",
        {"user": user},
    )


@app.get("/plakate", response_class=HTMLResponse)
def plakate_view(
    request: Request,
    db: Annotated[Session, Depends(get_db)],
    pdb: PlakatDB,
    user: CurrentUser,
):
    return templates.TemplateResponse(
        request,
        "plakate.html",
        {
            "user": user,
            "plakate_initial": _plakate_list_payload(db, pdb),
            "max_mb": MAX_UPLOAD_MB,
        },
    )


@app.get("/plakate/api/list")
def plakate_api_list(
    db: Annotated[Session, Depends(get_db)],
    pdb: PlakatDB,
    _: CurrentUser,
):
    return JSONResponse(_plakate_list_payload(db, pdb))


@app.post("/plakate/api/hinzufuegen")
async def plakate_hinzufuegen(
    request: Request,
    db: Annotated[Session, Depends(get_db)],
    pdb: PlakatDB,
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
    p = Plakat(
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
            dest = UPLOAD_DIR / rel
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
    return JSONResponse({"ok": True, "plakate": _plakate_list_payload(db, pdb)})


@app.post("/plakate/api/abhaengen/{plakat_id}")
def plakate_abhaengen(
    plakat_id: int,
    db: Annotated[Session, Depends(get_db)],
    pdb: PlakatDB,
    user: CurrentUser,
):
    p = pdb.get(Plakat, plakat_id)
    if not p or p.removed_at is not None:
        raise HTTPException(
            status_code=404,
            detail="Plakat nicht gefunden oder bereits abgehängt.",
        )
    p.removed_by_user_id = user.id
    p.removed_at = datetime.utcnow()
    pdb.add(p)
    pdb.commit()
    return JSONResponse({"ok": True, "plakate": _plakate_list_payload(db, pdb)})


@app.get("/registrierung", response_class=HTMLResponse)
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


@app.post("/registrierung", response_class=HTMLResponse)
def registrierung_submit(
    request: Request,
    db: Annotated[Session, Depends(get_db)],
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
    if (
        db.query(models.User)
        .filter(func.lower(models.User.username) == username_norm)
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
    # Gründer:in = erste erfolgreiche Registrierung (persistiertes Flag, nicht nur User-Zähler)
    founder_done = db.get(models.AppSetting, "founder_done")
    is_first_user = founder_done is None
    db.add(
        models.User(
            username=username_norm,
            password_hash=hash_password(password),
            display_name=display_name,
            is_approved=is_first_user,
            is_admin=is_first_user,
        ),
    )
    if is_first_user:
        db.merge(models.AppSetting(key="founder_done", value="1"))
    db.commit()
    if is_first_user:
        return RedirectResponse("/login?registered=first", status_code=302)
    return RedirectResponse("/login?registered=1", status_code=302)


def _admin_count(db: Session) -> int:
    return (
        db.query(models.User).filter(models.User.is_admin.is_(True)).count()
    )


@app.get("/admin/benutzer", response_class=HTMLResponse)
def admin_benutzer_list(
    request: Request,
    db: Annotated[Session, Depends(get_db)],
    user: AdminUser,
):
    all_users = (
        db.query(models.User).order_by(models.User.created_at.asc()).all()
    )
    return templates.TemplateResponse(
        request,
        "admin_benutzer.html",
        {
            "user": user,
            "users": all_users,
            "admin_count": _admin_count(db),
        },
    )


@app.post("/admin/benutzer/{uid}/freigeben")
def admin_benutzer_freigeben(
    uid: int,
    db: Annotated[Session, Depends(get_db)],
    _: AdminUser,
):
    u = db.get(models.User, uid)
    if u and not u.is_approved:
        u.is_approved = True
        db.commit()
    return RedirectResponse("/admin/benutzer", status_code=302)


@app.post("/admin/benutzer/{uid}/admin-ernennen")
def admin_benutzer_admin_ernennen(
    uid: int,
    db: Annotated[Session, Depends(get_db)],
    _: AdminUser,
):
    u = db.get(models.User, uid)
    if u:
        u.is_admin = True
        u.is_approved = True
        db.commit()
    return RedirectResponse("/admin/benutzer", status_code=302)


@app.post("/admin/benutzer/{uid}/admin-entfernen")
def admin_benutzer_admin_entfernen(
    uid: int,
    db: Annotated[Session, Depends(get_db)],
    _: AdminUser,
):
    u = db.get(models.User, uid)
    if not u or not u.is_admin:
        return RedirectResponse("/admin/benutzer", status_code=302)
    if _admin_count(db) <= 1:
        raise HTTPException(
            status_code=403,
            detail="Es muss mindestens ein Administrator bleiben.",
        )
    u.is_admin = False
    db.commit()
    return RedirectResponse("/admin/benutzer", status_code=302)


@app.post("/admin/benutzer/{uid}/loeschen")
def admin_benutzer_loeschen(
    uid: int,
    db: Annotated[Session, Depends(get_db)],
    actor: AdminUser,
):
    if uid == actor.id:
        raise HTTPException(
            status_code=403,
            detail="Du kannst dein eigenes Konto nicht löschen.",
        )
    u = db.get(models.User, uid)
    if not u:
        return RedirectResponse("/admin/benutzer", status_code=302)
    if u.is_admin and _admin_count(db) <= 1:
        raise HTTPException(
            status_code=403,
            detail="Der letzte Administrator kann nicht gelöscht werden.",
        )
    db.query(models.Termin).filter(
        models.Termin.created_by_id == uid,
    ).update(
        {models.Termin.created_by_id: actor.id},
        synchronize_session=False,
    )
    db.delete(u)
    db.commit()
    return RedirectResponse("/admin/benutzer", status_code=302)


@app.get("/logout")
def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/login", status_code=302)


def _termin_row_from_instance(t: models.Termin, user: models.User) -> dict:
    names = sorted(
        {
            (tn.user.display_name or tn.user.username).strip()
            for tn in t.teilnahmen
        },
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
    }


def _filter_extern_gast_keys(extern_gast: Optional[List[str]]) -> list[str]:
    if not extern_gast:
        return []
    return sorted({str(x) for x in extern_gast if str(x) in EXTERNE_TEILNEHMER_KEYS})


def _termin_form_context(
    *,
    user: models.User,
    termin: Optional[models.Termin],
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


def _termin_list_rows(db: Session, user: models.User) -> list[dict]:
    rows = (
        db.query(models.Termin)
        .options(
            selectinload(models.Termin.teilnahmen).selectinload(
                models.TerminTeilnahme.user
            ),
        )
        .order_by(models.Termin.starts_at.asc())
        .all()
    )
    return [_termin_row_from_instance(t, user) for t in rows]


def _termin_detail_row(db: Session, user: models.User, termin_id: int) -> dict | None:
    t = (
        db.query(models.Termin)
        .options(
            selectinload(models.Termin.teilnahmen).selectinload(
                models.TerminTeilnahme.user
            ),
        )
        .filter(models.Termin.id == termin_id)
        .first()
    )
    if not t:
        return None
    return _termin_row_from_instance(t, user)


def _split_termine_upcoming_past(rows: list[dict]) -> tuple[list[dict], list[dict]]:
    now = datetime.utcnow()
    upcoming = [r for r in rows if r["termin"].starts_at >= now]
    past = [r for r in rows if r["termin"].starts_at < now]
    past.sort(key=lambda r: r["termin"].starts_at, reverse=True)
    return upcoming, past


@app.get("/termine", response_class=HTMLResponse)
def termine_list(
    request: Request,
    db: Annotated[Session, Depends(get_db)],
    user: CurrentUser,
):
    termin_rows = _termin_list_rows(db, user)
    termin_upcoming, termin_past = _split_termine_upcoming_past(termin_rows)
    token = ensure_ics_token_for_ui(db, ICS_TOKEN)
    base = str(request.base_url).rstrip("/")
    feed_url = f"{base}/calendar.ics?t={token}"
    return templates.TemplateResponse(
        request,
        "termine_list.html",
        {
            "user": user,
            "termin_upcoming": termin_upcoming,
            "termin_past": termin_past,
            "feed_url": feed_url,
        },
    )


@app.get("/termine/neu", response_class=HTMLResponse)
def termin_new_form(request: Request, user: CurrentUser):
    return templates.TemplateResponse(
        request,
        "termin_form.html",
        _termin_form_context(user=user, termin=None, error=None),
    )


@app.post("/termine/neu", response_class=HTMLResponse)
async def termin_create(
    request: Request,
    db: Annotated[Session, Depends(get_db)],
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

    t = models.Termin(
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
    db.add(t)
    db.flush()

    if bild and bild.filename:
        ext = _safe_ext(bild.filename, bild.content_type)
        if ext and bild.content_type in ALLOWED_IMAGE:
            max_b = MAX_UPLOAD_MB * 1024 * 1024
            dest_name = f"{t.id}_{uuid.uuid4().hex}{ext}"
            dest = UPLOAD_DIR / dest_name
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
            db.add(t)

    db.commit()
    return RedirectResponse(f"/termine/{t.id}", status_code=302)


@app.get("/termine/{termin_id}", response_class=HTMLResponse)
def termin_detail(
    termin_id: int,
    request: Request,
    db: Annotated[Session, Depends(get_db)],
    user: CurrentUser,
):
    row = _termin_detail_row(db, user, termin_id)
    if not row:
        raise HTTPException(status_code=404, detail="Termin nicht gefunden")
    termin_vergangen = row["termin"].starts_at < datetime.utcnow()
    return templates.TemplateResponse(
        request,
        "termin_detail.html",
        {
            "user": user,
            "row": row,
            "termin_vergangen": termin_vergangen,
        },
    )


@app.post("/termine/{termin_id}/teilnehmen")
def termin_teilnehmen(
    termin_id: int,
    db: Annotated[Session, Depends(get_db)],
    user: CurrentUser,
    return_to: Annotated[str | None, Form()] = None,
):
    t = db.get(models.Termin, termin_id)
    if not t:
        raise HTTPException(status_code=404, detail="Termin nicht gefunden")
    exists = (
        db.query(models.TerminTeilnahme)
        .filter_by(termin_id=termin_id, user_id=user.id)
        .first()
    )
    if not exists:
        db.add(
            models.TerminTeilnahme(termin_id=termin_id, user_id=user.id),
        )
        db.commit()
    if return_to == "list":
        return RedirectResponse("/termine", status_code=302)
    return RedirectResponse(f"/termine/{termin_id}", status_code=302)


@app.post("/termine/{termin_id}/abmelden")
@app.post("/termine/{termin_id}/absagen")
def termin_abmelden(
    termin_id: int,
    db: Annotated[Session, Depends(get_db)],
    user: CurrentUser,
    return_to: Annotated[str | None, Form()] = None,
):
    row = (
        db.query(models.TerminTeilnahme)
        .filter_by(termin_id=termin_id, user_id=user.id)
        .first()
    )
    if row:
        db.delete(row)
        db.commit()
    if return_to == "list":
        return RedirectResponse("/termine", status_code=302)
    return RedirectResponse(f"/termine/{termin_id}", status_code=302)


@app.get("/termine/{termin_id}/bearbeiten", response_class=HTMLResponse)
def termin_edit_form(
    termin_id: int,
    request: Request,
    db: Annotated[Session, Depends(get_db)],
    user: CurrentUser,
):
    t = db.get(models.Termin, termin_id)
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


@app.post("/termine/{termin_id}/bearbeiten", response_class=HTMLResponse)
async def termin_edit_save(
    termin_id: int,
    request: Request,
    db: Annotated[Session, Depends(get_db)],
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
    t = db.get(models.Termin, termin_id)
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
        _unlink_upload(t.image_path)
        t.image_path = None

    if bild and bild.filename:
        ext = _safe_ext(bild.filename, bild.content_type)
        if ext and bild.content_type in ALLOWED_IMAGE:
            max_b = MAX_UPLOAD_MB * 1024 * 1024
            dest_name = f"{t.id}_{uuid.uuid4().hex}{ext}"
            dest = UPLOAD_DIR / dest_name
            size = 0
            with dest.open("wb") as f:
                while chunk := await bild.read(1024 * 1024):
                    size += len(chunk)
                    if size > max_b:
                        dest.unlink(missing_ok=True)
                        db.rollback()
                        db.refresh(t)
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
            _unlink_upload(t.image_path)
            t.image_path = dest_name

    db.add(t)
    db.commit()
    return RedirectResponse(f"/termine/{termin_id}", status_code=302)


@app.get("/termine/{termin_id}/loeschen", response_class=HTMLResponse)
def termin_delete_confirm(
    termin_id: int,
    request: Request,
    db: Annotated[Session, Depends(get_db)],
    user: CurrentUser,
):
    t = db.get(models.Termin, termin_id)
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


@app.post("/termine/{termin_id}/loeschen")
def termin_delete_do(
    termin_id: int,
    db: Annotated[Session, Depends(get_db)],
    user: CurrentUser,
):
    t = db.get(models.Termin, termin_id)
    if not t:
        raise HTTPException(status_code=404, detail="Termin nicht gefunden")
    if not _can_manage_termin(user, t):
        raise HTTPException(
            status_code=403,
            detail="Du darfst diesen Termin nicht löschen.",
        )
    _unlink_upload(t.image_path)
    db.delete(t)
    db.commit()
    return RedirectResponse("/termine", status_code=302)


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


@app.get("/calendar.ics")
def calendar_ics(
    request: Request,
    db: Annotated[Session, Depends(get_db)],
    t: Optional[str] = None,
):
    if not verify_ics_token(db, ICS_TOKEN, t):
        raise HTTPException(status_code=404, detail="Not found")
    termine = all_termine_for_feed(db)
    body = build_ics_calendar(termine)
    return Response(
        content=body,
        media_type="text/calendar; charset=utf-8",
        headers={
            "Content-Disposition": 'attachment; filename="wahlkampf.ics"',
            "Cache-Control": "no-store",
        },
    )
