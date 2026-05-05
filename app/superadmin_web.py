from __future__ import annotations

from typing import Annotated, List, Optional
from urllib.parse import quote, urlparse

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy import delete, func, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, selectinload
from starlette.templating import Jinja2Templates

from app.deps import LetzgoSuperadmin
from app.auth import hash_password
from app.config import PUBLIC_SITE_MANDANT_SLUG, is_superadmin_username
from app.ov_services import (
    delete_ortsverband_completely,
    register_ortsverband,
    validate_ov_slug,
)
from app.rss_fraktion_import import import_fraktion_termine_from_feed
from app.mandant_features import (
    FEATURE_FRAKTION,
    FEATURE_PLAKATE,
    FEATURE_SHAREPIC,
    is_mandant_feature_enabled,
    merge_mandant_feature,
)
from app.platform_database import get_platform_db
from app.platform_models import (
    Ortsverband,
    OvMembership,
    PlatformUser,
    Termin,
    TerminKommentar,
    TerminTeilnahme,
)

TEMPLATES_DIR = __import__("pathlib").Path(__file__).resolve().parent / "templates"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

router = APIRouter(tags=["superadmin"])

PASSWORD_MIN_SUPERADMIN = 8


def _form_ov_slug_list(raw: Optional[List[str] | str]) -> List[str]:
    """Checkbox-Werte: bei einem Eintrag liefert Starlette teils str statt list."""
    if raw is None:
        return []
    if isinstance(raw, str):
        s = raw.strip().lower()
        return [s] if s else []
    out: List[str] = []
    for x in raw:
        if not x:
            continue
        s = str(x).strip().lower()
        if s:
            out.append(s)
    return out


def _show_superadmin_delete_link(request: Request, pu: PlatformUser) -> bool:
    if is_superadmin_username(pu.username):
        return False
    suid = request.session.get("user_id")
    if suid is None:
        return False
    try:
        return int(suid) != pu.id
    except (TypeError, ValueError):
        return False


def _purge_dependencies_before_platform_user_delete(db: Session, user_id: int) -> None:
    """Bereinigt Zeilen, die auf dieses Nutzerkonto zeigen.

    Ältere platform.db-Dateien können ohne durchgängige ON DELETE CASCADE/SET NULL
    angelegt sein — dann schlägt ``DELETE FROM platform_users`` trotz aktuellem ORM fehl.
    """
    db.execute(delete(TerminKommentar).where(TerminKommentar.user_id == user_id))
    db.execute(delete(TerminTeilnahme).where(TerminTeilnahme.user_id == user_id))
    db.execute(delete(OvMembership).where(OvMembership.user_id == user_id))
    db.execute(
        update(Termin)
        .where(Termin.created_by_id == user_id)
        .values(created_by_id=None),
    )


def _superadmin_user_delete_blocked(
    request: Request,
    db: Session,
    pu: PlatformUser,
    user_id: int,
) -> Optional[str]:
    if is_superadmin_username(pu.username):
        return "Plattform-Superadmin-Konten können hier nicht gelöscht werden."
    suid = request.session.get("user_id")
    try:
        if suid is not None and int(suid) == user_id:
            return "Du kannst dein eigenes Konto hier nicht löschen."
    except (TypeError, ValueError):
        pass
    return None


def _superadmin_user_form_template_ctx(
    request: Request,
    pu: PlatformUser,
    ovs: list,
    mem_by_slug: dict,
    *,
    error: Optional[str] = None,
    flash_ok: bool = False,
) -> dict:
    return {
        "edit_user": pu,
        "ovs": ovs,
        "mem_by_slug": mem_by_slug,
        "error": error,
        "platform_superadmin": is_superadmin_username(pu.username),
        "flash_ok": flash_ok,
        "show_delete_link": _show_superadmin_delete_link(request, pu),
    }


def _sync_ov_memberships_superadmin(
    db: Session,
    user_id: int,
    member_slugs: List[str],
    admin_slugs: set[str],
    fraktion_slugs: set[str],
) -> None:
    """OV-Zuordnungen aus Superadmin-Sicht: immer freigegeben (`is_approved=True`)."""
    member_set = {s.strip().lower() for s in member_slugs if s and s.strip()}
    if not member_set:
        valid: set[str] = set()
    else:
        valid = {
            r[0].strip().lower()
            for r in db.query(Ortsverband.slug).filter(Ortsverband.slug.in_(member_set)).all()
        }
    member_set &= valid
    admin_set = {s.strip().lower() for s in admin_slugs} & member_set
    fraktion_set = {s.strip().lower() for s in fraktion_slugs} & member_set

    rows = db.query(OvMembership).filter(OvMembership.user_id == user_id).all()
    by_slug = {m.ov_slug.strip().lower(): m for m in rows}
    for slug in member_set:
        m = by_slug.pop(slug, None)
        if m:
            m.is_approved = True
            m.is_admin = slug in admin_set
            m.fraktion_member = slug in fraktion_set
            db.add(m)
        else:
            db.add(
                OvMembership(
                    user_id=user_id,
                    ov_slug=slug,
                    is_admin=slug in admin_set,
                    is_approved=True,
                    fraktion_member=slug in fraktion_set,
                )
            )
    for m in by_slug.values():
        db.delete(m)


def _validate_optional_fraktion_rss_url(raw: str) -> tuple[str | None, str | None]:
    s = (raw or "").strip()
    if not s:
        return None, None
    if len(s) > 8000:
        return None, "Die RSS-URL ist zu lang."
    p = urlparse(s)
    if p.scheme not in ("http", "https") or not p.netloc:
        return None, "RSS-URL muss eine gültige http(s)-Adresse sein."
    return s, None


@router.get("/admin", include_in_schema=False)
def superadmin_root():
    return RedirectResponse("/admin/nutzer", status_code=302)


@router.get("/admin/ortsverbaende", response_class=HTMLResponse)
def superadmin_ov_list(
    request: Request,
    db: Annotated[Session, Depends(get_platform_db)],
    _: LetzgoSuperadmin,
):
    ovs = db.query(Ortsverband).order_by(Ortsverband.slug.asc()).all()
    flash_ok = request.query_params.get("geloescht") == "1"
    flash_warn = request.query_params.get("ordner_warnung")
    return templates.TemplateResponse(
        request,
        "superadmin_ovs.html",
        {"ovs": ovs, "flash_ok": flash_ok, "flash_warn": flash_warn},
    )


@router.get("/admin/ortsverbaende/neu", response_class=HTMLResponse)
def superadmin_ov_new_form(
    request: Request,
    _: LetzgoSuperadmin,
):
    return templates.TemplateResponse(
        request,
        "superadmin_ov_form.html",
        {
            "error": None,
            "ov": None,
            "is_new": True,
            "rss_feed_url_input": None,
            "rss_flash_created": None,
            "rss_flash_err": None,
        },
    )


@router.post("/admin/ortsverbaende/neu", response_class=HTMLResponse)
def superadmin_ov_new_submit(
    request: Request,
    db: Annotated[Session, Depends(get_platform_db)],
    _: LetzgoSuperadmin,
    slug: Annotated[str, Form()],
    display_name: Annotated[str, Form()],
):
    err = validate_ov_slug(slug)
    if err:
        return templates.TemplateResponse(
            request,
            "superadmin_ov_form.html",
            {
                "error": err,
                "ov": None,
                "is_new": True,
                "rss_feed_url_input": None,
                "rss_flash_created": None,
                "rss_flash_err": None,
            },
            status_code=400,
        )
    s = slug.strip().lower()
    if db.get(Ortsverband, s):
        return templates.TemplateResponse(
            request,
            "superadmin_ov_form.html",
            {
                "error": "Dieser Slug existiert bereits.",
                "ov": None,
                "is_new": True,
                "rss_feed_url_input": None,
                "rss_flash_created": None,
                "rss_flash_err": None,
            },
            status_code=400,
        )
    register_ortsverband(db, s, display_name)
    return RedirectResponse("/admin/ortsverbaende", status_code=302)


@router.get("/admin/ortsverbaende/{slug}/bearbeiten", response_class=HTMLResponse)
def superadmin_ov_edit_form(
    slug: str,
    request: Request,
    db: Annotated[Session, Depends(get_platform_db)],
    _: LetzgoSuperadmin,
):
    ov = db.get(Ortsverband, slug.strip().lower())
    if not ov:
        raise HTTPException(status_code=404, detail="Unbekannt")
    rss_created_raw = request.query_params.get("rss_import_created")
    rss_flash_created: int | None = None
    if rss_created_raw is not None and rss_created_raw.isdigit():
        rss_flash_created = int(rss_created_raw)
    rss_flash_err = request.query_params.get("rss_import_err") or None
    return templates.TemplateResponse(
        request,
        "superadmin_ov_form.html",
        {
            "error": None,
            "ov": ov,
            "is_new": False,
            "feature_plakate": is_mandant_feature_enabled(db, ov.slug, FEATURE_PLAKATE),
            "feature_sharepic": is_mandant_feature_enabled(db, ov.slug, FEATURE_SHAREPIC),
            "feature_fraktion": is_mandant_feature_enabled(db, ov.slug, FEATURE_FRAKTION),
            "rss_feed_url_input": None,
            "rss_flash_created": rss_flash_created,
            "rss_flash_err": rss_flash_err,
        },
    )


@router.post("/admin/ortsverbaende/{slug}/bearbeiten", response_class=HTMLResponse)
def superadmin_ov_edit_submit(
    slug: str,
    request: Request,
    db: Annotated[Session, Depends(get_platform_db)],
    _: LetzgoSuperadmin,
    display_name: Annotated[str, Form()],
    feature_plakate: Annotated[Optional[str], Form()] = None,
    feature_sharepic: Annotated[Optional[str], Form()] = None,
    feature_fraktion: Annotated[Optional[str], Form()] = None,
    fraktion_rss_feed_url: Annotated[str, Form()] = "",
):
    ov = db.get(Ortsverband, slug.strip().lower())
    if not ov:
        raise HTTPException(status_code=404, detail="Unbekannt")
    feed_u, feed_err = _validate_optional_fraktion_rss_url(fraktion_rss_feed_url)
    if feed_err:
        return templates.TemplateResponse(
            request,
            "superadmin_ov_form.html",
            {
                "error": feed_err,
                "ov": ov,
                "is_new": False,
                "feature_plakate": is_mandant_feature_enabled(db, ov.slug, FEATURE_PLAKATE),
                "feature_sharepic": is_mandant_feature_enabled(db, ov.slug, FEATURE_SHAREPIC),
                "feature_fraktion": is_mandant_feature_enabled(db, ov.slug, FEATURE_FRAKTION),
                "rss_feed_url_input": fraktion_rss_feed_url.strip(),
                "rss_flash_created": None,
                "rss_flash_err": None,
            },
            status_code=400,
        )
    ov.display_name = " ".join(display_name.split()).strip() or ov.slug
    ov.fraktion_rss_feed_url = feed_u
    ms = ov.slug.strip().lower()
    merge_mandant_feature(db, ms, FEATURE_PLAKATE, feature_plakate == "1")
    merge_mandant_feature(db, ms, FEATURE_SHAREPIC, feature_sharepic == "1")
    merge_mandant_feature(db, ms, FEATURE_FRAKTION, feature_fraktion == "1")
    db.add(ov)
    db.commit()
    return RedirectResponse("/admin/ortsverbaende", status_code=302)


@router.post("/admin/ortsverbaende/{slug}/fraktion-rss-sync")
async def superadmin_ov_fraktion_rss_sync(
    request: Request,
    slug: str,
    db: Annotated[Session, Depends(get_platform_db)],
    _: LetzgoSuperadmin,
):
    s = slug.strip().lower()
    ov = db.get(Ortsverband, s)
    if not ov:
        raise HTTPException(status_code=404, detail="Unbekannt")
    form = await request.form()
    raw_feed = form.get("fraktion_rss_feed_url")
    if isinstance(raw_feed, str):
        draft = raw_feed.strip()
        if draft:
            feed_u, feed_err = _validate_optional_fraktion_rss_url(raw_feed)
            if feed_err:
                return RedirectResponse(
                    f"/admin/ortsverbaende/{s}/bearbeiten?rss_import_err={quote(feed_err)}",
                    status_code=302,
                )
            feed_url = feed_u or ""
        else:
            feed_url = ""
    else:
        feed_url = ov.fraktion_rss_feed_url or ""

    n, err = import_fraktion_termine_from_feed(db, ov.slug, feed_url)
    base = f"/admin/ortsverbaende/{s}/bearbeiten"
    if err:
        return RedirectResponse(f"{base}?rss_import_err={quote(err)}", status_code=302)
    return RedirectResponse(f"{base}?rss_import_created={n}", status_code=302)


@router.get("/admin/ortsverbaende/{slug}/loeschen", response_class=HTMLResponse)
def superadmin_ov_delete_form(
    slug: str,
    request: Request,
    db: Annotated[Session, Depends(get_platform_db)],
    _: LetzgoSuperadmin,
):
    s = slug.strip().lower()
    ov = db.get(Ortsverband, s)
    if not ov:
        raise HTTPException(status_code=404, detail="Unbekannt")
    warn_public_site = bool(PUBLIC_SITE_MANDANT_SLUG and s == PUBLIC_SITE_MANDANT_SLUG)
    return templates.TemplateResponse(
        request,
        "superadmin_ov_loeschen.html",
        {"ov": ov, "error": None, "warn_public_site": warn_public_site},
    )


@router.post("/admin/ortsverbaende/{slug}/loeschen")
def superadmin_ov_delete_submit(
    slug: str,
    request: Request,
    db: Annotated[Session, Depends(get_platform_db)],
    _: LetzgoSuperadmin,
    confirm_slug: Annotated[str, Form()],
):
    s = slug.strip().lower()
    ov = db.get(Ortsverband, s)
    if not ov:
        raise HTTPException(status_code=404, detail="Unbekannt")
    warn_public_site = bool(PUBLIC_SITE_MANDANT_SLUG and s == PUBLIC_SITE_MANDANT_SLUG)
    if confirm_slug.strip().lower() != s:
        return templates.TemplateResponse(
            request,
            "superadmin_ov_loeschen.html",
            {
                "ov": ov,
                "error": f"Zur Bestätigung bitte exakt den Slug „{s}“ eingeben.",
                "warn_public_site": warn_public_site,
            },
            status_code=400,
        )
    try:
        delete_ortsverband_completely(db, s)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except (OSError, RuntimeError) as e:
        q = quote(str(e), safe="")
        return RedirectResponse(
            f"/admin/ortsverbaende?ordner_warnung={q}",
            status_code=302,
        )
    return RedirectResponse("/admin/ortsverbaende?geloescht=1", status_code=302)


@router.get("/admin/nutzer", response_class=HTMLResponse)
def superadmin_user_list(
    request: Request,
    db: Annotated[Session, Depends(get_platform_db)],
    _: LetzgoSuperadmin,
):
    users = (
        db.query(PlatformUser)
        .options(selectinload(PlatformUser.memberships))
        .order_by(func.lower(PlatformUser.username))
        .all()
    )
    rows = []
    for u in users:
        mems = sorted(u.memberships, key=lambda m: m.ov_slug.lower())
        rows.append(
            {
                "user": u,
                "platform_superadmin": is_superadmin_username(u.username),
                "memberships": mems,
            }
        )
    flash_del = request.query_params.get("geloescht") == "1"
    return templates.TemplateResponse(
        request,
        "superadmin_users.html",
        {"rows": rows, "flash_geloescht": flash_del},
    )


@router.get("/admin/nutzer/{user_id}/bearbeiten", response_class=HTMLResponse)
def superadmin_user_edit_form(
    user_id: int,
    request: Request,
    db: Annotated[Session, Depends(get_platform_db)],
    _: LetzgoSuperadmin,
):
    pu = (
        db.query(PlatformUser)
        .options(selectinload(PlatformUser.memberships))
        .filter(PlatformUser.id == user_id)
        .first()
    )
    if not pu:
        raise HTTPException(status_code=404, detail="Nutzer nicht gefunden")
    ovs = db.query(Ortsverband).order_by(Ortsverband.slug.asc()).all()
    mem_by_slug = {m.ov_slug.strip().lower(): m for m in pu.memberships}
    flash_ok = request.query_params.get("gespeichert") == "1"
    return templates.TemplateResponse(
        request,
        "superadmin_user_form.html",
        _superadmin_user_form_template_ctx(
            request,
            pu,
            ovs,
            mem_by_slug,
            error=None,
            flash_ok=flash_ok,
        ),
    )


@router.post("/admin/nutzer/{user_id}/bearbeiten", response_class=HTMLResponse)
def superadmin_user_edit_submit(
    user_id: int,
    request: Request,
    db: Annotated[Session, Depends(get_platform_db)],
    _: LetzgoSuperadmin,
    display_name: Annotated[str, Form()],
    password_new: Annotated[str, Form()] = "",
    password_new2: Annotated[str, Form()] = "",
    ov_member: Annotated[Optional[List[str]], Form()] = None,
    ov_admin: Annotated[Optional[List[str]], Form()] = None,
    ov_fraktion: Annotated[Optional[List[str]], Form()] = None,
):
    pu = db.get(PlatformUser, user_id)
    if not pu:
        raise HTTPException(status_code=404, detail="Nutzer nicht gefunden")
    ovs = db.query(Ortsverband).order_by(Ortsverband.slug.asc()).all()
    mem_by_slug = {m.ov_slug.strip().lower(): m for m in pu.memberships}

    dn = " ".join(display_name.split()).strip()
    if len(dn) > 120:
        return templates.TemplateResponse(
            request,
            "superadmin_user_form.html",
            _superadmin_user_form_template_ctx(
                request,
                pu,
                ovs,
                mem_by_slug,
                error="Anzeigename darf höchstens 120 Zeichen haben.",
                flash_ok=False,
            ),
            status_code=400,
        )

    pw1 = (password_new or "").strip()
    pw2 = (password_new2 or "").strip()
    if pw1 or pw2:
        if not pw1 or not pw2:
            return templates.TemplateResponse(
                request,
                "superadmin_user_form.html",
                _superadmin_user_form_template_ctx(
                    request,
                    pu,
                    ovs,
                    mem_by_slug,
                    error="Neues Passwort bitte zweimal eingeben.",
                    flash_ok=False,
                ),
                status_code=400,
            )
        if len(pw1) < PASSWORD_MIN_SUPERADMIN:
            err = f"Neues Passwort mindestens {PASSWORD_MIN_SUPERADMIN} Zeichen."
            return templates.TemplateResponse(
                request,
                "superadmin_user_form.html",
                _superadmin_user_form_template_ctx(
                    request,
                    pu,
                    ovs,
                    mem_by_slug,
                    error=err,
                    flash_ok=False,
                ),
                status_code=400,
            )
        if pw1 != pw2:
            return templates.TemplateResponse(
                request,
                "superadmin_user_form.html",
                _superadmin_user_form_template_ctx(
                    request,
                    pu,
                    ovs,
                    mem_by_slug,
                    error="Die beiden Passwortfelder stimmen nicht überein.",
                    flash_ok=False,
                ),
                status_code=400,
            )

    pu.display_name = dn
    if pw1:
        pu.password_hash = hash_password(pw1)

    members = _form_ov_slug_list(ov_member)
    admins_raw = _form_ov_slug_list(ov_admin)
    fraktion_raw = _form_ov_slug_list(ov_fraktion)
    admin_set = set(admins_raw)
    fraktion_set = set(fraktion_raw)
    _sync_ov_memberships_superadmin(db, pu.id, members, admin_set, fraktion_set)

    db.add(pu)
    db.commit()
    return RedirectResponse(f"/admin/nutzer/{user_id}/bearbeiten?gespeichert=1", status_code=302)


@router.get("/admin/nutzer/{user_id}/loeschen", response_class=HTMLResponse)
def superadmin_user_delete_form(
    user_id: int,
    request: Request,
    db: Annotated[Session, Depends(get_platform_db)],
    _: LetzgoSuperadmin,
):
    pu = db.get(PlatformUser, user_id)
    if not pu:
        raise HTTPException(status_code=404, detail="Nutzer nicht gefunden")
    blocked = _superadmin_user_delete_blocked(request, db, pu, user_id)
    return templates.TemplateResponse(
        request,
        "superadmin_user_loeschen.html",
        {
            "del_user": pu,
            "blocked": blocked,
            "error": None,
        },
    )


@router.post("/admin/nutzer/{user_id}/loeschen")
def superadmin_user_delete_submit(
    user_id: int,
    request: Request,
    db: Annotated[Session, Depends(get_platform_db)],
    _: LetzgoSuperadmin,
    confirm_username: Annotated[str, Form()] = "",
):
    pu = db.get(PlatformUser, user_id)
    if not pu:
        raise HTTPException(status_code=404, detail="Nutzer nicht gefunden")
    blocked = _superadmin_user_delete_blocked(request, db, pu, user_id)
    if blocked:
        return templates.TemplateResponse(
            request,
            "superadmin_user_loeschen.html",
            {"del_user": pu, "blocked": blocked, "error": None},
            status_code=400,
        )
    expected = pu.username.strip().lower()
    got = (confirm_username or "").strip().lower()
    if got != expected:
        return templates.TemplateResponse(
            request,
            "superadmin_user_loeschen.html",
            {
                "del_user": pu,
                "blocked": None,
                "error": f"Zur Bestätigung bitte exakt den Benutzernamen „{pu.username}“ eingeben.",
            },
            status_code=400,
        )
    try:
        _purge_dependencies_before_platform_user_delete(db, user_id)
        db.delete(pu)
        db.commit()
    except IntegrityError:
        db.rollback()
        return templates.TemplateResponse(
            request,
            "superadmin_user_loeschen.html",
            {
                "del_user": pu,
                "blocked": None,
                "error": (
                    "Der Nutzer konnte nicht gelöscht werden (Datenbank-Einschränkung). "
                    "Es verweisen vermutlich noch Einträge auf dieses Konto."
                ),
            },
            status_code=409,
        )
    return RedirectResponse("/admin/nutzer?geloescht=1", status_code=302)
