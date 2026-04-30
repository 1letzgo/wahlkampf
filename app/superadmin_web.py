from __future__ import annotations

from typing import Annotated, List, Optional
from urllib.parse import quote

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy import func
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
from app.mandant_features import (
    FEATURE_PLAKATE,
    FEATURE_SHAREPIC,
    is_mandant_feature_enabled,
    merge_mandant_feature,
)
from app.platform_database import get_platform_db
from app.platform_models import Ortsverband, OvMembership, PlatformUser

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


def _sync_ov_memberships_superadmin(
    db: Session,
    user_id: int,
    member_slugs: List[str],
    admin_slugs: set[str],
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

    rows = db.query(OvMembership).filter(OvMembership.user_id == user_id).all()
    by_slug = {m.ov_slug.strip().lower(): m for m in rows}
    for slug in member_set:
        m = by_slug.pop(slug, None)
        if m:
            m.is_approved = True
            m.is_admin = slug in admin_set
            db.add(m)
        else:
            db.add(
                OvMembership(
                    user_id=user_id,
                    ov_slug=slug,
                    is_admin=slug in admin_set,
                    is_approved=True,
                )
            )
    for m in by_slug.values():
        db.delete(m)


@router.get("/admin", include_in_schema=False)
def superadmin_root():
    return RedirectResponse("/admin/ortsverbaende", status_code=302)


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
        {"error": None, "ov": None, "is_new": True},
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
            {"error": err, "ov": None, "is_new": True},
            status_code=400,
        )
    s = slug.strip().lower()
    if db.get(Ortsverband, s):
        return templates.TemplateResponse(
            request,
            "superadmin_ov_form.html",
            {"error": "Dieser Slug existiert bereits.", "ov": None, "is_new": True},
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
    return templates.TemplateResponse(
        request,
        "superadmin_ov_form.html",
        {
            "error": None,
            "ov": ov,
            "is_new": False,
            "feature_plakate": is_mandant_feature_enabled(db, ov.slug, FEATURE_PLAKATE),
            "feature_sharepic": is_mandant_feature_enabled(db, ov.slug, FEATURE_SHAREPIC),
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
):
    ov = db.get(Ortsverband, slug.strip().lower())
    if not ov:
        raise HTTPException(status_code=404, detail="Unbekannt")
    ov.display_name = " ".join(display_name.split()).strip() or ov.slug
    ms = ov.slug.strip().lower()
    merge_mandant_feature(db, ms, FEATURE_PLAKATE, feature_plakate == "1")
    merge_mandant_feature(db, ms, FEATURE_SHAREPIC, feature_sharepic == "1")
    db.add(ov)
    db.commit()
    return RedirectResponse("/admin/ortsverbaende", status_code=302)


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
    return templates.TemplateResponse(
        request,
        "superadmin_users.html",
        {"rows": rows},
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
        {
            "edit_user": pu,
            "ovs": ovs,
            "mem_by_slug": mem_by_slug,
            "error": None,
            "platform_superadmin": is_superadmin_username(pu.username),
            "flash_ok": flash_ok,
        },
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
            {
                "edit_user": pu,
                "ovs": ovs,
                "mem_by_slug": mem_by_slug,
                "error": "Anzeigename darf höchstens 120 Zeichen haben.",
                "platform_superadmin": is_superadmin_username(pu.username),
                "flash_ok": False,
            },
            status_code=400,
        )

    pw1 = (password_new or "").strip()
    pw2 = (password_new2 or "").strip()
    if pw1 or pw2:
        if not pw1 or not pw2:
            return templates.TemplateResponse(
                request,
                "superadmin_user_form.html",
                {
                    "edit_user": pu,
                    "ovs": ovs,
                    "mem_by_slug": mem_by_slug,
                    "error": "Neues Passwort bitte zweimal eingeben.",
                    "platform_superadmin": is_superadmin_username(pu.username),
                    "flash_ok": False,
                },
                status_code=400,
            )
        if len(pw1) < PASSWORD_MIN_SUPERADMIN:
            err = f"Neues Passwort mindestens {PASSWORD_MIN_SUPERADMIN} Zeichen."
            return templates.TemplateResponse(
                request,
                "superadmin_user_form.html",
                {
                    "edit_user": pu,
                    "ovs": ovs,
                    "mem_by_slug": mem_by_slug,
                    "error": err,
                    "platform_superadmin": is_superadmin_username(pu.username),
                    "flash_ok": False,
                },
                status_code=400,
            )
        if pw1 != pw2:
            return templates.TemplateResponse(
                request,
                "superadmin_user_form.html",
                {
                    "edit_user": pu,
                    "ovs": ovs,
                    "mem_by_slug": mem_by_slug,
                    "error": "Die beiden Passwortfelder stimmen nicht überein.",
                    "platform_superadmin": is_superadmin_username(pu.username),
                    "flash_ok": False,
                },
                status_code=400,
            )

    pu.display_name = dn
    if pw1:
        pu.password_hash = hash_password(pw1)

    members = _form_ov_slug_list(ov_member)
    admins_raw = _form_ov_slug_list(ov_admin)
    admin_set = set(admins_raw)
    _sync_ov_memberships_superadmin(db, pu.id, members, admin_set)

    db.add(pu)
    db.commit()
    return RedirectResponse(f"/admin/nutzer/{user_id}/bearbeiten?gespeichert=1", status_code=302)
