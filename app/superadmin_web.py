from __future__ import annotations

from typing import Annotated, List, Optional
from urllib.parse import quote

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy import delete, func, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, selectinload
from starlette.templating import Jinja2Templates

from app.deps import LetzgoSuperadmin
from app.auth import hash_password
from app.config import MAX_UPLOAD_MB, PUBLIC_SITE_MANDANT_SLUG, is_superadmin_username
from app.ov_services import (
    delete_ortsverband_completely,
    register_ortsverband,
    validate_ov_slug,
)
from app.cal_fraktion_import import (
    import_fraktion_termine_from_calendar,
    validate_and_normalize_cal_subscription_url,
)
from app.mandant_features import (
    FEATURE_PLAKATE,
    FEATURE_SHAREPIC,
    is_mandant_feature_enabled,
    merge_mandant_feature,
)
from app.termin_kategorie import normalize_termin_kategorie
from app.platform_user_admin import (
    PASSWORD_MIN_PLATFORM_USER,
    form_ov_slug_list as _form_ov_slug_list,
    superadmin_user_form_template_ctx as _superadmin_user_form_template_ctx,
    sync_ov_memberships_superadmin,
)
from app.platform_database import get_platform_db
from app.platform_models import (
    ExternCalSubscription,
    MandantPlakat,
    Ortsverband,
    OvMembership,
    PlatformUser,
    Termin,
    TerminKommentar,
    TerminTeilnahme,
)
from app.settings_store import save_sharepic_slogan_default, sharepic_slogan_default_value

TEMPLATES_DIR = __import__("pathlib").Path(__file__).resolve().parent / "templates"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

router = APIRouter(tags=["superadmin"])


def _ov_edit_form_ctx(
    request: Request,
    db: Session,
    ov: Ortsverband,
    *,
    error: str | None = None,
    sharepic_slogan_input: str | None = None,
    flash_ov_gespeichert: bool = False,
) -> dict:
    slogan_default = (
        sharepic_slogan_input
        if sharepic_slogan_input is not None
        else sharepic_slogan_default_value(db, ov.slug, ov.display_name or ov.slug)
    )
    return {
        "error": error,
        "ov": ov,
        "is_new": False,
        "feature_plakate": is_mandant_feature_enabled(db, ov.slug, FEATURE_PLAKATE),
        "feature_sharepic": is_mandant_feature_enabled(db, ov.slug, FEATURE_SHAREPIC),
        "sharepic_slogan_default": slogan_default,
        "flash_ov_gespeichert": flash_ov_gespeichert,
        "max_upload_mb": MAX_UPLOAD_MB,
    }


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
            "flash_ov_gespeichert": False,
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
                "flash_ov_gespeichert": False,
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
                "flash_ov_gespeichert": False,
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
    flash_ov_gespeichert = request.query_params.get("gespeichert") == "1"
    return templates.TemplateResponse(
        request,
        "superadmin_ov_form.html",
        _ov_edit_form_ctx(
            request,
            db,
            ov,
            error=None,
            flash_ov_gespeichert=flash_ov_gespeichert,
        ),
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
    sharepic_slogan_default: Annotated[Optional[str], Form()] = None,
):
    ov = db.get(Ortsverband, slug.strip().lower())
    if not ov:
        raise HTTPException(status_code=404, detail="Unbekannt")
    ov.display_name = " ".join(display_name.split()).strip() or ov.slug
    ms = ov.slug.strip().lower()
    merge_mandant_feature(db, ms, FEATURE_PLAKATE, feature_plakate == "1")
    merge_mandant_feature(db, ms, FEATURE_SHAREPIC, feature_sharepic == "1")
    if sharepic_slogan_default is not None:
        save_sharepic_slogan_default(db, ms, sharepic_slogan_default)
    db.add(ov)
    db.commit()
    return RedirectResponse(
        f"/admin/ortsverbaende/{ms}/bearbeiten?gespeichert=1",
        status_code=302,
    )


@router.get("/admin/kalender-abos", response_class=HTMLResponse)
def superadmin_cal_sub_list(
    request: Request,
    db: Annotated[Session, Depends(get_platform_db)],
    _: LetzgoSuperadmin,
):
    subs = (
        db.query(ExternCalSubscription)
        .order_by(ExternCalSubscription.mandant_slug.asc(), ExternCalSubscription.id.asc())
        .all()
    )
    ovs = {o.slug: o for o in db.query(Ortsverband).all()}
    return templates.TemplateResponse(
        request,
        "superadmin_cal_subscriptions.html",
        {"subs": subs, "ovs": ovs},
    )


@router.get("/admin/kalender-abos/neu", response_class=HTMLResponse)
def superadmin_cal_sub_new_form(
    request: Request,
    db: Annotated[Session, Depends(get_platform_db)],
    _: LetzgoSuperadmin,
):
    ovs = db.query(Ortsverband).order_by(Ortsverband.slug.asc()).all()
    return templates.TemplateResponse(
        request,
        "superadmin_cal_subscription_form.html",
        _cal_sub_form_ctx(
            sub=None,
            ovs=ovs,
            error=None,
            feed_url_input=None,
            flash_ok=False,
            cal_flash_created=None,
            cal_flash_err=None,
            termin_kategorie_override=None,
        ),
    )


@router.post("/admin/kalender-abos/neu", response_class=HTMLResponse)
def superadmin_cal_sub_new_submit(
    request: Request,
    db: Annotated[Session, Depends(get_platform_db)],
    _: LetzgoSuperadmin,
    mandant_slug: Annotated[str, Form()],
    label: Annotated[str, Form()] = "",
    feed_url: Annotated[str, Form()] = "",
    termin_kategorie: Annotated[str, Form()] = "verband",
    abo_active: Annotated[Optional[str], Form()] = None,
):
    ms = mandant_slug.strip().lower()
    ov = db.get(Ortsverband, ms)
    ovs = db.query(Ortsverband).order_by(Ortsverband.slug.asc()).all()
    tk = normalize_termin_kategorie(termin_kategorie)
    if not ov:
        return templates.TemplateResponse(
            request,
            "superadmin_cal_subscription_form.html",
            _cal_sub_form_ctx(
                sub=None,
                ovs=ovs,
                error="Ortsverband nicht gefunden.",
                feed_url_input=feed_url,
                flash_ok=False,
                cal_flash_created=None,
                cal_flash_err=None,
                termin_kategorie_override=tk,
            ),
            status_code=400,
        )
    feed_u, feed_err = validate_and_normalize_cal_subscription_url(feed_url)
    if feed_err:
        return templates.TemplateResponse(
            request,
            "superadmin_cal_subscription_form.html",
            _cal_sub_form_ctx(
                sub=None,
                ovs=ovs,
                error=feed_err,
                feed_url_input=feed_url.strip(),
                flash_ok=False,
                cal_flash_created=None,
                cal_flash_err=None,
                termin_kategorie_override=tk,
            ),
            status_code=400,
        )
    want_active = abo_active == "1"
    if want_active and not (feed_u or "").strip():
        return templates.TemplateResponse(
            request,
            "superadmin_cal_subscription_form.html",
            _cal_sub_form_ctx(
                sub=None,
                ovs=ovs,
                error="Für ein aktives Abo ist eine Feed-URL erforderlich.",
                feed_url_input=feed_url.strip(),
                flash_ok=False,
                cal_flash_created=None,
                cal_flash_err=None,
                termin_kategorie_override=tk,
            ),
            status_code=400,
        )
    sub = ExternCalSubscription(
        mandant_slug=ms,
        label=" ".join(label.split()).strip()[:200],
        feed_url=feed_u,
        abo_active=want_active,
        termin_kategorie=tk,
    )
    db.add(sub)
    db.commit()
    db.refresh(sub)
    return RedirectResponse(
        f"/admin/kalender-abos/{sub.id}/bearbeiten?gespeichert=1",
        status_code=302,
    )


def _cal_sub_form_ctx(
    *,
    sub: ExternCalSubscription | None,
    ovs: list,
    error: str | None,
    feed_url_input: str | None,
    flash_ok: bool,
    cal_flash_created: int | None,
    cal_flash_err: str | None,
    termin_kategorie_override: str | None = None,
) -> dict:
    if termin_kategorie_override is not None:
        tk_sel = normalize_termin_kategorie(termin_kategorie_override)
    elif sub is not None:
        tk_sel = normalize_termin_kategorie(getattr(sub, "termin_kategorie", None))
    else:
        tk_sel = normalize_termin_kategorie("verband")
    return {
        "sub": sub,
        "ovs": ovs,
        "error": error,
        "feed_url_input": feed_url_input,
        "flash_ok": flash_ok,
        "cal_flash_created": cal_flash_created,
        "cal_flash_err": cal_flash_err,
        "termin_kategorie_selected": tk_sel,
    }


@router.get("/admin/kalender-abos/{sub_id}/bearbeiten", response_class=HTMLResponse)
def superadmin_cal_sub_edit_form(
    sub_id: int,
    request: Request,
    db: Annotated[Session, Depends(get_platform_db)],
    _: LetzgoSuperadmin,
):
    sub = db.get(ExternCalSubscription, sub_id)
    if not sub:
        raise HTTPException(status_code=404, detail="Abo nicht gefunden")
    ovs = db.query(Ortsverband).order_by(Ortsverband.slug.asc()).all()
    flash_ok = request.query_params.get("gespeichert") == "1"
    cal_created_raw = request.query_params.get("cal_import_created")
    cal_flash_created: int | None = None
    if cal_created_raw is not None and cal_created_raw.isdigit():
        cal_flash_created = int(cal_created_raw)
    cal_flash_err = request.query_params.get("cal_import_err") or None
    return templates.TemplateResponse(
        request,
        "superadmin_cal_subscription_form.html",
        _cal_sub_form_ctx(
            sub=sub,
            ovs=ovs,
            error=None,
            feed_url_input=None,
            flash_ok=flash_ok,
            cal_flash_created=cal_flash_created,
            cal_flash_err=cal_flash_err,
            termin_kategorie_override=None,
        ),
    )


@router.post("/admin/kalender-abos/{sub_id}/bearbeiten", response_class=HTMLResponse)
def superadmin_cal_sub_edit_submit(
    sub_id: int,
    request: Request,
    db: Annotated[Session, Depends(get_platform_db)],
    _: LetzgoSuperadmin,
    mandant_slug: Annotated[str, Form()],
    label: Annotated[str, Form()] = "",
    feed_url: Annotated[str, Form()] = "",
    termin_kategorie: Annotated[str, Form()] = "verband",
    abo_active: Annotated[Optional[str], Form()] = None,
):
    sub = db.get(ExternCalSubscription, sub_id)
    if not sub:
        raise HTTPException(status_code=404, detail="Abo nicht gefunden")
    ovs = db.query(Ortsverband).order_by(Ortsverband.slug.asc()).all()
    tk = normalize_termin_kategorie(termin_kategorie)
    ms = mandant_slug.strip().lower()
    ov = db.get(Ortsverband, ms)
    if not ov:
        return templates.TemplateResponse(
            request,
            "superadmin_cal_subscription_form.html",
            _cal_sub_form_ctx(
                sub=sub,
                ovs=ovs,
                error="Ortsverband nicht gefunden.",
                feed_url_input=feed_url,
                flash_ok=False,
                cal_flash_created=None,
                cal_flash_err=None,
                termin_kategorie_override=tk,
            ),
            status_code=400,
        )
    feed_u, feed_err = validate_and_normalize_cal_subscription_url(feed_url)
    if feed_err:
        return templates.TemplateResponse(
            request,
            "superadmin_cal_subscription_form.html",
            _cal_sub_form_ctx(
                sub=sub,
                ovs=ovs,
                error=feed_err,
                feed_url_input=feed_url.strip(),
                flash_ok=False,
                cal_flash_created=None,
                cal_flash_err=None,
                termin_kategorie_override=tk,
            ),
            status_code=400,
        )
    want_active = abo_active == "1"
    if want_active and not (feed_u or "").strip():
        return templates.TemplateResponse(
            request,
            "superadmin_cal_subscription_form.html",
            _cal_sub_form_ctx(
                sub=sub,
                ovs=ovs,
                error="Für ein aktives Abo ist eine Feed-URL erforderlich.",
                feed_url_input=feed_url.strip(),
                flash_ok=False,
                cal_flash_created=None,
                cal_flash_err=None,
                termin_kategorie_override=tk,
            ),
            status_code=400,
        )
    sub.mandant_slug = ms
    sub.label = " ".join(label.split()).strip()[:200]
    sub.feed_url = feed_u
    sub.abo_active = want_active
    sub.termin_kategorie = tk
    db.add(sub)
    db.commit()
    return RedirectResponse(
        f"/admin/kalender-abos/{sub_id}/bearbeiten?gespeichert=1",
        status_code=302,
    )


@router.post("/admin/kalender-abos/{sub_id}/sync")
def superadmin_cal_sub_sync_now(
    sub_id: int,
    db: Annotated[Session, Depends(get_platform_db)],
    _: LetzgoSuperadmin,
):
    sub = db.get(ExternCalSubscription, sub_id)
    if not sub:
        raise HTTPException(status_code=404, detail="Abo nicht gefunden")
    url = (sub.feed_url or "").strip()
    if not url:
        return RedirectResponse(
            f"/admin/kalender-abos/{sub_id}/bearbeiten?cal_import_err={quote('Keine Feed-URL gespeichert.')}",
            status_code=302,
        )
    n, err = import_fraktion_termine_from_calendar(
        db,
        sub.mandant_slug,
        url,
        termin_kategorie=sub.termin_kategorie or "verband",
    )
    base = f"/admin/kalender-abos/{sub_id}/bearbeiten"
    if err:
        return RedirectResponse(f"{base}?cal_import_err={quote(err)}", status_code=302)
    return RedirectResponse(f"{base}?cal_import_created={n}", status_code=302)


@router.get("/admin/kalender-abos/{sub_id}/loeschen", response_class=HTMLResponse)
def superadmin_cal_sub_delete_form(
    sub_id: int,
    request: Request,
    db: Annotated[Session, Depends(get_platform_db)],
    _: LetzgoSuperadmin,
):
    sub = db.get(ExternCalSubscription, sub_id)
    if not sub:
        raise HTTPException(status_code=404, detail="Abo nicht gefunden")
    ov = db.get(Ortsverband, sub.mandant_slug)
    return templates.TemplateResponse(
        request,
        "superadmin_cal_subscription_loeschen.html",
        {"sub": sub, "ov": ov, "error": None},
    )


@router.post("/admin/kalender-abos/{sub_id}/loeschen")
def superadmin_cal_sub_delete_submit(
    sub_id: int,
    request: Request,
    db: Annotated[Session, Depends(get_platform_db)],
    _: LetzgoSuperadmin,
    confirm_id: Annotated[str, Form()],
):
    sub = db.get(ExternCalSubscription, sub_id)
    if not sub:
        raise HTTPException(status_code=404, detail="Abo nicht gefunden")
    if confirm_id.strip() != str(sub_id):
        ov = db.get(Ortsverband, sub.mandant_slug)
        return templates.TemplateResponse(
            request,
            "superadmin_cal_subscription_loeschen.html",
            {
                "sub": sub,
                "ov": ov,
                "error": "Zur Bestätigung bitte exakt die Abo-ID eingeben.",
            },
            status_code=400,
        )
    db.delete(sub)
    db.commit()
    return RedirectResponse("/admin/kalender-abos", status_code=302)


@router.post("/admin/ortsverbaende/{slug}/plakate-loeschen")
def superadmin_ov_plakate_loeschen(
    slug: str,
    db: Annotated[Session, Depends(get_platform_db)],
    _: LetzgoSuperadmin,
):
    s = slug.strip().lower()
    ov = db.get(Ortsverband, s)
    if not ov:
        raise HTTPException(status_code=404, detail="Unbekannt")
    db.query(MandantPlakat).filter(MandantPlakat.mandant_slug == s).delete(
        synchronize_session=False,
    )
    db.commit()
    return RedirectResponse(
        f"/admin/ortsverbaende/{s}/bearbeiten?plakate=geloescht",
        status_code=302,
    )


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
    ov_vorstand: Annotated[Optional[List[str]], Form()] = None,
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
        if len(pw1) < PASSWORD_MIN_PLATFORM_USER:
            err = f"Neues Passwort mindestens {PASSWORD_MIN_PLATFORM_USER} Zeichen."
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
    vorstand_raw = _form_ov_slug_list(ov_vorstand)
    admin_set = set(admins_raw)
    fraktion_set = set(fraktion_raw)
    vorstand_set = set(vorstand_raw)
    sync_ov_memberships_superadmin(
        db, pu.id, members, admin_set, vorstand_set, fraktion_set
    )

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
