from __future__ import annotations

import re
from typing import Annotated

from fastapi import Depends, HTTPException, Request, status
from sqlalchemy.orm import Session

from app.config import DEFAULT_MANDANT_SLUG, is_superadmin_username
from app.platform_database import get_platform_db
from app.platform_models import OvMembership, PlatformUser


def _effective_mandant_slug(request: Request) -> str:
    slug = request.path_params.get("mandant_slug")
    if slug:
        return slug.strip().lower()
    ms = request.session.get("mandant_slug")
    if ms:
        return str(ms).strip().lower()
    return DEFAULT_MANDANT_SLUG


_TERMINE_CROSS_OV_PATH_RES = (
    re.compile(r"^/termine/\d+$"),
    re.compile(r"^/termine/\d+/(teilnehmen|absagen|abmelden)$"),
    re.compile(r"^/termine/\d+/kommentare(/\d+)?$"),
    re.compile(r"^/fraktion/termine/\d+$"),
    re.compile(r"^/fraktion/termine/\d+/(teilnehmen|absagen|abmelden)$"),
    re.compile(r"^/fraktion/termine/\d+/kommentare(/\d+)?$"),
)


def _relative_path_under_mandant(request: Request) -> str | None:
    """Pfad relativ zu `/m/<slug>/`, z. B. `/termine/12/teilnehmen`."""
    path = request.scope.get("path") or ""
    rp = (request.scope.get("root_path") or "").rstrip("/")
    if rp and path.startswith(rp):
        path = path[len(rp) :] or "/"
    parts = [p for p in path.strip("/").split("/") if p]
    if len(parts) < 3 or parts[0] != "m":
        return None
    rest = parts[2:]
    if not rest:
        return "/"
    return "/" + "/".join(rest)


def _allow_logged_user_without_this_ov_membership(request: Request) -> bool:
    """Ein OV-Link darf Kreistermine (teilnehmen, Detail …) ohne Mitgliedschaft in DIESEM Slug."""
    rel = _relative_path_under_mandant(request)
    if rel is None:
        return False
    return any(p.match(rel) for p in _TERMINE_CROSS_OV_PATH_RES)


class AuthenticatedUser:
    """Angemeldeter Nutzer im Kontext eines Mandanten (Slug aus Pfad oder Session)."""

    __slots__ = ("platform_user", "membership", "mandant_slug", "_super")

    def __init__(
        self,
        platform_user: PlatformUser,
        mandant_slug: str,
        membership: OvMembership | None,
    ) -> None:
        self.platform_user = platform_user
        self.mandant_slug = mandant_slug.strip().lower()
        self.membership = membership
        self._super = is_superadmin_username(platform_user.username)

    @property
    def id(self) -> int:
        return self.platform_user.id

    @property
    def username(self) -> str:
        return self.platform_user.username

    @property
    def display_name(self) -> str:
        return self.platform_user.display_name

    @property
    def calendar_token(self) -> str | None:
        return self.platform_user.calendar_token

    @property
    def is_admin(self) -> bool:
        if self._super:
            return True
        return bool(self.membership and self.membership.is_admin)

    def membership_required_ok(self) -> bool:
        if self._super:
            return True
        return bool(self.membership and self.membership.is_approved)


def get_current_user(
    request: Request,
    pdb: Annotated[Session, Depends(get_platform_db)],
) -> AuthenticatedUser:
    uid = request.session.get("user_id")
    if not uid:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Nicht angemeldet",
        )
    pu = pdb.get(PlatformUser, int(uid))
    if not pu:
        request.session.clear()
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Sitzung ungültig",
        )
    slug = _effective_mandant_slug(request)
    membership: OvMembership | None = None
    if not is_superadmin_username(pu.username):
        membership = (
            pdb.query(OvMembership)
            .filter(
                OvMembership.user_id == pu.id,
                OvMembership.ov_slug == slug,
            )
            .first()
        )
        if membership is None:
            any_approved = (
                pdb.query(OvMembership)
                .filter(
                    OvMembership.user_id == pu.id,
                    OvMembership.is_approved.is_(True),
                )
                .first()
            )
            if any_approved is None:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=(
                        "Kein Zugang unter diesem Ortsverbands-Link. "
                        "Bitte die App über deinen Ortsverband öffnen."
                    ),
                )
            if _allow_logged_user_without_this_ov_membership(request):
                return AuthenticatedUser(pu, slug, None)
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=(
                    "Kein Zugang unter diesem Ortsverbands-Link. "
                    "Bitte die App über deinen Ortsverband öffnen."
                ),
            )
        if not membership.is_approved:
            request.session.clear()
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Konto noch nicht freigegeben.",
            )
    return AuthenticatedUser(pu, slug, membership)


CurrentUser = Annotated[AuthenticatedUser, Depends(get_current_user)]


def get_admin_user(user: CurrentUser) -> AuthenticatedUser:
    if not user.is_admin:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Nur für Administratoren.",
        )
    return user


AdminUser = Annotated[AuthenticatedUser, Depends(get_admin_user)]


def require_superadmin_platform(user: CurrentUser) -> AuthenticatedUser:
    if not is_superadmin_username(user.username):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Nur für Superadmins.",
        )
    return user


LetzgoSuperadmin = Annotated[AuthenticatedUser, Depends(require_superadmin_platform)]
