from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated

from fastapi import Depends, HTTPException, Request, status
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from app import models
from app.database import get_sessionmaker
from app.platform_database import get_platform_db
from app.platform_models import PlatformUser


@dataclass(frozen=True)
class SuperadminPrincipal:
    """Eingeloggter Superadmin: Eintrag in platform_users oder OV-Administrator."""

    platform_user_id: int | None = None
    mandant_slug: str | None = None
    mandant_user_id: int | None = None


def require_superadmin(
    request: Request,
    pdb: Annotated[Session, Depends(get_platform_db)],
) -> SuperadminPrincipal:
    pid = request.session.get("platform_admin_id")
    if pid is not None:
        try:
            pid_int = int(pid)
        except (TypeError, ValueError):
            request.session.pop("platform_admin_id", None)
        else:
            u = pdb.get(PlatformUser, pid_int)
            if u:
                return SuperadminPrincipal(platform_user_id=u.id)
            request.session.pop("platform_admin_id", None)

    slug = request.session.get("platform_superadmin_mandant_slug")
    uid = request.session.get("platform_superadmin_user_id")
    if slug and uid is not None:
        slug_s = str(slug).strip().lower()
        try:
            uid_int = int(uid)
        except (TypeError, ValueError):
            request.session.pop("platform_superadmin_mandant_slug", None)
            request.session.pop("platform_superadmin_user_id", None)
        else:
            SessionLocal = get_sessionmaker(slug_s)
            tdb = SessionLocal()
            try:
                try:
                    tu = tdb.get(models.User, uid_int)
                except OperationalError:
                    tu = None
                if tu and tu.is_admin and tu.is_approved:
                    return SuperadminPrincipal(
                        mandant_slug=slug_s,
                        mandant_user_id=uid_int,
                    )
            finally:
                tdb.close()
            request.session.pop("platform_superadmin_mandant_slug", None)
            request.session.pop("platform_superadmin_user_id", None)

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Superadmin-Anmeldung erforderlich.",
    )


PlatformAdmin = Annotated[SuperadminPrincipal, Depends(require_superadmin)]
