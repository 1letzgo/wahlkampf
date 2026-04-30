"""Pfade unter /admin, die die Plattform (Superadmin) bedient — ohne /m/<slug>-Prefix."""


def is_platform_superadmin_scope_path(rel: str) -> bool:
    """True = Request nicht mit PUBLIC_SITE_HOST-Mandanten-Prefix anreichern."""
    if rel == "/admin":
        return True
    return rel.startswith("/admin/ortsverbaende")
