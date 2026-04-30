from __future__ import annotations

from pathlib import Path

from app.config import upload_dir_for_slug


def sharepic_mask_src_suffix(mandant_slug: str) -> str:
    """Relativer Pfad inkl. Mandanten-Präfix für die Sharepic-HTML."""
    slug = mandant_slug.strip().lower()
    if (upload_dir_for_slug(slug) / "sharepic-mask.png").is_file():
        return f"/m/{slug}/media/sharepic-mask.png"
    return "/static/sharepic-mask2.png"
