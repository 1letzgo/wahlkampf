"""Globale statische Assets (z. B. Sharepic — eine Maske für alle OVs)."""

from __future__ import annotations

# Ein gemeinsames Layout; ausliefern unter app/static/
SHAREPIC_MASK_STATIC_URL = "/static/sharepic-mask.png"


def sharepic_mask_url() -> str:
    """Öffentlicher URL-Pfad zur Sharepic-Maske (nicht mandantenspezifisch)."""
    return SHAREPIC_MASK_STATIC_URL
