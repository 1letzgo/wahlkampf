"""Vordefinierte externe Termin-Gäste (nicht als App-Nutzer angemeldet)."""

from __future__ import annotations

import json
from typing import Final

EXTERNE_TEILNEHMER_OPTIONS: Final[tuple[tuple[str, str], ...]] = (
    ("bjoern_meyer", "Björn Meyer"),
    ("dennis_rohde", "Dennis Rohde"),
    ("minister", "Minister"),
)
EXTERNE_TEILNEHMER_KEYS: Final[frozenset[str]] = frozenset(
    k for k, _ in EXTERNE_TEILNEHMER_OPTIONS
)
EXTERNE_TEILNEHMER_LABEL: Final[dict[str, str]] = dict(EXTERNE_TEILNEHMER_OPTIONS)


def externe_teilnehmer_encode(selected_keys: list[str]) -> str:
    keys = sorted({k for k in selected_keys if k in EXTERNE_TEILNEHMER_KEYS})
    return json.dumps(keys)


def externe_teilnehmer_decode(raw: str | None) -> list[str]:
    if not raw or not str(raw).strip():
        return []
    try:
        data = json.loads(raw)
        if not isinstance(data, list):
            return []
        return sorted(
            {str(x) for x in data if str(x) in EXTERNE_TEILNEHMER_KEYS},
        )
    except (json.JSONDecodeError, TypeError):
        return []


def externe_teilnehmer_labels(keys: list[str]) -> list[str]:
    return [EXTERNE_TEILNEHMER_LABEL[k] for k in keys if k in EXTERNE_TEILNEHMER_LABEL]
