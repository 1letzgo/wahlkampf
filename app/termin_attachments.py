"""Termin-Dateianhänge (JSON-Liste, Speicher unter uploads/termin_anhang/)."""

from __future__ import annotations

import json
import re
import uuid
from pathlib import Path

from starlette.datastructures import UploadFile

ATTACH_SUBDIR = "termin_anhang"
MAX_TERMIN_ATTACHMENT_BYTES = 10 * 1024 * 1024

BLOCKED_SUFFIXES = frozenset({
    ".exe",
    ".bat",
    ".cmd",
    ".com",
    ".scr",
    ".msi",
    ".dll",
    ".sh",
    ".ps1",
    ".app",
    ".deb",
    ".rpm",
    ".vbs",
    ".jar",
})


def attachments_decode(raw: str | None) -> list[dict[str, str]]:
    if not raw or not str(raw).strip():
        return []
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return []
    if not isinstance(data, list):
        return []
    out: list[dict[str, str]] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        path = str(item.get("path") or "").strip().replace("\\", "/")
        name = str(item.get("name") or "").strip()
        if not path or ".." in path or path.startswith("/"):
            continue
        seg = path.split("/")
        if len(seg) < 2 or seg[0] != ATTACH_SUBDIR:
            continue
        if any(p == ".." or p == "" for p in seg):
            continue
        base = seg[-1]
        if not base or ".." in base:
            continue
        if not name:
            name = base
        out.append({"path": path, "name": name[:240]})
    return out


def attachments_encode(items: list[dict[str, str]]) -> str:
    return json.dumps(items, ensure_ascii=False)


def sanitize_original_filename(filename: str | None) -> tuple[str, str]:
    if not filename:
        return ("datei", "")
    base = Path(filename).name
    base = re.sub(r'[\x00-\x1f<>:"|?*\\/]', "_", base).strip() or "datei"
    suf = Path(base).suffix.lower()
    stem = Path(base).stem
    if suf in BLOCKED_SUFFIXES:
        raise ValueError(f'Dateityp "{suf}" ist aus Sicherheitsgründen nicht erlaubt.')
    max_stem = 180
    if len(stem) > max_stem:
        stem = stem[:max_stem]
    return (stem, suf)


async def save_attachment_upload(
    upload: UploadFile,
    *,
    termin_id: int,
    upload_root: Path,
) -> dict[str, str]:
    stem, suf = sanitize_original_filename(upload.filename)
    dest_name = f"{termin_id}_{uuid.uuid4().hex}_{stem}{suf}"
    rel = f"{ATTACH_SUBDIR}/{dest_name}"
    dest_dir = upload_root / ATTACH_SUBDIR
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / dest_name
    size = 0
    with dest.open("wb") as f:
        while chunk := await upload.read(1024 * 1024):
            size += len(chunk)
            if size > MAX_TERMIN_ATTACHMENT_BYTES:
                dest.unlink(missing_ok=True)
                raise ValueError(
                    f"Anhang zu groß (max. {MAX_TERMIN_ATTACHMENT_BYTES // (1024 * 1024)} MB pro Datei)."
                )
            f.write(chunk)
    display_name = f"{stem}{suf}"[:240]
    return {"path": rel, "name": display_name}
