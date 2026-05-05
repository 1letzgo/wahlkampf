"""OV-spezifische Sharepic-Hintergrundvorlagen (Uploads unter uploads/sharepic-vorlagen/)."""

from __future__ import annotations

import json
import re
import uuid
from pathlib import Path

from fastapi import UploadFile

from app.config import MAX_UPLOAD_MB, upload_dir_for_slug

SHAREPIC_TEMPLATE_SUBDIR = "sharepic-vorlagen"
MANIFEST_FILENAME = "manifest.json"
MAX_SHAREPIC_TEMPLATES = 24

ALLOWED_CT = frozenset({"image/jpeg", "image/png", "image/webp"})
_EXT_MAP = {".jpg": ".jpg", ".jpeg": ".jpg", ".png": ".png", ".webp": ".webp"}


def _safe_ext(filename: str | None, content_type: str | None) -> str:
    if filename:
        suf = Path(filename).suffix.lower()
        if suf in _EXT_MAP:
            return _EXT_MAP[suf]
    if content_type == "image/jpeg":
        return ".jpg"
    if content_type == "image/png":
        return ".png"
    if content_type == "image/webp":
        return ".webp"
    return ""


def _sanitize_label(raw: str | None, fallback: str) -> str:
    s = " ".join((raw or "").split()).strip()
    if not s:
        s = fallback
    return s[:120]


def templates_dir(slug: str) -> Path:
    return upload_dir_for_slug(slug.strip().lower()) / SHAREPIC_TEMPLATE_SUBDIR


def ensure_templates_dir(slug: str) -> Path:
    d = templates_dir(slug)
    d.mkdir(parents=True, exist_ok=True)
    mf = d / MANIFEST_FILENAME
    if not mf.exists():
        mf.write_text("[]", encoding="utf-8")
    return d


def load_manifest(dir_path: Path) -> list[dict]:
    mf = dir_path / MANIFEST_FILENAME
    if not mf.is_file():
        return []
    try:
        data = json.loads(mf.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []
    if not isinstance(data, list):
        return []
    out: list[dict] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        tid = str(item.get("id") or "").strip()
        fn = str(item.get("file") or "").strip()
        lbl = str(item.get("label") or "").strip()
        if not tid or not fn or "/" in fn or "\\" in fn or fn.startswith("."):
            continue
        out.append({"id": tid, "file": fn, "label": lbl or fn})
    return out


def save_manifest(dir_path: Path, entries: list[dict]) -> None:
    mf = dir_path / MANIFEST_FILENAME
    mf.write_text(json.dumps(entries, ensure_ascii=False, indent=2), encoding="utf-8")


def list_templates(slug: str) -> list[dict]:
    """Jede Zeile: id, label, rel_path (relativ zu uploads/, für /media/…)."""
    ms = slug.strip().lower()
    d = templates_dir(ms)
    if not d.is_dir():
        return []
    upload_root = upload_dir_for_slug(ms).resolve()
    entries = load_manifest(d)
    out: list[dict] = []
    for e in entries:
        fp = (d / e["file"]).resolve()
        try:
            fp.relative_to(upload_root)
        except ValueError:
            continue
        if not fp.is_file():
            continue
        rel = f"{SHAREPIC_TEMPLATE_SUBDIR}/{e['file']}"
        out.append({"id": e["id"], "label": e["label"], "rel_path": rel})
    return out


async def upload_template(slug: str, upload: UploadFile, label_raw: str | None) -> tuple[bool, str]:
    if not upload.filename:
        return False, "Keine Datei gewählt."
    ct = (upload.content_type or "").strip()
    ext = _safe_ext(upload.filename, ct or None)
    if not ext or ct not in ALLOWED_CT:
        return False, "Nur JPEG-, PNG- oder WebP-Bilder erlaubt."

    d = ensure_templates_dir(slug)
    manifest = load_manifest(d)
    if len(manifest) >= MAX_SHAREPIC_TEMPLATES:
        return False, f"Maximal {MAX_SHAREPIC_TEMPLATES} Vorlagen pro Ortsverband."

    stem = Path(upload.filename).stem
    stem_safe = re.sub(r"[^\w\-]+", "_", stem, flags=re.UNICODE)[:40] or "vorlage"
    fallback_label = stem_safe.replace("_", " ").strip() or "Vorlage"
    label = _sanitize_label(label_raw, fallback_label)

    uid = uuid.uuid4().hex
    fname = f"{uid}{ext}"
    dest = d / fname
    max_b = MAX_UPLOAD_MB * 1024 * 1024
    size = 0
    try:
        with dest.open("wb") as f:
            while chunk := await upload.read(1024 * 1024):
                size += len(chunk)
                if size > max_b:
                    dest.unlink(missing_ok=True)
                    return False, f"Bild zu groß (max. {MAX_UPLOAD_MB} MB)."
                f.write(chunk)
    except OSError:
        dest.unlink(missing_ok=True)
        return False, "Speichern fehlgeschlagen."

    manifest.append({"id": uid, "file": fname, "label": label})
    save_manifest(d, manifest)
    return True, ""


def delete_template(slug: str, template_id: str) -> tuple[bool, str]:
    tid = (template_id or "").strip()
    if not tid:
        return False, "Ungültige Vorlage."
    d = ensure_templates_dir(slug)
    manifest = load_manifest(d)
    new_m: list[dict] = []
    removed: str | None = None
    for e in manifest:
        if e["id"] == tid:
            removed = e["file"]
        else:
            new_m.append(e)
    if removed is None:
        return False, "Vorlage nicht gefunden."
    (d / removed).unlink(missing_ok=True)
    save_manifest(d, new_m)
    return True, ""
