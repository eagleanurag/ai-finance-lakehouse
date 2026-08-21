from __future__ import annotations

import json
import re
from datetime import date
from pathlib import Path
from typing import Any

from config import settings

_ENGINES: dict[str, Any] = {}


def available_engine() -> str | None:
    for name, checker in (
        ("pytesseract", _try_pytesseract),
        ("rapidocr", _try_rapidocr),
    ):
        if checker():
            return name
    return None


def _try_pytesseract() -> bool:
    if "pytesseract" in _ENGINES:
        return _ENGINES["pytesseract"] is not None
    try:
        import pytesseract

        pytesseract.get_tesseract_version()
        _ENGINES["pytesseract"] = pytesseract
    except Exception:
        _ENGINES["pytesseract"] = None
    return _ENGINES["pytesseract"] is not None


def _try_rapidocr() -> bool:
    if "rapidocr" in _ENGINES:
        return _ENGINES["rapidocr"] is not None
    try:
        from rapidocr_onnxruntime import RapidOCR

        _ENGINES["rapidocr"] = RapidOCR()
    except Exception:
        _ENGINES["rapidocr"] = None
    return _ENGINES["rapidocr"] is not None


def image_to_text(image_path: Path) -> str:
    engine = available_engine()
    if engine == "pytesseract":
        from PIL import Image

        text = _ENGINES["pytesseract"].image_to_string(Image.open(image_path))
        return str(text)
    if engine == "rapidocr":
        result, _ = _ENGINES["rapidocr"](str(image_path))
        if not result:
            return ""
        return "\n".join(line[1] for line in result)
    raise RuntimeError(
        "No OCR engine installed. Run: pip install rapidocr-onnxruntime "
        "(pip-only, no system binary) or install Tesseract + pytesseract."
    )


DATE_PATTERNS = [
    r"(?:invoice\s*date|dated?)\s*[:\-]?\s*([A-Z][a-z]+\s+\d{1,2},?\s+\d{4})",
    r"(?:invoice\s*date|dated?)\s*[:\-]?\s*(\d{4}-\d{2}-\d{2})",
    r"(?:invoice\s*date|dated?)\s*[:\-]?\s*(\d{1,2}/\d{1,2}/\d{2,4})",
]
MONEY_PATTERN = r"\$?\s?([\d,]{3,}\.\d{2})"
TOTAL_PATTERN = r"(?:grand\s*total|total\s*amount|amount\s*due|total)\s*[:\-]?\s*" + MONEY_PATTERN
INVOICE_NO_PATTERN = r"invoice\s*(?:#|no\.?|number)?\s*[:\-]?\s*([A-Z]{2,5}(?:[-/]\d+){1,3})"


def parse_invoice_fields(text: str) -> dict[str, Any]:
    cleaned = " ".join(text.split())
    fields: dict[str, Any] = {"raw_text_chars": len(cleaned)}
    invoice_no = re.search(INVOICE_NO_PATTERN, cleaned, re.IGNORECASE)
    fields["invoice_number"] = invoice_no.group(1).upper() if invoice_no else None

    parsed_date = None
    for pattern in DATE_PATTERNS:
        match = re.search(pattern, cleaned, re.IGNORECASE)
        if match:
            parsed_date = _coerce_date(match.group(1))
            if parsed_date:
                break
    fields["invoice_date"] = parsed_date.isoformat() if parsed_date else None

    total = None
    total_match = re.search(TOTAL_PATTERN, cleaned, re.IGNORECASE)
    if total_match:
        total = _to_float(total_match.group(1))
    if total is None:
        candidates = [_to_float(m) for m in re.findall(MONEY_PATTERN, cleaned)]
        candidates = [c for c in candidates if c is not None and c > 0]
        total = max(candidates) if candidates else None
    fields["total_amount_usd"] = total

    tax_match = re.search(r"(?:tax|gst|vat)\s*[:\-]?\s*" + MONEY_PATTERN, cleaned, re.IGNORECASE)
    fields["tax_amount_usd"] = _to_float(tax_match.group(1)) if tax_match else None

    first_line = next((ln.strip() for ln in text.splitlines() if ln.strip()), "")
    fields["vendor_name_guess"] = first_line[:60]
    return fields


def process_invoices_folder(folder: Path | None = None) -> list[dict[str, Any]]:
    folder = folder or settings.INVOICES_DIR
    records: list[dict[str, Any]] = []
    for image_path in sorted(folder.glob("*")):
        if image_path.suffix.lower() not in {".png", ".jpg", ".jpeg", ".webp", ".bmp"}:
            continue
        text = image_to_text(image_path)
        fields = parse_invoice_fields(text)
        fields.update(
            {
                "source_channel": "ocr_invoice",
                "source_file": image_path.name,
            }
        )
        records.append(fields)
    return records


def save_ocr_results(records: list[dict[str, Any]]) -> Path:
    out = settings.DATA_DIR / f"ocr_results_{date.today().isoformat()}.json"
    with open(out, "w", encoding="utf-8") as fh:
        json.dump(records, fh, indent=2)
    return out


def _to_float(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return float(value.replace(",", ""))
    except ValueError:
        return None


def _coerce_date(value: str) -> date | None:
    value = value.replace(",", "")
    for fmt in ("%B %d %Y", "%b %d %Y", "%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y", "%d-%m-%Y"):
        try:
            from datetime import datetime

            return datetime.strptime(value.strip(), fmt).date()
        except ValueError:
            continue
    return None
