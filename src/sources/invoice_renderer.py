from __future__ import annotations

import random
from datetime import date, timedelta
from pathlib import Path

from PIL import Image, ImageDraw

from config import settings

VENDOR_TEMPLATES = [
    ("Apex Cloud Services", "Technology", [("$4,800.00", "Cloud compute - 12 nodes"), ("$1,250.00", "Object storage tier 2")]),
    ("Meridian Consulting", "Services", [("$6,500.00", "Data platform advisory"), ("$2,300.00", "Migration support")]),
    ("Orbit Hardware Co", "Manufacturing", [("$9,120.00", "GPU workstations x3"), ("$540.00", "Networking gear")]),
    ("BlueRiver Media", "Media", [("$3,750.00", "Campaign creatives"), ("$1,900.00", "Analytics add-on")]),
]


def render_sample_invoices(count: int = 4, folder: Path | None = None) -> list[Path]:
    folder = folder or settings.INVOICES_DIR
    folder.mkdir(parents=True, exist_ok=True)
    rng = random.Random(99)
    paths: list[Path] = []
    for i in range(1, count + 1):
        vendor, category, items = VENDOR_TEMPLATES[(i - 1) % len(VENDOR_TEMPLATES)]
        invoice_date = date.today() - timedelta(days=rng.randint(1, 45))
        invoice_no = f"INV-2026-{1000 + i * 7}"
        img = Image.new("RGB", (820, 640), "white")
        draw = ImageDraw.Draw(img)
        y = 30
        draw.text((40, y), vendor.upper(), fill="black")
        draw.text((600, y), f"{category} Vendor", fill="#444444")
        y += 34
        draw.text((40, y), f"Invoice #: {invoice_no}", fill="black")
        y += 24
        draw.text((40, y), f"Invoice Date: {invoice_date.strftime('%B %d, %Y')}", fill="black")
        y += 24
        draw.text((40, y), f"Bill To: Internal - {rng.choice(['ENG', 'OPS', 'MKT', 'RND'])} Department", fill="black")
        y += 40
        draw.line((40, y, 780, y), fill="#888888")
        y += 18
        subtotal = 0.0
        for amount_str, desc in items:
            draw.text((60, y), desc, fill="black")
            draw.text((620, y), amount_str, fill="black")
            subtotal += float(amount_str.replace("$", "").replace(",", ""))
            y += 26
        tax = round(subtotal * 0.08, 2)
        total = round(subtotal + tax, 2)
        y += 10
        draw.line((40, y, 780, y), fill="#888888")
        y += 16
        draw.text((500, y), f"Tax: ${tax:,.2f}", fill="black")
        y += 26
        draw.text((500, y), f"Grand Total: ${total:,.2f}", fill="black")
        y += 40
        draw.text((40, y), "Payment due within 30 days. Thank you for your business.", fill="#333333")
        out_path = folder / f"invoice_{i:02d}.png"
        img.save(out_path)
        paths.append(out_path)
    return paths
