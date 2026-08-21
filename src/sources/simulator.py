from __future__ import annotations

import random
import uuid
from datetime import date, timedelta
from typing import Any

DEPARTMENTS = [
    {"dept_code": "ENG", "dept_name": "Engineering", "cost_center": "CC-1001", "headcount": 85},
    {"dept_code": "SAL", "dept_name": "Sales", "cost_center": "CC-1002", "headcount": 40},
    {"dept_code": "MKT", "dept_name": "Marketing", "cost_center": "CC-1003", "headcount": 22},
    {"dept_code": "FIN", "dept_name": "Finance", "cost_center": "CC-1004", "headcount": 15},
    {"dept_code": "HRD", "dept_name": "Human Resources", "cost_center": "CC-1005", "headcount": 12},
    {"dept_code": "OPS", "dept_name": "Operations", "cost_center": "CC-1006", "headcount": 30},
    {"dept_code": "CSX", "dept_name": "Customer Success", "cost_center": "CC-1007", "headcount": 25},
    {"dept_code": "RND", "dept_name": "R&D Labs", "cost_center": "CC-1008", "headcount": 18},
]

GL_ACCOUNTS = [
    {"account_code": "REV-4000", "account_name": "Product Revenue", "category": "Revenue"},
    {"account_code": "REV-4100", "account_name": "Subscription Revenue", "category": "Revenue"},
    {"account_code": "REV-4200", "account_name": "Professional Services Revenue", "category": "Revenue"},
    {"account_code": "EXP-6000", "account_name": "Salaries & Wages", "category": "OPEX"},
    {"account_code": "EXP-6100", "account_name": "Software & Cloud", "category": "OPEX"},
    {"account_code": "EXP-6200", "account_name": "Marketing Spend", "category": "OPEX"},
    {"account_code": "EXP-6300", "account_name": "Travel & Entertainment", "category": "OPEX"},
    {"account_code": "EXP-6400", "account_name": "Office Facilities", "category": "OPEX"},
    {"account_code": "EXP-6500", "account_name": "Professional Fees", "category": "OPEX"},
    {"account_code": "CAP-7000", "account_name": "Hardware & Equipment", "category": "CAPEX"},
]

VENDORS = [
    {"vendor_name": "CloudPeak Systems", "vendor_category": "Technology", "country": "USA"},
    {"vendor_name": "Meridian Consulting", "vendor_category": "Services", "country": "USA"},
    {"vendor_name": "Northwind Traders", "vendor_category": "Distribution", "country": "Netherlands"},
    {"vendor_name": "Apex Cloud Services", "vendor_category": "Technology", "country": "Ireland"},
    {"vendor_name": "BlueRiver Media", "vendor_category": "Media", "country": "USA"},
    {"vendor_name": "Orbit Hardware Co", "vendor_category": "Manufacturing", "country": "Taiwan"},
    {"vendor_name": "Sterling Legal LLP", "vendor_category": "Services", "country": "UK"},
    {"vendor_name": "Quantum Analytics", "vendor_category": "Technology", "country": "Germany"},
    {"vendor_name": "Harbor Facilities Group", "vendor_category": "Real Estate", "country": "USA"},
    {"vendor_name": "Vertex Travel Partners", "vendor_category": "Travel", "country": "Singapore"},
]

REVENUE_ACCOUNT_CODES = [a["account_code"] for a in GL_ACCOUNTS if a["category"] == "Revenue"]
EXPENSE_ACCOUNT_CODES = [a["account_code"] for a in GL_ACCOUNTS if a["category"] != "Revenue"]

_DEPT_EXPENSE_PROFILE = {
    "ENG": {"EXP-6000": 3.0, "EXP-6100": 1.8, "CAP-7000": 0.5, "EXP-6500": 0.3},
    "SAL": {"EXP-6000": 2.2, "EXP-6300": 1.4, "EXP-6200": 0.6},
    "MKT": {"EXP-6000": 1.2, "EXP-6200": 3.2, "EXP-6100": 0.7},
    "FIN": {"EXP-6000": 1.0, "EXP-6500": 1.1, "EXP-6100": 0.4},
    "HRD": {"EXP-6000": 0.9, "EXP-6500": 0.6, "EXP-6300": 0.3},
    "OPS": {"EXP-6000": 1.6, "EXP-6100": 1.0, "EXP-6400": 1.2, "CAP-7000": 0.4},
    "CSX": {"EXP-6000": 1.4, "EXP-6100": 0.9, "EXP-6300": 0.4},
    "RND": {"EXP-6000": 1.5, "EXP-6100": 1.3, "CAP-7000": 0.6},
}

_MONTH_SEASON = [0.86, 0.88, 1.02, 0.98, 1.00, 1.05, 0.92, 0.94, 1.08, 1.06, 1.12, 1.18]


def _month_factor(d: date) -> float:
    growth = 1.0 + 0.007 * ((d.year - 2024) * 12 + d.month - 1)
    month_end_push = 1.35 if d.day >= 27 else (1.10 if d.day >= 24 else 1.0)
    weekday_boost = 1.05 if d.weekday() < 5 else 0.72
    return _MONTH_SEASON[d.month - 1] * growth * month_end_push * weekday_boost


def generate_masters() -> dict[str, list[dict[str, Any]]]:
    return {
        "departments": DEPARTMENTS,
        "gl_accounts": GL_ACCOUNTS,
        "vendors": VENDORS,
    }


def generate_transactions(
    days: int,
    txns_per_day: int,
    end_date: date | None = None,
    seed: int = 42,
) -> list[dict[str, Any]]:
    rng = random.Random(seed)
    end = end_date or date.today()
    rows: list[dict[str, Any]] = []
    for offset in range(days):
        d = end - timedelta(days=offset)
        factor = _month_factor(d)
        n_rev = max(1, round(txns_per_day * 0.28 * (1 + (factor - 1) * 0.5)))
        n_exp = max(1, round(txns_per_day * 0.72))
        for _ in range(n_rev):
            dept = rng.choice(DEPARTMENTS)["dept_code"]
            acct = rng.choice(REVENUE_ACCOUNT_CODES)
            base = {"REV-4000": 14000, "REV-4100": 8000, "REV-4200": 4500}[acct]
            amount = round(base * factor * rng.uniform(0.55, 1.55), 2)
            rows.append(_txn(d, dept, acct, amount, "revenue", rng))
        for _ in range(n_exp):
            dept = rng.choice(DEPARTMENTS)["dept_code"]
            weights = _DEPT_EXPENSE_PROFILE[dept]
            acct = rng.choices(list(weights.keys()), weights=list(weights.values()))[0]
            base = {
                "EXP-6000": 2600, "EXP-6100": 1500, "EXP-6200": 2200, "EXP-6300": 700,
                "EXP-6400": 1800, "EXP-6500": 1100, "CAP-7000": 3200,
            }[acct]
            amount = round(base * factor * rng.uniform(0.45, 1.6), 2)
            rows.append(_txn(d, dept, acct, amount, "expense", rng))
    return rows


def _txn(d: date, dept: str, acct: str, amount: float, txn_type: str, rng: random.Random) -> dict[str, Any]:
    vendor_pool = VENDORS if txn_type == "expense" else [{"vendor_name": "Enterprise Customer"}]
    return {
        "transaction_id": uuid.uuid4().hex[:16],
        "event_ts": f"{d.isoformat()}T{rng.randint(6, 21):02d}:{rng.randint(0, 59):02d}:00Z",
        "event_date": d.isoformat(),
        "dept_code": dept,
        "account_code": acct,
        "vendor_name": rng.choice(vendor_pool)["vendor_name"],
        "amount_usd": amount,
        "currency": "USD",
        "txn_type": txn_type,
        "source_channel": "kafka_stream",
    }


def generate_budgets(months: int, end_date: date | None = None) -> list[dict[str, Any]]:
    rng = random.Random(7)
    end = end_date or date.today()
    rows: list[dict[str, Any]] = []
    current = date(end.year, end.month, 1)
    months_list = []
    cursor = current
    for _ in range(months):
        months_list.append(cursor)
        cursor = date(cursor.year + (cursor.month == 12), (cursor.month % 12) + 1, 1)
    months_list.reverse()
    for m in months_list:
        factor = _MONTH_SEASON[m.month - 1]
        for dept in DEPARTMENTS:
            for acct in EXPENSE_ACCOUNT_CODES:
                if acct not in _DEPT_EXPENSE_PROFILE[dept["dept_code"]]:
                    continue
                planned = round(rng.uniform(0.85, 1.15) * factor * 100000 / len(EXPENSE_ACCOUNT_CODES), 2)
                rows.append(
                    {
                        "budget_id": uuid.uuid4().hex[:12],
                        "year_month": m.strftime("%Y-%m"),
                        "dept_code": dept["dept_code"],
                        "account_code": acct,
                        "budget_amount_usd": planned,
                        "source_channel": "file_upload",
                    }
                )
    return rows


ADJUSTMENT_REASONS = [
    "Quarter-end accrual", "Vendor credit memo", "Intercompany charge",
    "Manual journal correction", "Deferred revenue release", "FX revaluation entry",
]


def generate_manual_adjustments(count: int = 140, end_date: date | None = None) -> list[dict[str, Any]]:
    rng = random.Random(21)
    end = end_date or date.today()
    rows: list[dict[str, Any]] = []
    for _ in range(count):
        d = end - timedelta(days=rng.randint(0, 90))
        is_revenue = rng.random() < 0.3
        acct = rng.choice(REVENUE_ACCOUNT_CODES if is_revenue else EXPENSE_ACCOUNT_CODES)
        amount = round(rng.uniform(500, 18000), 2)
        rows.append(
            {
                "transaction_id": uuid.uuid4().hex[:16],
                "event_ts": f"{d.isoformat()}T12:{rng.randint(0, 59):02d}:00Z",
                "event_date": d.isoformat(),
                "dept_code": rng.choice(DEPARTMENTS)["dept_code"],
                "account_code": acct,
                "vendor_name": f"Adjustment - {rng.choice(ADJUSTMENT_REASONS)}",
                "amount_usd": amount,
                "currency": "USD",
                "txn_type": "revenue" if is_revenue else "expense",
                "source_channel": "file_upload",
            }
        )
    return rows


ERP_EXPORT_DEPTS = [
    {"dept_code": d["dept_code"], "dept_name": d["dept_name"], "cost_center": d["cost_center"], "headcount": d["headcount"]}
    for d in DEPARTMENTS
]
