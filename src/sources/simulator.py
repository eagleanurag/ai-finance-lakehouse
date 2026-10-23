"""Synthetic enterprise finance sources at scale, with the defects real bronze layers see.

Every source deliberately carries data-quality problems so the silver notebook has real
work to do: at-least-once duplicates, CDC updates and deletes, late and future-dated
events, schema drift, mixed timestamp/amount/code formats, orphan keys, invalid
currencies, FX gaps, near-duplicate master records and SCD2 attribute changes.

Volumes are driven by the target fact row count (default 2M); every month is seeded
independently so re-runs reproduce the same history.
"""
from __future__ import annotations

import re
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterator

import numpy as np
import pandas as pd

# --------------------------------------------------------------------------- masters

# code, name, headcount weight, share of company revenue
FUNCTIONS = [
    ("ENG", "Engineering", 9.0, 0.05), ("SAL", "Sales", 6.0, 0.42), ("MKT", "Marketing", 3.5, 0.0),
    ("FIN", "Finance", 2.0, 0.0), ("HRD", "Human Resources", 1.5, 0.0), ("OPS", "Operations", 4.0, 0.0),
    ("CSX", "Customer Success", 3.5, 0.18), ("RND", "R&D Labs", 2.5, 0.0), ("LEG", "Legal", 1.0, 0.0),
    ("PRC", "Procurement", 1.0, 0.0), ("ITS", "IT Services", 2.0, 0.0), ("SEC", "Security", 1.2, 0.0),
    ("DAT", "Data & Analytics", 1.8, 0.0), ("PRD", "Product", 2.5, 0.10), ("QAS", "Quality Assurance", 1.5, 0.0),
    ("FAC", "Facilities", 1.0, 0.0), ("TRS", "Treasury", 0.6, 0.0), ("TAX", "Tax", 0.6, 0.0),
    ("AUD", "Internal Audit", 0.5, 0.0), ("BDV", "Business Development", 1.5, 0.25),
]
REGIONS = [("NA", "North America", 0.6), ("EMEA", "EMEA", 0.4)]

GL_ACCOUNTS = [
    ("REV-4000", "Product Revenue", "Revenue"), ("REV-4100", "Subscription Revenue", "Revenue"),
    ("REV-4200", "Professional Services Revenue", "Revenue"), ("REV-4300", "Support & Maintenance", "Revenue"),
    ("REV-4400", "Licensing Revenue", "Revenue"), ("REV-4500", "Training Revenue", "Revenue"),
    ("REV-4600", "Marketplace Fees", "Revenue"), ("REV-4700", "Hardware Sales", "Revenue"),
    ("REV-4800", "Partner Revenue", "Revenue"), ("REV-4900", "Other Operating Revenue", "Revenue"),
    ("COGS-5000", "Hosting Costs", "COGS"), ("COGS-5100", "Third-party Licenses", "COGS"),
    ("COGS-5200", "Payment Processing", "COGS"), ("COGS-5300", "Customer Support Labor", "COGS"),
    ("COGS-5400", "Hardware COGS", "COGS"), ("COGS-5500", "Implementation Labor", "COGS"),
    ("COGS-5600", "Data Costs", "COGS"), ("COGS-5700", "Freight", "COGS"),
    ("EXP-6000", "Salaries & Wages", "OPEX"), ("EXP-6010", "Bonuses", "OPEX"),
    ("EXP-6020", "Payroll Taxes", "OPEX"), ("EXP-6030", "Employee Benefits", "OPEX"),
    ("EXP-6040", "Contractors", "OPEX"), ("EXP-6050", "Recruiting", "OPEX"),
    ("EXP-6060", "Training & Development", "OPEX"), ("EXP-6100", "Software & SaaS", "OPEX"),
    ("EXP-6110", "Cloud Infrastructure", "OPEX"), ("EXP-6120", "IT Hardware (Expensed)", "OPEX"),
    ("EXP-6130", "Telecom", "OPEX"), ("EXP-6200", "Paid Media", "OPEX"),
    ("EXP-6210", "Events & Sponsorships", "OPEX"), ("EXP-6220", "Content & Creative", "OPEX"),
    ("EXP-6230", "Market Research", "OPEX"), ("EXP-6300", "Travel - Air", "OPEX"),
    ("EXP-6310", "Travel - Lodging", "OPEX"), ("EXP-6320", "Meals & Entertainment", "OPEX"),
    ("EXP-6400", "Rent", "OPEX"), ("EXP-6410", "Utilities", "OPEX"),
    ("EXP-6420", "Office Supplies", "OPEX"), ("EXP-6430", "Repairs & Maintenance", "OPEX"),
    ("EXP-6500", "Legal Fees", "OPEX"), ("EXP-6510", "Audit & Accounting", "OPEX"),
    ("EXP-6520", "Consulting", "OPEX"), ("EXP-6530", "Insurance", "OPEX"),
    ("EXP-6600", "Bank Fees", "OPEX"), ("EXP-6610", "Bad Debt", "OPEX"),
    ("EXP-6700", "Depreciation", "OPEX"), ("EXP-6710", "Amortization", "OPEX"),
    ("EXP-6800", "Dues & Subscriptions", "OPEX"), ("EXP-6900", "Miscellaneous (deprecated)", "OPEX"),
    ("CAP-7000", "Computer Equipment", "CAPEX"), ("CAP-7100", "Furniture & Fixtures", "CAPEX"),
    ("CAP-7200", "Leasehold Improvements", "CAPEX"), ("CAP-7300", "Capitalized Software", "CAPEX"),
    ("CAP-7400", "Vehicles", "CAPEX"), ("OTH-8000", "Interest Expense", "Other"),
    ("OTH-8100", "FX Gain/Loss", "Other"), ("OTH-8200", "Other Income", "Other"),
    ("OTH-8300", "Donations", "Other"), ("OTH-8400", "Penalties & Fines", "Other"),
]
DEPRECATED_ACCOUNTS = {"EXP-6900"}
# the source system spells categories inconsistently; silver maps these back
CATEGORY_SPELLINGS = {
    "Revenue": ["Revenue", "revenue", "REV"], "COGS": ["COGS", "Cost of Sales", "cogs"],
    "OPEX": ["OPEX", "Opex", "Operating Expense"], "CAPEX": ["CAPEX", "Capex"], "Other": ["Other", "Non-Operating"],
}

REVENUE_ACCOUNT_WEIGHTS = {
    "REV-4000": 3.0, "REV-4100": 4.0, "REV-4200": 1.5, "REV-4300": 1.2, "REV-4400": 0.6,
    "REV-4500": 0.3, "REV-4600": 0.4, "REV-4700": 0.5, "REV-4800": 0.8, "REV-4900": 0.2,
}
COMMON_EXPENSE_WEIGHTS = {
    "EXP-6000": 4.0, "EXP-6010": 0.5, "EXP-6020": 0.6, "EXP-6030": 0.8, "EXP-6100": 0.6,
    "EXP-6300": 0.3, "EXP-6310": 0.3, "EXP-6320": 0.2, "EXP-6420": 0.1, "EXP-6800": 0.1, "EXP-6900": 0.05,
}
FUNCTION_EXPENSE_WEIGHTS = {
    "ENG": {"EXP-6110": 2.0, "EXP-6040": 1.0, "CAP-7000": 0.5, "CAP-7300": 0.6, "COGS-5000": 1.0},
    "SAL": {"EXP-6300": 1.2, "EXP-6320": 0.8, "EXP-6200": 0.3},
    "MKT": {"EXP-6200": 3.0, "EXP-6210": 1.5, "EXP-6220": 1.0, "EXP-6230": 0.4},
    "FIN": {"EXP-6510": 1.0, "EXP-6600": 0.4, "OTH-8000": 0.3, "OTH-8100": 0.3, "EXP-6610": 0.2,
            "EXP-6530": 0.4, "EXP-6700": 0.6, "EXP-6710": 0.3, "COGS-5200": 0.5},
    "HRD": {"EXP-6050": 1.2, "EXP-6060": 0.8, "EXP-6030": 0.6},
    "OPS": {"COGS-5700": 0.8, "EXP-6400": 1.0, "EXP-6410": 0.5, "EXP-6430": 0.4, "CAP-7100": 0.3,
            "CAP-7400": 0.2, "COGS-5400": 0.6},
    "CSX": {"COGS-5300": 1.5, "EXP-6100": 0.5, "COGS-5500": 0.8},
    "RND": {"EXP-6040": 1.0, "CAP-7000": 0.6, "COGS-5600": 0.5},
    "LEG": {"EXP-6500": 2.0, "OTH-8400": 0.1},
    "PRC": {"EXP-6520": 0.6, "COGS-5100": 0.6},
    "ITS": {"EXP-6120": 1.0, "EXP-6130": 0.8, "EXP-6110": 1.0, "CAP-7000": 0.8},
    "SEC": {"EXP-6100": 1.0, "EXP-6520": 0.5},
    "DAT": {"COGS-5600": 1.2, "EXP-6110": 1.2},
    "PRD": {"EXP-6230": 0.5, "EXP-6040": 0.6},
    "QAS": {"EXP-6040": 0.5, "EXP-6120": 0.3},
    "FAC": {"EXP-6400": 2.5, "EXP-6410": 1.0, "EXP-6430": 0.8, "CAP-7200": 0.4, "CAP-7100": 0.3},
    "TRS": {"EXP-6600": 1.0, "OTH-8000": 0.8, "OTH-8100": 0.6, "OTH-8200": 0.3},
    "TAX": {"EXP-6510": 0.8, "OTH-8400": 0.2},
    "AUD": {"EXP-6510": 0.6, "EXP-6520": 0.4},
    "BDV": {"EXP-6300": 1.0, "EXP-6320": 0.6, "EXP-6210": 0.4, "OTH-8300": 0.1},
}
ACCOUNT_BASE_USD = {
    "REV-4000": 16000, "REV-4100": 9000, "REV-4700": 11000, "EXP-6000": 5200, "EXP-6010": 3000,
    "EXP-6020": 900, "EXP-6030": 1100, "EXP-6040": 4200, "EXP-6400": 7500, "EXP-6700": 3800,
}
GROUP_BASE_USD = {"REV": 6000, "COGS": 2600, "EXP": 1400, "CAP": 9000, "OTH": 1200}

VENDOR_PREFIX = ["Apex", "Blue", "Cedar", "Delta", "Echo", "Falcon", "Granite", "Harbor", "Iron", "Jade",
                 "Keystone", "Lumen", "Meridian", "North", "Orbit", "Pioneer", "Quantum", "River", "Summit",
                 "Titan", "Unity", "Vertex", "Willow", "Zenith", "Atlas", "Beacon", "Cobalt", "Crest", "Nova",
                 "Prime"]
VENDOR_CORE = {
    "Cloud": "Technology", "Data": "Technology", "Software": "Technology", "Networks": "Technology",
    "Digital": "Technology", "Systems": "Technology", "Logistics": "Distribution", "Supply": "Distribution",
    "Consulting": "Services", "Partners": "Services", "Solutions": "Services", "Analytics": "Services",
    "Legal": "Professional Services", "Media": "Media", "Facilities": "Real Estate", "Travel": "Travel",
    "Hardware": "Manufacturing", "Labs": "Research", "Energy": "Utilities", "Staffing": "Staffing",
}
VENDOR_SUFFIX = ["Inc", "LLC", "Ltd", "Group", "Co", "GmbH", "Corp", "Partners", "PLC", "AG"]
COUNTRIES = {  # canonical name: (weight, messy spellings seen in source systems)
    "United States": (0.45, ["United States", "USA", "US", "U.S."]),
    "United Kingdom": (0.10, ["United Kingdom", "UK", "GB"]),
    "Germany": (0.08, ["Germany", "DE", "Deutschland"]),
    "Netherlands": (0.05, ["Netherlands", "NL", "Holland"]),
    "Ireland": (0.05, ["Ireland", "IE"]),
    "India": (0.08, ["India", "IN"]),
    "Singapore": (0.05, ["Singapore", "SG"]),
    "Japan": (0.04, ["Japan", "JP"]),
    "Canada": (0.06, ["Canada", "CA"]),
    "France": (0.04, ["France", "FR"]),
}
CUSTOMER_A = ["Acme", "Bright", "Clear", "Daily", "Evergreen", "First", "Global", "Happy", "Infinite", "Juniper",
              "Kinetic", "Liberty", "Modern", "Next", "Open", "Pacific", "Quick", "Royal", "Silver", "True",
              "Urban", "Vital", "West", "Young", "Zen", "Alpine", "Bold", "Coastal", "Dynamic", "Elite"]
CUSTOMER_B = ["Health", "Retail", "Bank", "Foods", "Motors", "Energy", "Media", "Logistics", "Pharma", "Telecom",
              "Insurance", "Capital", "Airlines", "Hotels", "Labs", "Foods", "Robotics", "Studios", "Mining",
              "Textiles", "Analytics", "Ventures", "Brands", "Clinics", "Schools", "Games", "Payments",
              "Security", "Travel", "Farms"]
CUSTOMER_KIND = ["Inc", "LLC", "Ltd", "Group", "Holdings", "Corp", "Co", "SA", "GmbH", "Pte Ltd"]
CUSTOMER_REGIONS = {"NA": ["United States", "Canada"], "EMEA": ["United Kingdom", "Germany", "France", "Netherlands"],
                    "APAC": ["India", "Singapore", "Japan"], "LATAM": ["Brazil", "Mexico"]}

FX_START = {"EUR": (1.08, 0.004), "GBP": (1.27, 0.004), "CAD": (0.73, 0.003), "INR": (0.012, 0.003),
            "SGD": (0.74, 0.003), "JPY": (0.0067, 0.005)}
CURRENCIES = ["USD"] + list(FX_START)
REGION_CURRENCY_MIX = {"NA": {"USD": 0.9, "CAD": 0.1},
                       "EMEA": {"EUR": 0.55, "GBP": 0.25, "USD": 0.1, "INR": 0.05, "SGD": 0.03, "JPY": 0.02}}
COUNTRY_CURRENCY = {"United States": "USD", "Canada": "CAD", "United Kingdom": "GBP", "Germany": "EUR",
                    "Netherlands": "EUR", "Ireland": "EUR", "France": "EUR", "India": "INR",
                    "Singapore": "SGD", "Japan": "JPY"}

TXN_TYPE_SPELLINGS = {"revenue": ["Revenue", "REV", "rev", "SALE", "revenue "],
                      "expense": ["Expense", "EXP", "exp", "EXPENSE", " expense"],
                      "refund": ["Refund", "RFD", "return"]}
AP_STATUS_SPELLINGS = {"PAID": ["PAID", "paid", "Paid "], "OPEN": ["OPEN", "Open", "open"],
                       "PARTIAL": ["PARTIAL", "Partially Paid", "partial"], "VOID": ["VOID", "void", "Voided"]}
ADJUSTMENT_REASONS = ["Quarter-end accrual", "Vendor credit memo", "Intercompany charge",
                      "Manual journal correction", "Deferred revenue release", "FX revaluation entry"]

_MONTH_SEASON = [0.86, 0.88, 1.02, 0.98, 1.00, 1.05, 0.92, 0.94, 1.08, 1.06, 1.12, 1.18]
_EPOCH_MARKER = re.compile(r'"__MS__(\d+)"')

# rates at which each defect is injected into transaction sources
DEFECTS = {
    "exact_duplicate": 0.010, "cdc_update": 0.006, "cdc_delete": 0.002, "late_arriving": 0.005,
    "future_dated": 0.0003, "null_id": 0.0003, "orphan_dept": 0.001, "blank_dept": 0.001,
    "code_variant": 0.05, "amount_null": 0.0003, "amount_zero": 0.0005, "amount_outlier": 0.0001,
    "currency_lower": 0.03, "currency_null": 0.003, "currency_invalid": 0.0001,
    "txn_type_variant": 0.10, "txn_type_null": 0.005, "vendor_name_variant": 0.03,
    "vendor_id_missing": 0.05, "vendor_unregistered": 0.002, "customer_unknown": 0.0005,
    "customer_duplicate_id": 0.01, "malformed_json": 0.0005,
}


def _month_start(d: date) -> date:
    return date(d.year, d.month, 1)


def _add_months(d: date, n: int) -> date:
    y, m = divmod(d.month - 1 + n, 12)
    return date(d.year + y, m + 1, 1)


def _month_end(d: date) -> date:
    return _add_months(d, 1) - timedelta(days=1)


def _zipf_weights(n: int, rng: np.random.Generator, s: float = 1.05) -> np.ndarray:
    w = 1.0 / np.arange(1, n + 1) ** s
    rng.shuffle(w)
    return w / w.sum()


def _iso(ts: np.ndarray) -> np.ndarray:
    return np.datetime_as_string(ts.astype("datetime64[s]"), unit="s").astype(object) + "Z"


def _pick(rng: np.random.Generator, mask_rate: float, n: int) -> np.ndarray:
    return rng.random(n) < mask_rate


def _vary_codes(values: np.ndarray, rng: np.random.Generator, rate: float) -> np.ndarray:
    out = values.astype(object).copy()
    idx = np.flatnonzero(_pick(rng, rate, len(out)))
    styles = rng.integers(0, 4, len(idx))
    for i, s in zip(idx, styles):
        v = out[i]
        if not isinstance(v, str) or not v:
            continue
        out[i] = (v.lower(), f" {v} ", v.replace("-", "_"), v.replace("-", ""))[s]
    return out


def _vary_names(values: np.ndarray, rng: np.random.Generator, rate: float) -> np.ndarray:
    out = values.astype(object).copy()
    idx = np.flatnonzero(_pick(rng, rate, len(out)))
    styles = rng.integers(0, 4, len(idx))
    for i, s in zip(idx, styles):
        v = out[i]
        if not isinstance(v, str) or not v:
            continue
        out[i] = (v.upper(), f"  {v} ", v.replace(" & ", " &amp; ").replace(" ", "  ", 1), v.rstrip(".") + ".")[s]
    return out


def _vary_choice(canonical: np.ndarray, spellings: dict[str, list[str]], rng: np.random.Generator, rate: float) -> np.ndarray:
    out = canonical.astype(object).copy()
    idx = np.flatnonzero(_pick(rng, rate, len(out)))
    for i in idx:
        options = spellings.get(out[i])
        if options:
            out[i] = options[rng.integers(0, len(options))]
    return out


class FinanceDataGenerator:
    """Deterministic, vectorised generator for every raw source the lakehouse ingests."""

    def __init__(self, end_date: date | None = None, history_months: int = 24,
                 target_rows: int = 2_000_000, seed: int = 42) -> None:
        self.end_date = end_date or date.today()
        self.seed = seed
        self.history_start = _add_months(_month_start(self.end_date), -history_months)
        self.months = [_add_months(self.history_start, i) for i in range(history_months + 1)]
        self.calendar_start = self.history_start - timedelta(days=150)  # room for late-arriving events
        self.target_rows = target_rows
        self.n_vendors = 5000
        self.n_customers = 50000

        self._build_departments()
        self._build_accounts()
        self._build_vendors()
        self._build_customers()
        self._build_fx()
        self._build_volume_plan()

    # ------------------------------------------------------------------ structure
    def _build_departments(self) -> None:
        rng = np.random.default_rng(self.seed + 1)
        rows = []
        for f_code, f_name, weight, rev_share in FUNCTIONS:
            for r_code, r_name, r_share in REGIONS:
                rows.append({
                    "dept_code": f"{f_code}-{r_code}", "dept_name": f"{f_name} - {r_name}",
                    "function": f_code, "region": r_code,
                    "cost_center": f"CC-{1001 + len(rows)}",
                    "headcount": int(max(4, rng.normal(weight * 9 * (1.3 if r_code == "NA" else 0.8), weight * 2))),
                    "is_active": True, "_rev_share": rev_share * r_share,
                })
        self.departments = pd.DataFrame(rows)
        hc = self.departments["headcount"].to_numpy(float)
        self.dept_expense_p = hc / hc.sum()
        rs = self.departments["_rev_share"].to_numpy(float)
        self.dept_revenue_p = rs / rs.sum()
        self.dept_codes = self.departments["dept_code"].to_numpy(object)
        self.dept_region = self.departments["region"].to_numpy(object)

    def _build_accounts(self) -> None:
        self.accounts = pd.DataFrame(GL_ACCOUNTS, columns=["account_code", "account_name", "category"])
        self.accounts["parent_account_code"] = self.accounts["account_code"].str.slice(0, -2) + "00"
        self.accounts["is_active"] = ~self.accounts["account_code"].isin(DEPRECATED_ACCOUNTS)
        self.account_codes = self.accounts["account_code"].to_numpy(object)
        code_idx = {c: i for i, c in enumerate(self.account_codes)}
        self.account_base = np.array([
            ACCOUNT_BASE_USD.get(c, GROUP_BASE_USD[c.split("-")[0]]) for c in self.account_codes], float)
        self.account_category = self.accounts["category"].to_numpy(object)

        rev = REVENUE_ACCOUNT_WEIGHTS
        self.rev_acct_idx = np.array([code_idx[c] for c in rev])
        self.rev_acct_p = np.array(list(rev.values())) / sum(rev.values())

        self.dept_acct_idx: list[np.ndarray] = []
        self.dept_acct_p: list[np.ndarray] = []
        for f_code in self.departments["function"]:
            weights = dict(COMMON_EXPENSE_WEIGHTS)
            for k, v in FUNCTION_EXPENSE_WEIGHTS[f_code].items():
                weights[k] = weights.get(k, 0) + v
            self.dept_acct_idx.append(np.array([code_idx[c] for c in weights]))
            p = np.array(list(weights.values()), float)
            self.dept_acct_p.append(p / p.sum())

    def _build_vendors(self) -> None:
        rng = np.random.default_rng(self.seed + 2)
        combos = [(p, c, s) for p in VENDOR_PREFIX for c in VENDOR_CORE for s in VENDOR_SUFFIX]
        picks = rng.choice(len(combos), self.n_vendors, replace=False)
        names, cats = [], []
        for i in picks:
            p, c, s = combos[i]
            names.append(f"{p} {c} {s}".replace("Legal", "Legal &" if s == "Partners" else "Legal"))
            cats.append(VENDOR_CORE[c])
        country_names = list(COUNTRIES)
        cw = np.array([COUNTRIES[c][0] for c in country_names])
        countries = rng.choice(country_names, self.n_vendors, p=cw / cw.sum())
        self.vendors = pd.DataFrame({
            "vendor_id": [f"V{i:05d}" for i in range(1, self.n_vendors + 1)],
            "vendor_name": names, "vendor_category": cats, "country": countries,
            "payment_terms_days": rng.choice([15, 30, 45, 60], self.n_vendors, p=[0.1, 0.55, 0.2, 0.15]),
            "is_active": rng.random(self.n_vendors) > 0.03,
        })
        self.vendor_p = _zipf_weights(self.n_vendors, rng)
        self.vendor_currency = self.vendors["country"].map(COUNTRY_CURRENCY).fillna("USD").to_numpy(object)

    def _build_customers(self) -> None:
        rng = np.random.default_rng(self.seed + 3)
        n = self.n_customers
        names = (pd.Series(rng.choice(CUSTOMER_A, n)) + " " + pd.Series(rng.choice(CUSTOMER_B, n)) + " "
                 + pd.Series(rng.choice(CUSTOMER_KIND, n)))
        ids = pd.Series([f"C{i:07d}" for i in range(1, n + 1)])
        regions = rng.choice(list(CUSTOMER_REGIONS), n, p=[0.5, 0.3, 0.15, 0.05])
        countries = np.empty(n, dtype=object)
        for r, options in CUSTOMER_REGIONS.items():
            mask = regions == r
            countries[mask] = rng.choice(options, mask.sum())
        slug = names.str.lower().str.replace(r"[^a-z]+", "", regex=True).str.slice(0, 18)
        created = np.datetime64(self.end_date, "D") - rng.integers(0, 2200, n)
        self.customers = pd.DataFrame({
            "customer_id": ids, "customer_name": names,
            "email": "billing@" + slug + ids.str.slice(-5) + ".com",
            "segment": rng.choice(["Enterprise", "Mid-Market", "SMB"], n, p=[0.1, 0.3, 0.6]),
            "region": regions, "country": countries,
            "created_date": np.datetime_as_string(created),
        })
        test = rng.random(n) < 0.005
        self.customers.loc[test, "customer_name"] = "TEST ACCOUNT " + self.customers.loc[test, "customer_id"]
        self.customers.loc[test, "email"] = "qa+" + self.customers.loc[test, "customer_id"].str.lower() + "@example.com"
        self.customer_p = _zipf_weights(n, rng)
        # duplicate CRM records: same email, new id -- transactions sometimes reference these ids
        dup_src = rng.choice(n, int(n * 0.02), replace=False)
        dups = self.customers.iloc[dup_src].copy()
        dups["customer_id"] = [f"C{i:07d}" for i in range(n + 1, n + 1 + len(dups))]
        dups["email"] = dups["email"].str.upper()
        dups["customer_name"] = dups["customer_name"].str.upper()
        dups["created_date"] = self.end_date.isoformat()
        self.customer_duplicates = dups.reset_index(drop=True)
        self.customer_dup_of = dict(zip(dups["customer_id"], self.customers["customer_id"].iloc[dup_src]))

    def _build_fx(self) -> None:
        rng = np.random.default_rng(self.seed + 4)
        bdays = pd.bdate_range(self.calendar_start, self.end_date)
        all_days = pd.date_range(self.calendar_start, self.end_date + timedelta(days=500))
        published = {}
        truth = {"USD": np.ones(len(all_days))}
        for ccy, (start, vol) in FX_START.items():
            rates = start * np.exp(np.cumsum(rng.normal(0, vol, len(bdays))))
            series = pd.Series(np.round(rates, 6), index=bdays)
            missing = rng.random(len(series)) < 0.01
            nulls = (~missing) & (rng.random(len(series)) < 0.002)
            zeros = (~missing) & (~nulls) & (rng.random(len(series)) < 0.0005)
            raw = series.copy().astype(object)
            raw[nulls] = None
            raw[zeros] = 0.0
            published[ccy] = raw[~missing]
            good = series[~missing & ~nulls & ~zeros]
            truth[ccy] = good.reindex(all_days).ffill().bfill().to_numpy()
        self.fx_published = published
        self.fx_truth = np.vstack([truth[c] for c in CURRENCIES])
        self.fx_days0 = np.datetime64(self.calendar_start, "D")

    def _build_volume_plan(self) -> None:
        weights = {}
        for m in self.months:
            days = (min(_month_end(m), self.end_date) - m).days + 1
            weights[m] = days / _month_end(m).day * self._season(m)
        self.rows_per_unit = self.target_rows / sum(weights.values())
        self.month_rows = {m: int(round(self.rows_per_unit * w)) for m, w in weights.items()}

    def _season(self, m: date) -> float:
        growth = 1.0 + 0.009 * ((m.year - self.history_start.year) * 12 + m.month - self.history_start.month)
        return _MONTH_SEASON[m.month - 1] * growth

    def fx_rate(self, currency_idx: np.ndarray, days: np.ndarray) -> np.ndarray:
        offsets = (days.astype("datetime64[D]") - self.fx_days0).astype(int)
        offsets = np.clip(offsets, 0, self.fx_truth.shape[1] - 1)
        return self.fx_truth[currency_idx, offsets]

    # ------------------------------------------------------------------ masters
    def department_snapshots(self) -> list[tuple[date, pd.DataFrame]]:
        """Three SharePoint snapshots: headcount drift, cost-centre moves, a rename, a closure, a new dept."""
        rng = np.random.default_rng(self.seed + 10)
        base = self.departments.drop(columns=["_rev_share"]).copy()
        snaps = []
        dates = [self.history_start, _add_months(self.history_start, 12), self.end_date]
        current = base.copy()
        for i, d in enumerate(dates):
            if i > 0:
                current = current.copy()
                current["headcount"] = (current["headcount"] * rng.uniform(0.9, 1.15, len(current))).round().astype(int)
                moved = rng.choice(len(current), 3, replace=False)
                current.loc[moved, "cost_center"] = [f"CC-{2001 + i * 10 + k}" for k in range(3)]
            if i == 1:
                current.loc[current["dept_code"] == "CSX-EMEA", "dept_name"] = "Customer Experience - EMEA"
                current = pd.concat([current, pd.DataFrame([{
                    "dept_code": "AIX-NA", "dept_name": "AI Platform - North America", "function": "AIX",
                    "region": "NA", "cost_center": "CC-3001", "headcount": 14, "is_active": True}])],
                    ignore_index=True)
            if i == 2:
                current.loc[current["dept_code"] == "AUD-EMEA", "is_active"] = False
            snap = current.copy()
            if i == 2:  # defects in the latest extract
                snap.loc[snap.sample(2, random_state=1).index, "headcount"] = -1
                snap.loc[snap.sample(1, random_state=2).index, "dept_name"] = None
            snaps.append((d, snap))
        return snaps

    def department_local_export(self) -> pd.DataFrame:
        """An older ERP export with lower precedence: padded codes and stale headcounts."""
        df = self.departments.drop(columns=["_rev_share"]).copy()
        df["dept_code"] = "  " + df["dept_code"].str.lower() + " "
        df["headcount"] = (df["headcount"] * 0.9).round().astype(int)
        return df

    def gl_accounts_raw(self) -> pd.DataFrame:
        rng = np.random.default_rng(self.seed + 11)
        df = self.accounts.copy()
        df["category"] = [CATEGORY_SPELLINGS[c][rng.integers(0, len(CATEGORY_SPELLINGS[c]))] for c in df["category"]]
        df["account_code"] = _vary_codes(df["account_code"].to_numpy(), rng, 0.15)
        dup = df.sample(3, random_state=3)
        return pd.concat([df, dup], ignore_index=True)

    def vendors_raw(self) -> pd.DataFrame:
        """5k vendors plus near-duplicate records (new id, messy name) pointing at their survivor."""
        rng = np.random.default_rng(self.seed + 12)
        df = self.vendors.copy()
        df["merged_into"] = None
        terms = df["payment_terms_days"].astype(str)
        style = rng.integers(0, 3, len(df))
        df["payment_terms"] = np.where(style == 0, "NET" + terms, np.where(style == 1, terms, "Net " + terms + " days"))
        df = df.drop(columns=["payment_terms_days"])
        spell = {c: v[1] for c, v in COUNTRIES.items()}
        df["country"] = _vary_choice(df["country"].to_numpy(), spell, rng, 0.35)
        dup_src = rng.choice(len(df), 100, replace=False)
        dups = df.iloc[dup_src].copy()
        dups["vendor_id"] = [f"V{i:05d}" for i in range(self.n_vendors + 1, self.n_vendors + 1 + len(dups))]
        dups["vendor_name"] = _vary_names(dups["vendor_name"].to_numpy(), rng, 1.0)
        half = len(dups) // 2
        dups.iloc[:half, dups.columns.get_loc("merged_into")] = df["vendor_id"].iloc[dup_src[:half]].to_numpy()
        out = pd.concat([df, dups], ignore_index=True)
        out.loc[rng.random(len(out)) < 0.01, "country"] = None
        return out

    def vendors_local_export(self) -> pd.DataFrame:
        df = self.vendors.sample(400, random_state=5).copy()
        df["vendor_category"] = "Unclassified"
        df["vendor_name"] = df["vendor_name"].str.upper()
        return df[["vendor_id", "vendor_name", "vendor_category", "country"]]

    def customer_snapshots(self) -> list[tuple[date, pd.DataFrame]]:
        """CRM snapshot a year ago and today; ~5% of customers change segment/region between them."""
        rng = np.random.default_rng(self.seed + 13)
        later = pd.concat([self.customers, self.customer_duplicates], ignore_index=True)
        earlier = self.customers.copy()
        changed = rng.random(len(earlier)) < 0.05
        earlier.loc[changed, "segment"] = rng.choice(["Enterprise", "Mid-Market", "SMB"], changed.sum())
        earlier = earlier[pd.to_datetime(earlier["created_date"]) <= pd.Timestamp(_add_months(self.end_date, -12))]
        later = later.copy()
        later.loc[rng.random(len(later)) < 0.01, "segment"] = None
        later.loc[rng.random(len(later)) < 0.01, "email"] = None
        return [(_add_months(_month_start(self.end_date), -12), earlier), (self.end_date, later)]

    def fx_rates_raw(self) -> pd.DataFrame:
        """ECB-style publication (business days, gaps, null/zero rates) plus a bank feed that overlaps it."""
        rng = np.random.default_rng(self.seed + 14)
        frames = []
        for ccy, series in self.fx_published.items():
            frames.append(pd.DataFrame({"currency": ccy, "rate_date": series.index.strftime("%Y-%m-%d"),
                                        "rate_to_usd": series.to_numpy(), "provider": "ecb"}))
            vals = pd.to_numeric(series, errors="coerce")
            sub = vals[(rng.random(len(vals)) < 0.3) & vals.notna() & (vals > 0)]
            frames.append(pd.DataFrame({"currency": ccy.lower() if rng.random() < 0.5 else ccy,
                                        "rate_date": sub.index.strftime("%Y-%m-%d"),
                                        "rate_to_usd": (sub * rng.uniform(0.999, 1.001, len(sub))).round(6).to_numpy(),
                                        "provider": "bank_feed"}))
        df = pd.concat(frames, ignore_index=True)
        df["rate_to_usd"] = df["rate_to_usd"].astype(float)
        return df

    # ------------------------------------------------------------------ transactions
    def transaction_months(self) -> Iterator[tuple[date, dict[str, Any]]]:
        for m in self.months:
            yield m, self.transactions_for_month(m)

    def transactions_for_month(self, m: date) -> dict[str, Any]:
        """One month of ledger activity split across ERP (parquet), POS (CSV) and Kafka (JSONL)."""
        rng = np.random.default_rng([self.seed, m.year, m.month])
        n = self.month_rows[m]
        last_day = min(_month_end(m), self.end_date)
        days = pd.date_range(m, last_day).to_numpy().astype("datetime64[D]")
        dow = (days.astype("datetime64[D]").view("int64") - 4) % 7  # 0 = Monday
        dom = (days - days[0]).astype(int) + 1
        dw = np.where(dow < 5, 1.0, 0.35) * np.where(dom >= 27, 1.35, np.where(dom >= 24, 1.1, 1.0))
        ev_day = days[rng.choice(len(days), n, p=dw / dw.sum())]
        secs = rng.integers(6 * 3600, 22 * 3600, n)
        ingest_ts = ev_day.astype("datetime64[s]") + secs + rng.integers(60, 36 * 3600, n)

        late = _pick(rng, DEFECTS["late_arriving"], n)
        ev_day = ev_day.copy()
        ev_day[late] = np.maximum(ev_day[late] - rng.integers(30, 120, late.sum()), np.datetime64(self.history_start, "D"))
        future = _pick(rng, DEFECTS["future_dated"], n) & ~late
        ev_day[future] += rng.integers(200, 500, future.sum())
        event_ts = ev_day.astype("datetime64[s]") + secs

        kind = rng.choice(np.array(["revenue", "expense", "refund"], dtype=object), n, p=[0.30, 0.68, 0.02])
        is_exp = kind == "expense"
        dept = np.empty(n, int)
        dept[is_exp] = rng.choice(len(self.dept_codes), is_exp.sum(), p=self.dept_expense_p)
        dept[~is_exp] = rng.choice(len(self.dept_codes), (~is_exp).sum(), p=self.dept_revenue_p)
        acct = np.empty(n, int)
        acct[~is_exp] = rng.choice(self.rev_acct_idx, (~is_exp).sum(), p=self.rev_acct_p)
        for d in range(len(self.dept_codes)):
            mask = is_exp & (dept == d)
            if mask.any():
                acct[mask] = rng.choice(self.dept_acct_idx[d], mask.sum(), p=self.dept_acct_p[d])

        amount_usd = self.account_base[acct] * np.exp(rng.normal(-0.18, 0.6, n)) * self._season(m)
        refund = kind == "refund"
        amount_usd[refund] = -amount_usd[refund] * rng.uniform(0.2, 1.0, refund.sum())

        region = self.dept_region[dept]
        ccy = np.empty(n, dtype=object)
        for r, mix in REGION_CURRENCY_MIX.items():
            mask = region == r
            ccy[mask] = rng.choice(list(mix), mask.sum(), p=list(mix.values()))
        ccy_idx = pd.Series(ccy).map({c: i for i, c in enumerate(CURRENCIES)}).to_numpy(int)
        rate = self.fx_rate(ccy_idx, ev_day)
        amount_local = np.round(amount_usd / rate, 2)

        vendor = np.full(n, -1)
        vendor[is_exp] = rng.choice(self.n_vendors, is_exp.sum(), p=self.vendor_p)
        customer = np.full(n, -1)
        customer[~is_exp] = rng.choice(self.n_customers, (~is_exp).sum(), p=self.customer_p)

        ids = np.char.add(f"TX{m:%Y%m}", np.char.zfill(np.arange(n).astype(str), 7)).astype(object)
        ids[_pick(rng, DEFECTS["null_id"], n)] = None

        dept_code = self.dept_codes[dept].copy()
        dept_code[_pick(rng, DEFECTS["orphan_dept"], n)] = rng.choice(["ZZZ-XX", "TMP", "MKT-APAC"])
        blank = _pick(rng, DEFECTS["blank_dept"], n)
        dept_code[blank] = rng.choice(np.array(["", None], dtype=object), blank.sum())
        dept_code = _vary_codes(dept_code, rng, DEFECTS["code_variant"])
        account_code = _vary_codes(self.account_codes[acct], rng, DEFECTS["code_variant"])

        vendor_id = np.where(vendor >= 0, self.vendors["vendor_id"].to_numpy(object)[np.maximum(vendor, 0)], None)
        vendor_name = np.where(vendor >= 0, self.vendors["vendor_name"].to_numpy(object)[np.maximum(vendor, 0)], None)
        vendor_name = _vary_names(vendor_name, rng, DEFECTS["vendor_name_variant"])
        vendor_id[_pick(rng, DEFECTS["vendor_id_missing"], n)] = None
        unreg = is_exp & _pick(rng, DEFECTS["vendor_unregistered"], n)
        vendor_id[unreg] = None
        vendor_name[unreg] = [f"Unregistered Supplier {k}" for k in rng.integers(1, 400, unreg.sum())]

        cust_ids = self.customers["customer_id"].to_numpy(object)
        customer_id = np.where(customer >= 0, cust_ids[np.maximum(customer, 0)], None)
        dup_ids = self.customer_duplicates["customer_id"].to_numpy(object)
        use_dup = (customer >= 0) & _pick(rng, DEFECTS["customer_duplicate_id"], n)
        customer_id[use_dup] = rng.choice(dup_ids, use_dup.sum())
        unknown_c = (customer >= 0) & _pick(rng, DEFECTS["customer_unknown"], n)
        customer_id[unknown_c] = "C9999999"

        amount = amount_local.astype(float)
        amount[_pick(rng, DEFECTS["amount_null"], n)] = np.nan
        amount[_pick(rng, DEFECTS["amount_zero"], n)] = 0.0
        out = _pick(rng, DEFECTS["amount_outlier"], n)
        amount[out] = amount[out] * 1000

        currency = ccy.copy()
        lower = _pick(rng, DEFECTS["currency_lower"], n)
        currency[lower] = np.char.lower(currency[lower].astype(str)).astype(object)
        currency[_pick(rng, DEFECTS["currency_null"], n) & (ccy == "USD")] = None
        currency[_pick(rng, DEFECTS["currency_invalid"], n)] = "XXX"

        txn_type = _vary_choice(kind, TXN_TYPE_SPELLINGS, rng, DEFECTS["txn_type_variant"])
        txn_type[_pick(rng, DEFECTS["txn_type_null"], n)] = None

        df = pd.DataFrame({
            "transaction_id": ids, "event_ts": _iso(event_ts), "dept_code": dept_code,
            "account_code": account_code, "vendor_id": vendor_id, "vendor_name": vendor_name,
            "customer_id": customer_id, "amount": amount, "currency": currency, "txn_type": txn_type,
            "op": "I", "event_version": 1, "ingested_at": _iso(ingest_ts),
        })
        df["_kind"] = kind
        df["_ingest"] = ingest_ts

        # CDC: corrected versions and tombstones arrive later; redelivered duplicates are byte-identical
        upd = np.flatnonzero(_pick(rng, DEFECTS["cdc_update"], n))
        dele = np.setdiff1d(np.flatnonzero(_pick(rng, DEFECTS["cdc_delete"], n)), upd)
        dup = np.flatnonzero(_pick(rng, DEFECTS["exact_duplicate"], n))
        u = df.iloc[upd].copy()
        u["amount"] = (u["amount"] * rng.uniform(0.8, 1.2, len(u))).round(2)
        u["op"], u["event_version"] = "U", 2
        u["_ingest"] = u["_ingest"] + rng.integers(3600, 20 * 86400, len(u)).astype("timedelta64[s]")
        dl = df.iloc[dele].copy()
        dl["op"], dl["event_version"] = "D", 2
        dl["_ingest"] = dl["_ingest"] + rng.integers(3600, 30 * 86400, len(dl)).astype("timedelta64[s]")
        for frame in (u, dl):
            frame["ingested_at"] = _iso(frame["_ingest"].to_numpy())
        df = pd.concat([df, u, dl, df.iloc[dup]], ignore_index=True)

        # routing: POS carries some revenue, Kafka carries the last 30 days, ERP the rest
        n2 = len(df)
        route = np.full(n2, "erp", dtype=object)
        pos = (df["_kind"].to_numpy() != "expense") & (rng.random(n2) < 0.10)
        route[pos] = "pos"
        recent = pd.to_datetime(df["event_ts"].str.slice(0, 10), errors="coerce") >= pd.Timestamp(self.end_date - timedelta(days=30))
        route[(~pos) & recent.to_numpy() & (rng.random(n2) < 0.4)] = "kafka"

        erp = df[route == "erp"].drop(columns=["_kind", "_ingest"]).reset_index(drop=True)
        if (self.end_date - m).days < 190:  # schema drift: newer ERP extracts gained columns
            erp["cost_center"] = None
            erp["memo"] = np.where(rng.random(len(erp)) < 0.02, "manual override", None)
        return {"erp": erp, "pos": self._to_pos(df[route == "pos"], rng),
                "kafka": self._to_kafka_lines(df[route == "kafka"], rng)}

    @staticmethod
    def _to_pos(df: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
        """Legacy point-of-sale CSV: renamed columns, US dates, formatted amount strings."""
        if df.empty:
            return pd.DataFrame()
        amt = df["amount"].to_numpy(float)
        style = rng.integers(0, 10, len(df))
        formatted = np.where(style < 6, [f"{a:,.2f}" for a in amt],
                             np.where(style < 9, [f"${a:,.2f}" for a in amt], [f"{a:.2f}" for a in amt])).astype(object)
        formatted[np.isnan(amt)] = rng.choice(["N/A", ""], int(np.isnan(amt).sum())) if np.isnan(amt).any() else []
        formatted[rng.random(len(df)) < 0.002] = "N/A"
        ts = pd.to_datetime(df["event_ts"], errors="coerce", utc=True)
        kind = df["_kind"].to_numpy()
        ccy = df["currency"].to_numpy(object).copy()
        lower = rng.random(len(df)) < 0.3
        ccy[lower] = [c.lower() if isinstance(c, str) else c for c in ccy[lower]]
        return pd.DataFrame({
            "txn_ref": df["transaction_id"].to_numpy(), "posting_dt": ts.dt.strftime("%m/%d/%Y %H:%M").to_numpy(),
            "store_dept": df["dept_code"].to_numpy(), "gl": df["account_code"].to_numpy(),
            "cust_no": df["customer_id"].to_numpy(), "amt": formatted, "ccy": ccy,
            "type": np.where(kind == "refund", "RETURN", "SALE"), "ingested_at": df["ingested_at"].to_numpy(),
            "record_status": np.where(df["op"].to_numpy() == "D", "VOIDED", "ACTIVE"),
            "rev": df["event_version"].to_numpy(),
        })

    @staticmethod
    def _to_kafka_lines(df: pd.DataFrame, rng: np.random.Generator) -> list[str]:
        """Kafka envelopes: epoch-millis or offset timestamps, schema v1 vs v2 payloads, a few corrupt lines."""
        if df.empty:
            return []
        payload = df.drop(columns=["_kind", "_ingest"]).reset_index(drop=True)
        n = len(payload)
        ts = pd.to_datetime(payload["event_ts"], utc=True)
        style = rng.integers(0, 10, n)
        epoch_ms = ts.dt.as_unit("ms").astype("int64").astype(str)  # serialised as a bare JSON number below
        offset = (ts + pd.Timedelta(hours=5, minutes=30)).dt.strftime("%Y-%m-%dT%H:%M:%S") + "+05:30"
        payload["event_ts"] = np.where(style < 3, "__MS__" + epoch_ms, np.where(style < 6, offset, payload["event_ts"]))
        v1 = rng.random(n) < 0.2
        lines: list[str] = []
        for schema_v, mask in ((1, v1), (2, ~v1)):
            part = payload[mask]
            if part.empty:
                continue
            if schema_v == 1:
                part = part.rename(columns={"amount": "amt"})
            body = part.to_json(orient="records", lines=True).splitlines()
            ids = rng.integers(0, 16 ** 12, len(body), dtype=np.int64)
            head = f'{{"headers": {{"producer": "pos-gateway", "schema_version": {schema_v}, "message_id": "'
            lines += [f'{head}{i:012x}"}}, "payload": {b}}}' for i, b in zip(ids, body)]
        lines = [_EPOCH_MARKER.sub(r"\1", ln) for ln in lines]
        for i in np.flatnonzero(rng.random(len(lines)) < DEFECTS["malformed_json"]):
            lines[i] = lines[i][: len(lines[i]) // 2] if rng.random() < 0.7 else "<<binary frame dropped by connector>>"
        return lines

    def manual_adjustments(self, count: int = 1500) -> pd.DataFrame:
        """Finance-team CSV of journal adjustments; ~5% of rows were uploaded twice."""
        rng = np.random.default_rng(self.seed + 20)
        rev = rng.random(count) < 0.3
        accounts = np.where(rev, rng.choice(list(REVENUE_ACCOUNT_WEIGHTS), count),
                            rng.choice(list(COMMON_EXPENSE_WEIGHTS), count))
        dates = [self.end_date - timedelta(days=int(o)) for o in rng.integers(0, 90, count)]
        df = pd.DataFrame({
            "transaction_id": [f"ADJ{self.end_date:%Y%m}{i:05d}" for i in range(count)],
            "event_date": [d.isoformat() for d in dates],
            "dept_code": np.where(rev, rng.choice(self.dept_codes, count, p=self.dept_revenue_p),
                                  rng.choice(self.dept_codes, count, p=self.dept_expense_p)),
            "account_code": accounts,
            "vendor_name": ["Adjustment - " + r for r in rng.choice(ADJUSTMENT_REASONS, count)],
            "amount_usd": np.round(rng.uniform(500, 18000, count) * np.where(rng.random(count) < 0.1, -1, 1), 2),
            "currency": "USD",
            "txn_type": np.where(rev, "revenue", "expense"),
            "source_channel": "file_upload",
        })
        return pd.concat([df, df.sample(frac=0.05, random_state=7)], ignore_index=True)

    def stream_events(self, days: int, per_day: int) -> list[dict[str, Any]]:
        """Fresh events for the live Kafka topic (same envelope as the replay files, ~2% redelivered)."""
        rng = np.random.default_rng()
        events = []
        for offset in range(days):
            d = self.end_date - timedelta(days=offset)
            for _ in range(per_day):
                exp = rng.random() < 0.7
                di = rng.choice(len(self.dept_codes), p=self.dept_expense_p if exp else self.dept_revenue_p)
                ai = rng.choice(self.dept_acct_idx[di], p=self.dept_acct_p[di]) if exp else rng.choice(self.rev_acct_idx, p=self.rev_acct_p)
                vi = rng.choice(self.n_vendors, p=self.vendor_p) if exp else None
                ci = None if exp else rng.choice(self.n_customers, p=self.customer_p)
                ts = datetime(d.year, d.month, d.day, int(rng.integers(6, 22)), int(rng.integers(0, 60)), tzinfo=timezone.utc)
                payload = {
                    "transaction_id": "KS" + uuid.uuid4().hex[:14], "event_ts": ts.isoformat().replace("+00:00", "Z"),
                    "dept_code": self.dept_codes[di], "account_code": self.account_codes[ai],
                    "vendor_id": None if vi is None else self.vendors["vendor_id"].iat[vi],
                    "vendor_name": None if vi is None else self.vendors["vendor_name"].iat[vi],
                    "customer_id": None if ci is None else self.customers["customer_id"].iat[ci],
                    "amount": round(float(self.account_base[ai] * np.exp(rng.normal(-0.18, 0.6))), 2),
                    "currency": "USD", "txn_type": "expense" if exp else "revenue",
                    "op": "I", "event_version": 1, "ingested_at": datetime.now(timezone.utc).isoformat(),
                }
                events.append({"headers": {"producer": "live-stream", "schema_version": 2,
                                           "message_id": uuid.uuid4().hex[:12]}, "payload": payload})
        redelivered = [events[i] for i in rng.choice(len(events), max(1, len(events) // 50))] if events else []
        return events + redelivered

    # ------------------------------------------------------------------ budgets & AP
    def budget_files(self) -> list[tuple[str, pd.DataFrame]]:
        """Plan files per fiscal year: ORIGINAL, Q2/Q3 reforecasts (different layout), and a resubmission."""
        rng = np.random.default_rng(self.seed + 30)
        expected = self._expected_monthly_by_dept_account()
        first_fy, last_fy = self.history_start.year, _add_months(self.end_date, 12).year
        files = []
        for fy in range(first_fy, last_fy + 1):
            for version, start_month, layout in (("ORIGINAL", 1, "std"), ("REFORECAST_Q2", 4, "alt"), ("REFORECAST_Q3", 7, "alt")):
                issued = date(fy, start_month, 1) - timedelta(days=20)
                if issued > self.end_date:
                    continue
                rows = []
                for month in range(start_month, 13):
                    m = date(fy, month, 1)
                    for (d, a), base in expected.items():
                        # actuals scale with the season in both volume and ticket size
                        amt = base * self._season(m) ** 2 * rng.uniform(0.9, 1.15)
                        rows.append((m, self.dept_codes[d], self.account_codes[a], round(float(amt), 2)))
                df = pd.DataFrame(rows, columns=["month", "dept_code", "account_code", "amount"])
                df["version"] = version
                df["submitted_at"] = f"{issued.isoformat()}T09:00:00Z"
                df = self._budget_defects(df, rng)
                files.append((f"budget_FY{fy}_{version}.csv", self._budget_layout(df, layout)))
                if version == "REFORECAST_Q3":  # finance re-sends a corrected file a week later
                    resub = df.sample(frac=0.1, random_state=fy).copy()
                    resub["amount"] = pd.to_numeric(resub["amount"], errors="coerce") * 1.05
                    resub["submitted_at"] = f"{(issued + timedelta(days=7)).isoformat()}T16:30:00Z"
                    files.append((f"budget_FY{fy}_{version}_resubmitted.csv", self._budget_layout(resub, layout)))
        return files

    def _expected_monthly_by_dept_account(self) -> dict[tuple[int, int], float]:
        mean_mult = float(np.exp(-0.18 + 0.6 ** 2 / 2))
        per_month = self.rows_per_unit
        out: dict[tuple[int, int], float] = {}
        for d in range(len(self.dept_codes)):
            exp_n = per_month * 0.68 * self.dept_expense_p[d]
            for a, p in zip(self.dept_acct_idx[d], self.dept_acct_p[d]):
                out[(d, int(a))] = exp_n * p * self.account_base[a] * mean_mult
            rev_n = per_month * 0.30 * self.dept_revenue_p[d]
            if rev_n > 0:
                for a, p in zip(self.rev_acct_idx, self.rev_acct_p):
                    out[(d, int(a))] = rev_n * p * self.account_base[a] * mean_mult
        return out

    @staticmethod
    def _budget_defects(df: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
        df = df.copy()
        df["amount"] = df["amount"].astype(object)
        n = len(df)
        df.loc[rng.random(n) < 0.002, "amount"] = -1000.0
        df.loc[rng.random(n) < 0.002, "amount"] = None
        df.loc[rng.random(n) < 0.001, "dept_code"] = "MKT-APAC"
        df["dept_code"] = _vary_codes(df["dept_code"].to_numpy(), rng, 0.03)
        return df

    @staticmethod
    def _budget_layout(df: pd.DataFrame, layout: str) -> pd.DataFrame:
        if layout == "std":
            return pd.DataFrame({"year_month": df["month"].map(lambda m: m.strftime("%Y-%m")),
                                 "dept_code": df["dept_code"], "account_code": df["account_code"],
                                 "budget_amount_usd": df["amount"], "version": df["version"],
                                 "submitted_at": df["submitted_at"]})
        amounts = [f"{a:,.2f}" if isinstance(a, (int, float)) and not pd.isna(a) else "" for a in df["amount"]]
        periods = df["month"].map(lambda m: m.strftime("%b-%Y")).to_numpy(object)
        periods[: max(1, len(periods) // 500)] = "13-2025"  # unparseable period from a broken formula
        return pd.DataFrame({"Period": periods, "Dept Code": df["dept_code"], "GL Account": df["account_code"],
                             "Amount (USD)": amounts, "Plan Version": df["version"],
                             "Submitted At": df["submitted_at"]})

    def ap_invoice_months(self) -> Iterator[tuple[date, pd.DataFrame]]:
        """AP sub-ledger: invoices, credit notes, status changes as CDC, duplicate billing, bad due dates."""
        per_month = int(round(150_000 / len(self.months)))
        vendor_ids = self.vendors["vendor_id"].to_numpy(object)
        terms = self.vendors["payment_terms_days"].to_numpy(int)
        for m in self.months:
            rng = np.random.default_rng([self.seed, 77, m.year, m.month])
            last_day = min(_month_end(m), self.end_date)
            days = pd.date_range(m, last_day).to_numpy().astype("datetime64[D]")
            n = int(per_month * len(days) / _month_end(m).day)
            v = rng.choice(self.n_vendors, n, p=self.vendor_p)
            inv_date = days[rng.integers(0, len(days), n)]
            due = inv_date + terms[v]
            ccy = self.vendor_currency[v]
            ccy_idx = np.array([CURRENCIES.index(c) for c in ccy])
            usd = np.exp(rng.normal(np.log(3800), 0.9, n))
            credit = rng.random(n) < 0.01
            usd[credit] = -usd[credit] * 0.3
            amount = np.round(usd / self.fx_rate(ccy_idx, inv_date), 2)

            today = np.datetime64(self.end_date, "D")
            pay_delay = rng.normal(0, 9, n).astype(int)
            paid_on = due + pay_delay
            overdue_days = np.maximum((today - due).astype(int), 0)
            still_open = rng.random(n) < 0.3 * np.exp(-overdue_days / 30)  # stragglers get chased and paid
            is_paid = (paid_on <= today) & ~still_open
            status = np.where(is_paid, "PAID", "OPEN").astype(object)
            partial = (~is_paid) & (rng.random(n) < 0.08)
            status[partial] = "PARTIAL"
            void = rng.random(n) < 0.01
            status[void] = "VOID"
            paid_amount = np.where(status == "PAID", amount, np.where(status == "PARTIAL", np.round(amount * 0.4, 2), 0.0))
            over = (status == "PAID") & (rng.random(n) < 0.0005)
            paid_amount[over] = np.round(paid_amount[over] * 1.1, 2)

            ids = np.char.add(f"AP{m:%Y%m}", np.char.zfill(np.arange(n).astype(str), 6)).astype(object)
            df = pd.DataFrame({
                "invoice_id": ids,
                "invoice_number": [f"{vendor_ids[x][-3:]}-{rng.integers(10000, 99999)}" for x in v],
                "vendor_id": vendor_ids[v],
                "invoice_date": np.datetime_as_string(inv_date).astype(object),
                "due_date": np.datetime_as_string(due).astype(object),
                "doc_type": np.where(credit, "CREDIT_NOTE", "INVOICE"),
                "amount": amount, "currency": ccy,
                "status": _vary_choice(status, AP_STATUS_SPELLINGS, rng, 0.2),
                "paid_amount": paid_amount,
                "paid_date": np.where(np.isin(status, ["PAID", "PARTIAL"]), np.datetime_as_string(np.minimum(paid_on, today)), None),
                "ingested_at": f"{last_day.isoformat()}T23:00:00Z",
            })
            bad_due = rng.random(n) < 0.001
            df.loc[bad_due, "due_date"] = np.datetime_as_string(inv_date[bad_due] - 5)
            df.loc[rng.random(n) < 0.001, "vendor_id"] = None
            dup = df.sample(frac=0.005, random_state=m.month).copy()  # vendor billed twice
            dup["invoice_id"] = dup["invoice_id"] + "D"
            # earlier OPEN version of paid invoices, as the sub-ledger emitted it at invoice time
            opened = df[(status == "PAID") & (rng.random(n) < 0.3)].copy()
            opened["status"], opened["paid_amount"], opened["paid_date"] = "OPEN", 0.0, None
            opened["ingested_at"] = opened["invoice_date"] + "T23:00:00Z"
            yield m, pd.concat([df, dup, opened], ignore_index=True)
