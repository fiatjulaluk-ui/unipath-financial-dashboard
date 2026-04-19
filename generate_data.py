#!/usr/bin/env python3
"""
UniPath Financial Data Generator
==================================
Generates synthetic but realistic financial data for UniPath Pty Ltd.
Covers FY2026 (Jul 2025 – Jun 2026), with data through March 2026 (Q3).

Run this script once before launching the Streamlit dashboard:
    python generate_data.py
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import date, timedelta
import random
import os

random.seed(42)
np.random.seed(42)

# ─────────────────────────────────────────────────────────────────────────────
# TAX COMPLIANCE CONFIGURATION
# Change rates here — cascades through all queries and reports automatically.
# ─────────────────────────────────────────────────────────────────────────────

_INDIVIDUAL_TOP_RATE = 0.45    # s12-5 ITAA 1997: top marginal income tax rate
_MEDICARE_LEVY       = 0.02    # s8 Medicare Levy Act 1986
_GST_RATE            = 0.10    # s9-70 GSTA 1999

_FBT_RATE     = _INDIVIDUAL_TOP_RATE + _MEDICARE_LEVY
# Type 2: 1 / (1 - FBT_RATE)
_GROSS_UP_T2  = round(1 / (1 - _FBT_RATE), 4)
# Type 1: (1 + GST_RATE) / (1 - FBT_RATE)
# Algebraic result: 1.10 / 0.53 = 2.0755.
# ATO publishes 2.0802 for FY2026 — use ATO-authoritative value directly.
_GROSS_UP_T1  = 2.0802

TAX_CONFIG = {
    "SGC_RATE":                      0.12,               # Superannuation Guarantee (12% from 1 Jul 2025)
    "VIC_PAYROLL_TAX_RATE":          0.0485,             # Victorian payroll tax rate (4.85%)
    "VIC_PAYROLL_TAX_THRESHOLD_ANN": 1_000_000,          # Annual wage threshold (VIC — increased to $1M from 1 Jul 2025)
    "GST_RATE":                      _GST_RATE,          # Goods & Services Tax (10%)
    "INDIVIDUAL_TOP_RATE":           _INDIVIDUAL_TOP_RATE,  # Top marginal income tax rate (s12-5 ITAA 1997)
    "MEDICARE_LEVY":                 _MEDICARE_LEVY,     # Medicare Levy (s8 Medicare Levy Act 1986)
    "FBT_RATE":                      _FBT_RATE,          # FBT rate = top marginal + Medicare levy
    "FBT_GROSS_UP_TYPE1":            _GROSS_UP_T1,       # ATO-published FY2026; formula gives 2.0755
    "FBT_GROSS_UP_TYPE2":            _GROSS_UP_T2,       # Derived: 1 / (1 - FBT)
    # ── EV Home Charging Rate — PCG 2024/2 ──────────────────────────────────
    # ATO-published rate for calculating the electricity cost component of running
    # a zero-emission vehicle under the Operating Cost Method (alternative to stat formula).
    # Updated annually; FY2027: 5.47c/km — confirmed per PCG 2024/2 update.
    "EV_HOME_CHARGING_C_PER_KM":     4.20,               # cents/km — PCG 2024/2, FY2026
    # ── LCT Thresholds — indexed annually (1 July each year) ────────────────
    # FY2027 projections: ICE ~$82,000+, FEV ~$92,000–$95,000 (CPI March quarter).
    # Update these each May when ATO publishes confirmed thresholds.
    "LCT_THRESHOLD_ICE":             80_567,             # Standard vehicles (LCTA 1999)
    "LCT_THRESHOLD_FEV":             91_387,             # Fuel-efficient ≤3.5L/100km
    "LCT_RATE":                      0.33,               # 33% — s9 LCTA 1999 (not indexed)
    # ── Corporate Income Tax ────────────────────────────────────────────────
    # s66-5 ITAA 1997: standard company rate 30%.
    # Base Rate Entity (BRE) rate 25% applies if aggregated turnover < $50M and
    # passive income ≤ 80% of assessable income. UniPath is a wholly-owned
    # subsidiary — aggregated group turnover likely exceeds $50M → 30% applied.
    # Deferred tax (timing differences on depreciation, provisions) is recognised
    # separately; this rate drives the current-period tax provision only.
    "CORP_TAX_RATE":                 0.30,               # s66-5 ITAA 1997 — standard rate
}

# ─────────────────────────────────────────────────────────────────────────────
# GST SUPPLY TYPE REFERENCE
# Covers both AR (output tax) and AP (input tax credits) sides.
# Used by gst_transactions, accounts_receivable, accounts_payable and bas_returns.
# ─────────────────────────────────────────────────────────────────────────────

GST_SUPPLY_TYPES = [
    # (code, description, applies_to, gst_rate, bas_field, itc_claimable, legislative_ref)
    # ── AR side (Output Tax) ─────────────────────────────────────────────────
    ("TAXABLE",        "Taxable Supply",               "AR", 0.10, "G1",  False, "s9-5 GSTA 1999"),
    ("EXPORT",         "GST-Free Export",              "AR", 0.00, "G3",  False, "s38-185 GSTA 1999"),
    ("GST_FREE_SALE",  "GST-Free Domestic Supply",     "AR", 0.00, "G3",  False, "s38-1 GSTA 1999"),
    # ── AP side (Input Tax Credits) ──────────────────────────────────────────
    ("TAXABLE_PURCH",  "Creditable Acquisition",       "AP", 0.10, "G10", True,  "s11-5 GSTA 1999"),
    ("GST_FREE_PURCH", "GST-Free Acquisition",         "AP", 0.00, "G14", False, "s38-1 GSTA 1999"),
    ("INPUT_TAXED",    "Input-Taxed Acquisition",      "AP", 0.00, "G15", False, "s11-15 GSTA 1999"),
]

# AP supply-type weights by supplier category
AP_SUPPLY_WEIGHTS = {
    "Technology":        [("TAXABLE_PURCH", 80), ("GST_FREE_PURCH", 15), ("INPUT_TAXED",  5)],
    "Facilities":        [("TAXABLE_PURCH", 88), ("GST_FREE_PURCH", 12), ("INPUT_TAXED",  0)],
    "Professional Svcs": [("TAXABLE_PURCH", 68), ("INPUT_TAXED",    22), ("GST_FREE_PURCH", 10)],
    "Marketing":         [("TAXABLE_PURCH", 80), ("GST_FREE_PURCH", 20), ("INPUT_TAXED",  0)],
    "Utilities":         [("TAXABLE_PURCH", 92), ("INPUT_TAXED",     8), ("GST_FREE_PURCH",  0)],
}

DB_PATH = os.path.join(os.path.dirname(__file__), "data", "rmit_finance.db")
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")

# ─────────────────────────────────────────────────────────────────────────────
# REFERENCE DATA
# ─────────────────────────────────────────────────────────────────────────────

ACCOUNTS = [
    # (code, name, type, section, normal_balance)
    ("1001", "Cash at Bank – Operating",              "Asset",     "Current Assets",            "Debit"),
    ("1002", "Cash at Bank – Term Deposit",            "Asset",     "Current Assets",            "Debit"),
    ("1100", "Accounts Receivable – Domestic",         "Asset",     "Current Assets",            "Debit"),
    ("1101", "Accounts Receivable – International",    "Asset",     "Current Assets",            "Debit"),
    ("1102", "Allowance for Doubtful Debts",           "Asset",     "Current Assets",            "Credit"),
    ("1200", "Prepaid Expenses",                       "Asset",     "Current Assets",            "Debit"),
    ("1201", "GST Receivable",                         "Asset",     "Current Assets",            "Debit"),
    ("1300", "Property, Plant & Equipment",            "Asset",     "Non-Current Assets",        "Debit"),
    ("1301", "Accum. Depreciation – PP&E",             "Asset",     "Non-Current Assets",        "Credit"),
    ("1400", "Right-of-Use Assets",                    "Asset",     "Non-Current Assets",        "Debit"),
    ("1401", "Accum. Depreciation – ROU",              "Asset",     "Non-Current Assets",        "Credit"),
    ("2001", "Accounts Payable",                       "Liability", "Current Liabilities",       "Credit"),
    ("2100", "Accrued Liabilities",                    "Liability", "Current Liabilities",       "Credit"),
    ("2101", "Accrued Payroll",                        "Liability", "Current Liabilities",       "Credit"),
    ("2200", "GST Payable",                            "Liability", "Current Liabilities",       "Credit"),
    ("2201", "Payroll Tax Payable",                    "Liability", "Current Liabilities",       "Credit"),
    ("2202", "FBT Payable",                            "Liability", "Current Liabilities",       "Credit"),
    ("2203", "Income Tax Payable",                     "Liability", "Current Liabilities",       "Credit"),
    ("2300", "Deferred Revenue",                       "Liability", "Current Liabilities",       "Credit"),
    ("2400", "Intercompany Payable – Parent University", "Liability", "Current Liabilities",       "Credit"),
    ("2500", "Lease Liabilities",                      "Liability", "Non-Current Liabilities",   "Credit"),
    ("3001", "Retained Earnings",                      "Equity",    "Equity",                    "Credit"),
    ("3002", "Current Year Earnings",                  "Equity",    "Equity",                    "Credit"),
    ("4001", "Course Fees – Domestic",                 "Revenue",   "Revenue",                   "Credit"),
    ("4002", "Course Fees – International",            "Revenue",   "Revenue",                   "Credit"),
    ("4003", "Consulting & Advisory Revenue",          "Revenue",   "Revenue",                   "Credit"),
    ("4004", "Government Grants",                      "Revenue",   "Revenue",                   "Credit"),
    ("4005", "Other Income",                           "Revenue",   "Revenue",                   "Credit"),
    ("5001", "Salaries & Wages",                       "Expense",   "Employee Benefits",         "Debit"),
    ("5002", "Payroll Tax",                            "Expense",   "Employee Benefits",         "Debit"),
    ("5003", "Superannuation",                         "Expense",   "Employee Benefits",         "Debit"),
    ("5100", "Depreciation – PP&E",                    "Expense",   "Depreciation",              "Debit"),
    ("5101", "Depreciation – ROU Assets",              "Expense",   "Depreciation",              "Debit"),
    ("5200", "Occupancy Expenses",                     "Expense",   "Operating Expenses",        "Debit"),
    ("5201", "IT & Technology",                        "Expense",   "Operating Expenses",        "Debit"),
    ("5202", "Marketing & Communications",             "Expense",   "Operating Expenses",        "Debit"),
    ("5203", "Professional Services",                  "Expense",   "Operating Expenses",        "Debit"),
    ("5204", "Travel & Entertainment",                 "Expense",   "Operating Expenses",        "Debit"),
    ("5205", "FBT Expense",                            "Expense",   "Operating Expenses",        "Debit"),
    ("5300", "Intercompany Charges – Parent University", "Expense",   "Intercompany",              "Debit"),
    ("5400", "Income Tax Expense",                     "Expense",   "Tax",                       "Debit"),
]

COST_CENTRES = {
    "CC001": "Academic Programs",
    "CC002": "Student Services",
    "CC003": "Finance & Administration",
    "CC004": "IT & Digital Services",
    "CC005": "Marketing & Communications",
    "CC006": "Facilities Management",
    "CC007": "Executive Office",
}

CUSTOMERS = [
    ("CUST001", "Melbourne City Council",           "Government",   "Domestic"),
    ("CUST002", "Deloitte Australia",               "Corporate",    "Domestic"),
    ("CUST003", "ANZ Banking Group",                "Corporate",    "Domestic"),
    ("CUST004", "Dept. of Education Victoria",      "Government",   "Domestic"),
    ("CUST005", "National Australia Bank",          "Corporate",    "Domestic"),
    ("CUST006", "University of Melbourne",          "Education",    "Domestic"),
    ("CUST007", "PwC Australia",                    "Corporate",    "Domestic"),
    ("CUST008", "State Government of Victoria",     "Government",   "Domestic"),
    ("CUST009", "China Education Group",            "Education",    "International"),
    ("CUST010", "Vietnam National University",      "Education",    "International"),
    ("CUST011", "Singapore Management University",  "Education",    "International"),
    ("CUST012", "India Skills Foundation",          "Education",    "International"),
    ("CUST013", "BHP Group",                        "Corporate",    "Domestic"),
    ("CUST014", "Telstra Corporation",              "Corporate",    "Domestic"),
    ("CUST015", "City of Yarra",                    "Government",   "Domestic"),
]

SUPPLIERS = [
    # (supplier_id, name, type, payment_terms_days, expense_account, cost_centre)
    ("SUPP001", "Microsoft Australia",        "Technology",         60, "5201", "CC004"),
    ("SUPP002", "CBRE Group",                 "Facilities",         30, "5200", "CC006"),
    ("SUPP003", "PwC Australia",              "Professional Svcs",  30, "5203", "CC003"),
    ("SUPP004", "Telstra Enterprise",          "Technology",         30, "5201", "CC004"),
    ("SUPP005", "ISS Facility Services",       "Facilities",         30, "5200", "CC006"),
    ("SUPP006", "Ogilvy Australia",            "Marketing",          45, "5202", "CC005"),
    ("SUPP007", "Herbert Smith Freehills",     "Professional Svcs",  30, "5203", "CC003"),
    ("SUPP008", "Johnson Controls",            "Facilities",         30, "5200", "CC006"),
    ("SUPP009", "Oracle Corporation",          "Technology",         60, "5201", "CC004"),
    ("SUPP010", "KPMG Australia",              "Professional Svcs",  30, "5203", "CC003"),
    ("SUPP011", "AGL Energy",                  "Utilities",          30, "5200", "CC006"),
    ("SUPP012", "DXC Technology",             "Technology",         45, "5201", "CC004"),
    ("SUPP013", "Minter Ellison",              "Professional Svcs",  30, "5203", "CC003"),
    ("SUPP014", "Salesforce Australia",        "Technology",         30, "5201", "CC004"),
    ("SUPP015", "JLL Australia",               "Facilities",         45, "5200", "CC006"),
]

FIXED_ASSETS = [
    # (asset_id, name, category, cost, useful_life_yrs, purchase_date, method)
    ("FA001", "Leasehold Improvements – Level 5",  "Leasehold Improvements", 450_000, 10, date(2021, 7, 1),  "Straight-Line"),
    ("FA002", "Leasehold Improvements – Level 6",  "Leasehold Improvements", 320_000, 10, date(2022, 1, 15), "Straight-Line"),
    ("FA003", "Server Infrastructure",             "IT Equipment",           280_000, 5,  date(2022, 3, 1),  "Straight-Line"),
    ("FA004", "Workstations & Laptops (Batch A)",  "IT Equipment",            95_000, 3,  date(2023, 7, 1),  "Straight-Line"),
    ("FA005", "Workstations & Laptops (Batch B)",  "IT Equipment",            88_000, 3,  date(2024, 7, 1),  "Straight-Line"),
    ("FA006", "AV & Conference Equipment",         "Furniture & Fittings",    62_000, 7,  date(2022, 9, 1),  "Straight-Line"),
    ("FA007", "Office Furniture – Level 5",        "Furniture & Fittings",    48_000, 10, date(2021, 7, 1),  "Straight-Line"),
    ("FA008", "Office Furniture – Level 6",        "Furniture & Fittings",    35_000, 10, date(2022, 1, 15), "Straight-Line"),
    ("FA009", "Learning Management System",        "Software",               150_000, 5,  date(2023, 1, 1),  "Straight-Line"),
    ("FA010", "Fleet Vehicle – Toyota HiLux",      "Motor Vehicles",          55_000, 5,  date(2023, 7, 1),  "Straight-Line"),
]

# ─────────────────────────────────────────────────────────────────────────────
# DATE HELPERS
# ─────────────────────────────────────────────────────────────────────────────

FY_START = date(2025, 7, 1)
CURRENT_DATE = date(2026, 3, 31)


def get_fy_months():
    months = []
    d = FY_START
    while d <= CURRENT_DATE:
        months.append((d.year, d.month))
        d = date(d.year + (d.month // 12), (d.month % 12) + 1, 1)
    return months


def month_end(year, month):
    if month == 12:
        return date(year + 1, 1, 1) - timedelta(days=1)
    return date(year, month + 1, 1) - timedelta(days=1)


def rand_date(year, month):
    start = date(year, month, 1)
    end = month_end(year, month)
    days_range = (end - start).days
    d = start + timedelta(days=random.randint(0, days_range))
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


FY_MONTHS = get_fy_months()

# ─────────────────────────────────────────────────────────────────────────────
# TABLE GENERATORS
# ─────────────────────────────────────────────────────────────────────────────

def gen_chart_of_accounts():
    rows = []
    for code, name, acct_type, section, normal_bal in ACCOUNTS:
        rows.append({
            "account_code": code,
            "account_name": name,
            "account_type": acct_type,
            "report_section": section,
            "normal_balance": normal_bal,
            "is_active": 1,
        })
    return pd.DataFrame(rows)


def gen_cost_centres():
    rows = [{"cost_centre_code": k, "cost_centre_name": v} for k, v in COST_CENTRES.items()]
    return pd.DataFrame(rows)


def gen_gst_supply_types():
    """Reference table: all GST supply/acquisition classifications for AR and AP."""
    rows = []
    for code, desc, applies_to, gst_rate, bas_field, itc_claimable, leg_ref in GST_SUPPLY_TYPES:
        rows.append({
            "supply_type_code": code,
            "description":      desc,
            "applies_to":       applies_to,
            "gst_rate":         gst_rate,
            "gst_rate_pct":     round(gst_rate * 100, 1),
            "bas_field":        bas_field,
            "itc_claimable":    int(itc_claimable),
            "legislative_ref":  leg_ref,
        })
    return pd.DataFrame(rows)


def gen_customers():
    rows = []
    for cid, name, ctype, region in CUSTOMERS:
        rows.append({
            "customer_id": cid,
            "customer_name": name,
            "customer_type": ctype,
            "region": region,
            "payment_terms_days": 30 if region == "Domestic" else 45,
        })
    return pd.DataFrame(rows)


def gen_general_ledger():
    """Generate month-end journals for all P&L and some BS movements."""
    rows = []
    je_num = 1000

    # Monthly revenue profile (seasonality: higher in Jul-Oct, Jan-Mar)
    rev_weights = {7: 1.2, 8: 1.1, 9: 1.3, 10: 1.0, 11: 0.9, 12: 0.7,
                   1: 1.1, 2: 1.0, 3: 1.2, 4: 1.0, 5: 0.9, 6: 0.8}

    for year, month in FY_MONTHS:
        w = rev_weights.get(month, 1.0)
        me = month_end(year, month)
        period = f"{year}-{month:02d}"

        # ── Revenue journals ────────────────────────────────────────────────
        revenue_items = [
            ("4001", "Course Fees – Domestic",       round(random.gauss(480_000 * w, 25_000), 2), "CC001"),
            ("4002", "Course Fees – International",  round(random.gauss(310_000 * w, 20_000), 2), "CC001"),
            ("4003", "Consulting & Advisory Revenue",round(random.gauss(95_000 * w,  10_000), 2), "CC003"),
            ("4004", "Government Grants",             round(random.gauss(75_000,       5_000), 2), "CC007"),
            ("4005", "Other Income",                  round(random.gauss(12_000,        2_000), 2), "CC003"),
        ]
        for acct, desc, amt, cc in revenue_items:
            amt = abs(amt)
            je_num += 1
            jeid = f"JE{je_num:05d}"
            # Dr Cash/AR, Cr Revenue
            rows.append({
                "journal_id": jeid, "period": period, "journal_date": me,
                "account_code": "1001", "cost_centre": cc,
                "description": f"Revenue recognition – {desc}",
                "debit": amt, "credit": 0.0,
                "journal_type": "Revenue", "posted_by": "System",
            })
            rows.append({
                "journal_id": jeid, "period": period, "journal_date": me,
                "account_code": acct, "cost_centre": cc,
                "description": f"Revenue recognition – {desc}",
                "debit": 0.0, "credit": amt,
                "journal_type": "Revenue", "posted_by": "System",
            })

        # ── Payroll journals ────────────────────────────────────────────────
        gross_wages = round(random.gauss(620_000, 18_000), 2)
        super_amt   = round(gross_wages * TAX_CONFIG["SGC_RATE"], 2)
        ptax_amt    = round(gross_wages * TAX_CONFIG["VIC_PAYROLL_TAX_RATE"], 2)

        for acct, amt, desc in [
            ("5001", gross_wages, "Salaries & wages – monthly payroll"),
            ("5003", super_amt,   "Superannuation – monthly payroll"),
            ("5002", ptax_amt,    "Payroll tax – VIC"),
        ]:
            je_num += 1
            jeid = f"JE{je_num:05d}"
            rows.append({
                "journal_id": jeid, "period": period, "journal_date": me,
                "account_code": acct, "cost_centre": "CC003",
                "description": desc, "debit": amt, "credit": 0.0,
                "journal_type": "Payroll", "posted_by": "Finance_Team",
            })
            rows.append({
                "journal_id": jeid, "period": period, "journal_date": me,
                "account_code": "2101", "cost_centre": "CC003",
                "description": desc, "debit": 0.0, "credit": amt,
                "journal_type": "Payroll", "posted_by": "Finance_Team",
            })

        # ── Operating expense journals ───────────────────────────────────────
        opex_items = [
            ("5200", "CC006", "Occupancy expenses",           round(random.gauss(65_000, 3_000), 2)),
            ("5201", "CC004", "IT & technology costs",        round(random.gauss(42_000, 4_000), 2)),
            ("5202", "CC005", "Marketing & communications",   round(random.gauss(28_000, 5_000), 2)),
            ("5203", "CC003", "Professional services",        round(random.gauss(35_000, 8_000), 2)),
            ("5204", "CC007", "Travel & entertainment",       round(random.gauss(12_000, 2_000), 2)),
        ]
        for acct, cc, desc, amt in opex_items:
            amt = abs(amt)
            je_num += 1
            jeid = f"JE{je_num:05d}"
            rows.append({
                "journal_id": jeid, "period": period, "journal_date": me,
                "account_code": acct, "cost_centre": cc,
                "description": f"{desc} – {period}",
                "debit": amt, "credit": 0.0,
                "journal_type": "Expense", "posted_by": "Finance_Team",
            })
            rows.append({
                "journal_id": jeid, "period": period, "journal_date": me,
                "account_code": "2001", "cost_centre": cc,
                "description": f"{desc} – {period}",
                "debit": 0.0, "credit": amt,
                "journal_type": "Expense", "posted_by": "Finance_Team",
            })

        # ── Intercompany charge ──────────────────────────────────────────────
        ic_amt = round(random.gauss(85_000, 5_000), 2)
        je_num += 1
        jeid = f"JE{je_num:05d}"
        for line in [
            ("5300", ic_amt, 0.0),
            ("2400", 0.0, ic_amt),
        ]:
            rows.append({
                "journal_id": jeid, "period": period, "journal_date": me,
                "account_code": line[0], "cost_centre": "CC003",
                "description": "Intercompany charge – Parent University shared services",
                "debit": line[1], "credit": line[2],
                "journal_type": "Intercompany", "posted_by": "Finance_Team",
            })

    df = pd.DataFrame(rows)
    df["journal_date"] = df["journal_date"].astype(str)
    return df


def gen_accounts_receivable():
    GST_RATE = TAX_CONFIG["GST_RATE"]

    # AR supply type weights by customer type/region
    # International → always EXPORT (s38-185, offshore education services)
    # Domestic Education → mix of GST-free and taxable education programs
    # Domestic Government → mostly taxable, some GST-free grants/education
    # Domestic Corporate → almost all taxable consulting/training
    AR_SUPPLY_WEIGHTS = {
        "International": [("EXPORT",         100), ("TAXABLE",   0), ("GST_FREE_SALE",  0)],
        "Education":     [("GST_FREE_SALE",    55), ("TAXABLE",  40), ("EXPORT",         5)],
        "Government":    [("TAXABLE",          65), ("GST_FREE_SALE", 30), ("EXPORT",    5)],
        "Corporate":     [("TAXABLE",          92), ("GST_FREE_SALE",  8), ("EXPORT",    0)],
    }

    rows = []
    inv_num = 10000
    for year, month in FY_MONTHS:
        # 8-14 invoices per month
        for _ in range(random.randint(8, 14)):
            inv_num += 1
            cust = random.choice(CUSTOMERS)
            cid, cname, ctype, region = cust
            inv_date = rand_date(year, month)
            terms = 30 if region == "Domestic" else 45
            due_date = inv_date + timedelta(days=terms)
            amount = round(random.uniform(8_000, 85_000), 2)

            # Assign supply type
            key = region if region == "International" else ctype
            weights_list = AR_SUPPLY_WEIGHTS.get(key, AR_SUPPLY_WEIGHTS["Corporate"])
            codes    = [w[0] for w in weights_list]
            weights  = [w[1] for w in weights_list]
            supply_type = random.choices(codes, weights=weights)[0]

            gst_rate_val = GST_RATE if supply_type == "TAXABLE" else 0.0
            gst_amt      = round(amount * gst_rate_val, 2)
            total_inc_gst = amount + gst_amt

            # Payment status based on age
            days_outstanding = (CURRENT_DATE - inv_date).days
            if days_outstanding > 90:
                status = random.choices(["Paid", "Overdue"], weights=[0.65, 0.35])[0]
            elif days_outstanding > 60:
                status = random.choices(["Paid", "Overdue", "Outstanding"], weights=[0.55, 0.25, 0.20])[0]
            elif days_outstanding > 30:
                status = random.choices(["Paid", "Outstanding"], weights=[0.70, 0.30])[0]
            else:
                status = random.choices(["Paid", "Outstanding"], weights=[0.40, 0.60])[0]

            paid_date = None
            paid_amount = 0.0
            if status == "Paid":
                paid_date = inv_date + timedelta(days=random.randint(5, min(terms + 20, days_outstanding)))
                paid_amount = total_inc_gst

            acct_code = "1100" if region == "Domestic" else "1101"
            rows.append({
                "invoice_number":    f"INV{inv_num:05d}",
                "customer_id":       cid,
                "customer_name":     cname,
                "customer_type":     ctype,
                "region":            region,
                "supply_type_code":  supply_type,
                "invoice_date":      str(inv_date),
                "due_date":          str(due_date),
                "period":            f"{year}-{month:02d}",
                "amount_excl_gst":   amount,
                "gst_amount":        gst_amt,
                "total_inc_gst":     total_inc_gst,
                "status":            status,
                "paid_date":         str(paid_date) if paid_date else None,
                "paid_amount":       paid_amount,
                "account_code":      acct_code,
                "days_outstanding":  days_outstanding if status != "Paid" else 0,
            })
    return pd.DataFrame(rows)


def gen_bank_transactions():
    rows = []
    txn_num = 5000
    opening_balance = 2_450_000.0
    running_balance = opening_balance

    for year, month in FY_MONTHS:
        # 25-40 bank transactions per month
        num_txns = random.randint(25, 40)
        for i in range(num_txns):
            txn_num += 1
            txn_date = rand_date(year, month)
            txn_type = random.choices(
                ["Receipt", "Payment", "Bank Fee", "Interest"],
                weights=[0.45, 0.45, 0.07, 0.03]
            )[0]
            if txn_type == "Receipt":
                amount = round(random.uniform(10_000, 120_000), 2)
                credit = amount; debit = 0.0
                desc = f"Receipt – {random.choice([c[1] for c in CUSTOMERS])}"
                gl_matched = random.random() > 0.08   # 8% timing differences
            elif txn_type == "Payment":
                amount = round(random.uniform(5_000, 95_000), 2)
                debit = amount; credit = 0.0
                desc = f"Payment – {random.choice(['Rent', 'IT Licence', 'Professional Fee', 'Payroll', 'ATO BAS', 'Payroll Tax SRO'])}"
                gl_matched = random.random() > 0.06
            elif txn_type == "Bank Fee":
                amount = round(random.uniform(50, 350), 2)
                debit = amount; credit = 0.0
                desc = "Bank service fee"
                gl_matched = random.random() > 0.15
            else:
                amount = round(random.uniform(200, 1_800), 2)
                credit = amount; debit = 0.0
                desc = "Interest earned – operating account"
                gl_matched = random.random() > 0.10

            running_balance += (credit - debit)
            rows.append({
                "transaction_id":  f"BNK{txn_num:05d}",
                "transaction_date": str(txn_date),
                "period":          f"{year}-{month:02d}",
                "description":     desc,
                "transaction_type":txn_type,
                "debit":           debit,
                "credit":          credit,
                "balance":         round(running_balance, 2),
                "gl_matched":      int(gl_matched),
                "reconciled":      int(gl_matched),
            })
    return pd.DataFrame(rows)


def gen_fixed_assets():
    rows = []
    for asset_id, name, category, cost, life, purchase_date, method in FIXED_ASSETS:
        annual_dep = round(cost / life, 2)
        monthly_dep = round(annual_dep / 12, 2)

        # Accumulated depreciation to FY start
        months_to_fy_start = (
            (FY_START.year - purchase_date.year) * 12
            + (FY_START.month - purchase_date.month)
        )
        months_to_fy_start = max(months_to_fy_start, 0)
        accum_dep_opening = round(monthly_dep * months_to_fy_start, 2)
        accum_dep_opening = min(accum_dep_opening, cost)

        fully_depreciated = accum_dep_opening >= cost
        rows.append({
            "asset_id":            asset_id,
            "asset_name":          name,
            "category":            category,
            "cost":                cost,
            "useful_life_years":   life,
            "depreciation_method": method,
            "purchase_date":       str(purchase_date),
            "annual_depreciation": annual_dep,
            "monthly_depreciation":monthly_dep,
            "accum_dep_opening":   accum_dep_opening,
            "net_book_value_open": round(cost - accum_dep_opening, 2),
            "is_fully_depreciated":int(fully_depreciated),
            "status":              "Disposed" if asset_id == "FA003" and months_to_fy_start > 48 else "Active",
        })
    return pd.DataFrame(rows)


def gen_depreciation_schedule(fa_df):
    rows = []
    for _, asset in fa_df.iterrows():
        if asset["status"] == "Disposed":
            continue
        accum = asset["accum_dep_opening"]
        for year, month in FY_MONTHS:
            monthly_dep = asset["monthly_depreciation"]
            if accum >= asset["cost"]:
                monthly_dep = 0.0
            actual_dep = min(monthly_dep, max(asset["cost"] - accum, 0))
            accum = round(accum + actual_dep, 2)
            nbv = round(asset["cost"] - accum, 2)
            rows.append({
                "asset_id":        asset["asset_id"],
                "asset_name":      asset["asset_name"],
                "category":        asset["category"],
                "period":          f"{year}-{month:02d}",
                "depreciation":    round(actual_dep, 2),
                "accum_dep_close": accum,
                "nbv_close":       max(nbv, 0),
            })
    return pd.DataFrame(rows)


def gen_payroll_tax():
    """Monthly Victorian payroll tax on wages above monthly threshold."""
    VIC_MONTHLY_THRESHOLD = TAX_CONFIG["VIC_PAYROLL_TAX_THRESHOLD_ANN"] / 12
    VIC_RATE = TAX_CONFIG["VIC_PAYROLL_TAX_RATE"]
    rows = []
    for year, month in FY_MONTHS:
        gross_wages = round(random.gauss(620_000, 18_000), 2)
        taxable_wages = max(gross_wages - VIC_MONTHLY_THRESHOLD, 0)
        tax_due = round(taxable_wages * VIC_RATE, 2)
        due_date = month_end(year, month) + timedelta(days=7)
        paid = (date(year, month, 1) + timedelta(days=45)) < CURRENT_DATE
        rows.append({
            "period":           f"{year}-{month:02d}",
            "gross_wages":      gross_wages,
            "threshold":        round(VIC_MONTHLY_THRESHOLD, 2),
            "taxable_wages":    round(taxable_wages, 2),
            "tax_rate":         VIC_RATE,
            "tax_due":          tax_due,
            "due_date":         str(due_date),
            "lodgement_status": "Lodged" if paid else "Pending",
            "payment_status":   "Paid" if paid else "Pending",
        })
    return pd.DataFrame(rows)


def gen_gst_transactions():
    """
    GST transactions split by supply/acquisition type.

    AR side (Output Tax):
      TAXABLE       ~55% of revenue  — 10% GST collected (G1 on BAS)
      EXPORT        ~37% of revenue  — 0% GST, GST-free export (G3 on BAS)
      GST_FREE_SALE  ~8% of revenue  — 0% GST, domestic exempt supply (G3 on BAS)

    AP side (Input Tax Credits):
      TAXABLE_PURCH  ~72% of expenses — 10% GST, full ITC claimable (G10 on BAS)
      GST_FREE_PURCH ~18% of expenses — 0% GST, no ITC (G14 on BAS)
      INPUT_TAXED    ~10% of expenses — 0% GST, no ITC (G15 on BAS)
    """
    GST_RATE = TAX_CONFIG["GST_RATE"]
    rows = []
    txn_num = 8000

    # Monthly revenue mix by supply type (totals ~$600K/month gross)
    AR_TYPES = [
        ("TAXABLE",       "Taxable supply — domestic programs & consulting",  "2200", 330_000, 20_000),
        ("EXPORT",        "GST-free export — offshore program delivery",       "4002", 225_000, 15_000),
        ("GST_FREE_SALE", "GST-free domestic — education/exempt services",     "4001",  45_000,  5_000),
    ]
    # Monthly expense mix by acquisition type (totals ~$180K/month gross)
    AP_TYPES = [
        ("TAXABLE_PURCH",  "Creditable acquisition — domestic suppliers",       "1201", 130_000, 12_000),
        ("GST_FREE_PURCH", "GST-free acquisition — overseas/exempt suppliers",  "1202",  32_000,  5_000),
        ("INPUT_TAXED",    "Input-taxed acquisition — financial/insurance",     "1203",  18_000,  3_000),
    ]

    for year, month in FY_MONTHS:
        period   = f"{year}-{month:02d}"
        txn_date = str(month_end(year, month))

        for supply_code, desc, acct, mean_amt, std_amt in AR_TYPES:
            txn_num += 1
            amount  = abs(round(random.gauss(mean_amt, std_amt), 2))
            gst_amt = round(amount * GST_RATE, 2) if supply_code == "TAXABLE" else 0.0
            txn_type = "Output Tax" if supply_code == "TAXABLE" else "GST-Free Sale"
            rows.append({
                "gst_id":           f"GST{txn_num:05d}",
                "period":           period,
                "transaction_date": txn_date,
                "description":      desc,
                "supply_type_code": supply_code,
                "transaction_type": txn_type,
                "amount_excl_gst":  amount,
                "gst_amount":       gst_amt,
                "account_code":     acct,
            })

        for supply_code, desc, acct, mean_amt, std_amt in AP_TYPES:
            txn_num += 1
            amount  = abs(round(random.gauss(mean_amt, std_amt), 2))
            gst_amt = round(amount * GST_RATE, 2) if supply_code == "TAXABLE_PURCH" else 0.0
            txn_type = "Input Tax Credit" if supply_code == "TAXABLE_PURCH" else "GST-Free Purchase"
            rows.append({
                "gst_id":           f"GST{txn_num:05d}",
                "period":           period,
                "transaction_date": txn_date,
                "description":      desc,
                "supply_type_code": supply_code,
                "transaction_type": txn_type,
                "amount_excl_gst":  amount,
                "gst_amount":       gst_amt,
                "account_code":     acct,
            })

    return pd.DataFrame(rows)


def gen_bas_returns(gst_df, ptax_df):
    """
    Quarterly BAS lodgements derived from actual GST transaction and payroll data.

    BAS labels used:
      G1  — Total taxable sales (Output Tax supplies)
      G3  — GST-free & export sales (EXPORT + GST_FREE_SALE)
      G10 — Creditable acquisitions (TAXABLE_PURCH)
      G14 — GST-free acquisitions (GST_FREE_PURCH)
      G15 — Input-taxed acquisitions (INPUT_TAXED)
      1A  — GST on sales (output tax collected)
      1B  — GST credits (input tax credits claimed)
      W2  — PAYG withheld on wages (approx 32% of gross wages)
    """
    QUARTERS = [
        ("Q1 FY2026", "2025-07", "2025-09",
         ["2025-07", "2025-08", "2025-09"], date(2025, 10, 28), "2025-10-25"),
        ("Q2 FY2026", "2025-10", "2025-12",
         ["2025-10", "2025-11", "2025-12"], date(2026,  1, 28), "2026-01-25"),
        ("Q3 FY2026", "2026-01", "2026-03",
         ["2026-01", "2026-02", "2026-03"], date(2026,  4, 28), None),
    ]
    rows = []
    for qname, period_from, period_to, periods, due_dt, lodged_dt in QUARTERS:
        q = gst_df[gst_df["period"].isin(periods)]

        # ── AR side ──────────────────────────────────────────────────────────
        g1  = round(q[q["supply_type_code"] == "TAXABLE"]["amount_excl_gst"].sum(), 2)
        g3  = round(q[q["supply_type_code"].isin(["EXPORT", "GST_FREE_SALE"])]["amount_excl_gst"].sum(), 2)
        gst_on_sales = round(q[q["transaction_type"] == "Output Tax"]["gst_amount"].sum(), 2)

        # ── AP side ──────────────────────────────────────────────────────────
        g10 = round(q[q["supply_type_code"] == "TAXABLE_PURCH"]["amount_excl_gst"].sum(), 2)
        g14 = round(q[q["supply_type_code"] == "GST_FREE_PURCH"]["amount_excl_gst"].sum(), 2)
        g15 = round(q[q["supply_type_code"] == "INPUT_TAXED"]["amount_excl_gst"].sum(), 2)
        itc = round(q[q["transaction_type"] == "Input Tax Credit"]["gst_amount"].sum(), 2)

        net_gst = round(gst_on_sales - itc, 2)

        # ── PAYG withheld (W2) — approx 32% effective rate on gross wages ───
        q_wages = ptax_df[ptax_df["period"].isin(periods)]["gross_wages"].sum()
        payg    = round(q_wages * 0.32, 2)

        total_payable = round(net_gst + payg, 2)
        is_lodged     = lodged_dt is not None

        rows.append({
            "quarter":             qname,
            "period_from":         period_from,
            "period_to":           period_to,
            # G-codes
            "g1_taxable_sales":    g1,
            "g3_gst_free_sales":   g3,
            "g10_creditable_acq":  g10,
            "g14_gst_free_acq":    g14,
            "g15_input_taxed_acq": g15,
            # BAS summary fields
            "gst_collected":       gst_on_sales,   # 1A
            "gst_itc":             itc,             # 1B
            "net_gst":             net_gst,
            "withheld_tax_payg":   payg,            # W2
            "total_payable":       total_payable,
            "due_date":            str(due_dt),
            "lodged_date":         lodged_dt,
            "lodgement_status":    "Lodged" if is_lodged else "Due",
            "payment_status":      "Paid"   if is_lodged else "Pending",
        })
    return pd.DataFrame(rows)


def gen_month_end_checklist():
    """Month-end close checklist with Day 1–5 targets and dependency sequencing.

    dependency logic:
      Day 1 — foundation journals (no dependencies)
      Day 2 — accruals & subsidiary ledgers (depend on Day 1 payroll)
      Day 3 — depreciation & intercompany (depend on Day 2 accruals / AP)
      Day 4 — variance analysis & BS recs (depend on Day 3 complete)
      Day 5 — manager sign-off (depends on all Day 4 complete)
    """
    TASKS = [
        # (seq, name,                               target_day, depends_on_seq)
        (1,  "Post payroll journals",                1, None),
        (2,  "Reconcile bank statement",             1, None),
        (3,  "Post accrual journals",                2, 1),
        (4,  "Reconcile accounts receivable",        2, 1),
        (5,  "Reconcile accounts payable",           2, 2),
        (6,  "Post depreciation journals",           3, 3),
        (7,  "Clear intercompany balances",          3, 5),
        (8,  "Review P&L variance analysis",         4, 6),
        (9,  "Prepare balance sheet reconciliations",4, 7),
        (10, "Manager review & sign-off",            5, 9),
    ]

    rows = []
    completed_months = FY_MONTHS[:-1]
    current_month    = FY_MONTHS[-1]

    for year, month in completed_months:
        next_month_start = date(year + (month // 12), (month % 12) + 1, 1)
        for seq, task, target_day, depends_on in TASKS:
            # Simulate realistic close within target day ± 1
            actual_day  = random.randint(max(target_day - 1, 1), target_day + 1)
            completed_date = next_month_start + timedelta(days=actual_day - 1)
            rows.append({
                "period":         f"{year}-{month:02d}",
                "task_sequence":  seq,
                "task_name":      task,
                "target_day":     target_day,
                "depends_on_seq": depends_on,
                "status":         "Complete",
                "completed_date": str(completed_date),
                "owner":          random.choice(["J. Smith", "A. Nguyen", "P. Krishnan", "Finance_Team"]),
                "is_current":     0,
            })

    # Current month — partial completion respecting dependency order
    y, m = current_month
    partial_done = random.randint(4, 7)   # tasks 1..partial_done are Complete
    for seq, task, target_day, depends_on in TASKS:
        i = seq - 1
        if i < partial_done:
            status = "Complete"
            completed_date = rand_date(y, m)
        elif i == partial_done:
            status = "In Progress"
            completed_date = None
        else:
            status = "Pending"
            completed_date = None
        rows.append({
            "period":         f"{y}-{m:02d}",
            "task_sequence":  seq,
            "task_name":      task,
            "target_day":     target_day,
            "depends_on_seq": depends_on,
            "status":         status,
            "completed_date": str(completed_date) if completed_date else None,
            "owner":          random.choice(["J. Smith", "A. Nguyen", "P. Krishnan", "Finance_Team"]),
            "is_current":     1,
        })
    return pd.DataFrame(rows)


def gen_intercompany():
    rows = []
    ic_num = 3000
    for year, month in FY_MONTHS:
        ic_num += 1
        charge = round(random.gauss(85_000, 5_000), 2)
        rows.append({
            "ic_id":          f"IC{ic_num:04d}",
            "period":         f"{year}-{month:02d}",
            "transaction_date": str(month_end(year, month)),
            "entity_from":    "Parent University",
            "entity_to":      "UniPath Pty Ltd",
            "description":    "Shared services recharge – Finance, HR, IT, Legal",
            "amount":         charge,
            "account_dr":     "5300",
            "account_cr":     "2400",
            "status":         "Matched" if random.random() > 0.05 else "Unmatched",
        })
    return pd.DataFrame(rows)


def gen_suppliers():
    rows = []
    for sid, name, stype, terms, acct, cc in SUPPLIERS:
        rows.append({
            "supplier_id":         sid,
            "supplier_name":       name,
            "supplier_type":       stype,
            "payment_terms_days":  terms,
            "expense_account":     acct,
            "cost_centre":         cc,
        })
    return pd.DataFrame(rows)


def gen_accounts_payable():
    """Generate supplier invoices across FY2026 with realistic aging, payment status and GST supply type."""
    GST_RATE = TAX_CONFIG["GST_RATE"]
    rows = []
    inv_num = 5000
    ref_date = CURRENT_DATE   # 31 March 2026

    # Amount ranges by supplier type
    amt_ranges = {
        "Technology":        (15_000, 85_000),
        "Facilities":        (20_000, 65_000),
        "Professional Svcs": (10_000, 55_000),
        "Marketing":         (8_000,  40_000),
        "Utilities":         (5_000,  20_000),
    }

    for year, month in FY_MONTHS:
        period = f"{year}-{month:02d}"
        n_invoices = random.randint(5, 9)
        sample_suppliers = random.sample(SUPPLIERS, min(n_invoices, len(SUPPLIERS)))

        for sid, sname, stype, terms, exp_acct, cc in sample_suppliers:
            inv_num += 1
            inv_date = rand_date(year, month)
            due_date = inv_date + timedelta(days=terms)

            lo, hi   = amt_ranges.get(stype, (5_000, 30_000))
            amt_ex   = round(random.gauss((lo + hi) / 2, (hi - lo) / 6), 2)
            amt_ex   = max(lo, abs(amt_ex))

            # Assign supply type using AP_SUPPLY_WEIGHTS by supplier category
            weights_list = AP_SUPPLY_WEIGHTS.get(stype, [("TAXABLE_PURCH", 80), ("GST_FREE_PURCH", 20)])
            codes   = [w[0] for w in weights_list if w[1] > 0]
            weights = [w[1] for w in weights_list if w[1] > 0]
            supply_type = random.choices(codes, weights=weights)[0]

            gst_amt = round(amt_ex * GST_RATE, 2) if supply_type == "TAXABLE_PURCH" else 0.0
            total   = round(amt_ex + gst_amt, 2)

            days_since_due = (ref_date - due_date).days

            # Payment likelihood increases with age
            if days_since_due > 90:
                status = "Paid"
            elif days_since_due > 60:
                status = random.choices(["Paid", "Unpaid"], weights=[85, 15])[0]
            elif days_since_due > 30:
                status = random.choices(["Paid", "Unpaid"], weights=[70, 30])[0]
            elif days_since_due > 0:
                status = random.choices(["Paid", "Unpaid"], weights=[40, 60])[0]
            else:
                status = random.choices(["Unpaid", "Paid"], weights=[80, 20])[0]

            pay_date = None
            if status == "Paid":
                lag = random.randint(terms, terms + 15)
                pay_date = str(inv_date + timedelta(days=lag))

            rows.append({
                "invoice_number":      f"SINV{inv_num:05d}",
                "supplier_id":         sid,
                "supplier_name":       sname,
                "supplier_type":       stype,
                "supply_type_code":    supply_type,
                "period":              period,
                "invoice_date":        str(inv_date),
                "due_date":            str(due_date),
                "payment_terms_days":  terms,
                "amount_ex_gst":       amt_ex,
                "gst_amount":          gst_amt,
                "total_inc_gst":       total,
                "status":              status,
                "payment_date":        pay_date,
                "expense_account":     exp_acct,
                "cost_centre":         cc,
            })

    return pd.DataFrame(rows)


def gen_monthly_budget():
    """
    Generate FY2026 monthly budget targets.
    Budget is set at start of year using prior-year assumptions.
    Revenue target = 5% growth on prior year seasonal base.
    Expense budget = actuals-like base with tighter cost discipline.
    """
    rev_weights = {7: 1.2, 8: 1.1, 9: 1.3, 10: 1.0, 11: 0.9, 12: 0.7,
                   1: 1.1, 2: 1.0, 3: 1.2, 4: 1.0, 5: 0.9, 6: 0.8}
    rows = []
    for year, month in FY_MONTHS:
        w = rev_weights.get(month, 1.0)
        period = f"{year}-{month:02d}"
        # Revenue budget: base annual target spread by seasonality
        budget_rev  = round((480_000 + 310_000 + 95_000 + 75_000 + 12_000) * w * 1.05, 0)
        # Operating expense budget: tighter than actuals (payroll capped, opex restrained)
        # Includes FBT monthly accrual ($29,272 / 12 ≈ $2,439) and monthly intercompany
        budget_exp  = round((620_000 * 1.12 + 620_000 * 0.0485 + 65_000 + 42_000
                             + 28_000 + 35_000 + 12_000 + 83_333 + 2_439) * 1.02, 0)
        budget_ebt  = round(budget_rev - budget_exp, 0)
        # Tax provision budget: 30% of positive EBT (s66-5 ITAA 1997)
        budget_tax  = round(max(0, budget_ebt) * TAX_CONFIG["CORP_TAX_RATE"], 0)
        budget_npat = round(budget_ebt - budget_tax, 0)
        rows.append({
            "period":           period,
            "budget_revenue":   budget_rev,
            "budget_expenses":  budget_exp,
            "budget_ebt":       budget_ebt,
            "budget_tax":       budget_tax,
            "budget_net":       budget_npat,   # budget_net = budgeted NPAT for KPI comparison
        })
    return pd.DataFrame(rows)


def gen_tax_config():
    """Export TAX_CONFIG as a database table — single source of truth for all rates."""
    descriptions = {
        "SGC_RATE":                      ("Superannuation Guarantee Rate",                             "Payroll", "1 Jul 2025 — 12%"),
        "VIC_PAYROLL_TAX_RATE":          ("Victorian Payroll Tax Rate",                                "Payroll", "Ongoing"),
        "VIC_PAYROLL_TAX_THRESHOLD_ANN": ("Victorian Payroll Tax Annual Threshold",                    "Payroll", "1 Jul 2025 — increased to $1M"),
        "GST_RATE":                      ("Goods & Services Tax Rate",                                 "GST",     "Ongoing"),
        "INDIVIDUAL_TOP_RATE":           ("Top Marginal Income Tax Rate (s12-5 ITAA 1997)",            "FBT",     "FY2026"),
        "MEDICARE_LEVY":                 ("Medicare Levy Rate (s8 Medicare Levy Act 1986)",            "FBT",     "FY2026"),
        "FBT_RATE":                      ("FBT Rate = Top Marginal + Medicare Levy (s5 FBTAA 1986)",   "FBT",     "FY2026 — derived"),
        "FBT_GROSS_UP_TYPE1":            ("FBT Type 1 Gross-Up — ATO published FY2026",                "FBT",     "FY2026 — ATO authoritative"),
        "FBT_GROSS_UP_TYPE2":            ("FBT Type 2 Gross-Up = 1/(1-FBT)",                          "FBT",     "FY2026 — derived"),
        "EV_HOME_CHARGING_C_PER_KM":     ("EV Home Charging Rate (Operating Cost Method)",            "FBT-EV",  "PCG 2024/2 — FY2026; FY2027 draft 5.47c"),
        "LCT_THRESHOLD_ICE":             ("LCT Threshold — Standard Vehicles",                        "LCT",     "1 Jul 2025 — indexed annually"),
        "LCT_THRESHOLD_FEV":             ("LCT Threshold — Fuel-Efficient Vehicles (≤3.5L/100km)",    "LCT",     "1 Jul 2025 — indexed annually"),
        "LCT_RATE":                      ("Luxury Car Tax Rate (s9 LCTA 1999)",                       "LCT",     "Ongoing — not indexed"),
        "CORP_TAX_RATE":                 ("Corporate Income Tax Rate (s66-5 ITAA 1997)",               "Corp Tax", "30% — standard rate (BRE 25% not applicable)"),
    }
    rows = []
    for key, value in TAX_CONFIG.items():
        desc, category, effective = descriptions[key]
        rows.append({
            "config_key":       key,
            "description":      desc,
            "category":         category,
            "rate_or_amount":   value,
            "effective_from":   effective,
        })
    return pd.DataFrame(rows)


# ─────────────────────────────────────────────────────────────────────────────
# FBT REGISTER
# Production sub-ledger: one row per vehicle/benefit per FBT year.
# Schema aligns with DDL spec — supports AI agent consumption and STP Phase 2 export.
# ─────────────────────────────────────────────────────────────────────────────

def gen_fbt_register():
    """
    FBT Register — FBTAA 1986 | ITAA 1997 | LCTA 1999
    Compliance period: 1 April 2025 – 31 March 2026 (FY2026).

    Columns
    -------
    fbt_year          : FBT year (YYYY)
    asset_id          : Internal fleet identifier
    description       : Human-readable benefit description
    fuel_type         : 'BEV', 'ICE', 'PHEV' — used in s58P exemption gate
    gross_up_type     : 'Type 1' (GST claimable) | 'Type 2' (no GST)
    purchase_price    : GST-inclusive purchase price
    fuel_consumption  : Litres/100km (determines LCT threshold band)
    first_use_date    : Required for s58P BEV exemption (must be ≥ 2022-07-01)
    years_held        : Years held at 1 April 2025 (start of FBT year)
    lct_threshold     : Applicable LCT threshold (3.5L rule)
    lct_payable       : (price − threshold) / 1.1 × 33%  — s9 LCTA 1999
    base_val_pre_disc : purchase_price + lct_payable
    discount_applies  : True if years_held ≥ 4  — s9(2) FBTAA 1986
    base_val_final    : base_val_pre_disc × 2/3 if discount_applies else as-is
    private_days      : Days available for private use
    taxable_value     : base_val_final × 20% × (private_days / 365) — s9 FBTAA
    bev_exempt        : True if FUEL_TYPE=='BEV' AND first_use≥2022-07-01 AND price≤LCT_FEV
    fbt_payable       : taxable_value × gross_up_rate × FBT_RATE  ($0 if exempt)
    rfba_amount       : taxable_value × gross_up_rate (T1 or T2) — s5E FBTAA / s136 ITAA
                        Reported on employee income statement if > $2,000.
                        NOTE: BEV uses T2 for RFBA; all others mirror their gross-up type.
    compliance_chk    : True if all legislative conditions programmatically verified
    """

    # ── Global FBT constants — pulled from TAX_CONFIG (single source of truth) ─
    FBT_RATE    = _FBT_RATE                               # 0.47
    T1          = _GROSS_UP_T1                            # 2.0802 (ATO-published FY2026)
    T2          = _GROSS_UP_T2                            # 1.8868 (derived: 1/(1-FBT_RATE))
    LCT_ICE     = TAX_CONFIG["LCT_THRESHOLD_ICE"]         # Standard ICE threshold (1 Jul 2025)
    LCT_FEV     = TAX_CONFIG["LCT_THRESHOLD_FEV"]         # Fuel-efficient threshold ≤3.5L/100km
    STAT_RATE   = 0.20                                    # Statutory formula rate — s9 FBTAA 1986
    RFBA_FLOOR  = 2_000                                   # s5E(3): reporting only required if RFBA > $2,000
    PHEV_CUTOFF = "2025-04-01"                            # s58P exemption ended for PHEVs

    # ── Fleet register: raw inputs ─────────────────────────────────────────────
    # binding_commitment_date: required for PHEV grandfathering under s58P(2).
    # Must be a date string < "2025-04-01" to qualify. None = not applicable.
    # For BEV/ICE: None (exemption is asset-based, not commitment-based).
    # For PHEV post-cutoff without a pre-cutoff commitment: compliance_chk = False.
    # Employee linkage — in production this joins to the HR/payroll system.
    # STP Phase 2 requires payee_id + masked TFN on every RFBA disclosure row.
    # Non-vehicle benefits (entertainment, expense pmts) are pooled — no single
    # employee assignment; marked as "POOL" and excluded from STP line-item export.
    EMPLOYEE_MAP = {
        "VEH-001": ("EMP-0042", "J. Anderson",   "XXX-XXX-042"),   # BEV driver
        "VEH-002": ("EMP-0017", "M. Tran",        "XXX-XXX-017"),   # ICE fleet car
        "VEH-003": ("POOL",     "Pooled – Staff", "N/A"),           # Entertainment
        "VEH-004": ("POOL",     "Pooled – Staff", "N/A"),           # Expense payments
    }

    fleet = [
        # (asset_id, description, fuel_type, gross_up_type, purchase_price,
        #  fuel_l_100km, first_use_date, years_held, private_days, binding_commitment_date)
        ("VEH-001", "BEV – Tesla Model 3 Long Range",     "BEV",  "Type 1", 68_500, 0.0, "2023-09-01", 1.58, 310, None),
        ("VEH-002", "ICE Fleet Car – Toyota Camry Hybrid", "ICE", "Type 1", 52_800, 8.2, "2023-07-01", 1.75, 310, None),
        ("VEH-003", "Entertainment – Meals & Events",      "N/A", "Type 2", 0,      0.0, "N/A",         0.0,   0, None),
        ("VEH-004", "Expense Payments – Reimbursements",   "N/A", "Type 2", 0,      0.0, "N/A",         0.0,   0, None),
        # Example grandfathered PHEV (binding commitment pre-1 Apr 2025 — s58P(2)):
        # ("VEH-005", "PHEV – Mitsubishi Outlander",       "PHEV", "Type 1", 58_000, 2.1, "2024-11-01", 0.42, 310, "2024-10-15"),
    ]

    # Fixed taxable values for non-vehicle benefits (not formula-driven)
    _NON_VEHICLE_TV = {"VEH-003": 14_800.0, "VEH-004": 8_320.0}

    rows = []
    for (asset_id, desc, fuel_type, gu_type, price, fuel_l, first_use, yrs_held, priv_days, binding_date) in fleet:

        gu_rate = T1 if gu_type == "Type 1" else T2

        # ── LCT check (s9 LCTA 1999) ──────────────────────────────────────────
        lct_threshold = LCT_FEV if fuel_l <= 3.5 else LCT_ICE
        if price > lct_threshold and fuel_type not in ("N/A",):
            lct_payable = round((price - lct_threshold) / 1.1 * 0.33, 2)
        else:
            lct_payable = 0.0
        base_pre_disc = round(price + lct_payable, 2)

        # ── 4-year 1/3 discount (s9(2) FBTAA 1986) ───────────────────────────
        discount_applies = yrs_held >= 4
        base_final = round(base_pre_disc * 2 / 3, 2) if discount_applies else base_pre_disc

        # ── Taxable value (statutory formula or fixed for non-vehicle) ────────
        if asset_id in _NON_VEHICLE_TV:
            taxable_val = _NON_VEHICLE_TV[asset_id]
        else:
            taxable_val = round(base_final * STAT_RATE * (priv_days / 365), 2)

        # ── s58P BEV exemption gate (strict fuel-type validation) ─────────────
        bev_exempt = (
            fuel_type == "BEV"
            and first_use >= "2022-07-01"        # Treasury Laws Amendment condition
            and price <= LCT_FEV                 # cost ≤ LCT fuel-efficient threshold
        )

        # ── FBT payable ───────────────────────────────────────────────────────
        if bev_exempt:
            fbt_payable = 0.0
            # RFBA for exempt BEV uses T2 (no GST ITC on notional value)
            rfba_amount = round(taxable_val * T2, 2)
        else:
            fbt_payable = round(taxable_val * gu_rate * FBT_RATE, 2)
            # RFBA mirrors the benefit's gross-up type (T1 or T2)
            rfba_amount = round(taxable_val * gu_rate, 2)

        # ── compliance_chk: all conditions must hold ──────────────────────────
        # PHEV post-cutoff requires a binding_commitment_date < PHEV_CUTOFF (s58P(2)).
        # A PHEV with no binding_commitment_date or one dated after the cutoff fails.
        phev_grandfathered = (
            fuel_type == "PHEV"
            and binding_date is not None
            and binding_date < PHEV_CUTOFF
        )
        compliance_chk = (
            (bev_exempt or fbt_payable > 0 or asset_id in _NON_VEHICLE_TV)
            and (fuel_type != "PHEV" or phev_grandfathered)
        )

        rows.append({
            "fbt_year":                2026,
            "asset_id":                asset_id,
            "description":             desc,
            "fuel_type":               fuel_type,
            "gross_up_type":           gu_type,
            "purchase_price":          price,
            "fuel_consumption":        fuel_l,
            "first_use_date":          first_use,
            "binding_commitment_date": binding_date if binding_date else "N/A",
            "phev_grandfathered":      phev_grandfathered,
            "years_held":              yrs_held,
            "lct_threshold":     lct_threshold,
            "lct_payable":       lct_payable,
            "base_val_pre_disc": base_pre_disc,
            "discount_applies":  discount_applies,
            "base_val_final":    base_final,
            "private_days":      priv_days,
            "taxable_value":     taxable_val,
            "bev_exempt":        bev_exempt,
            "fbt_payable":       fbt_payable,
            "rfba_amount":       rfba_amount,
            "rfba_reportable":   rfba_amount > RFBA_FLOOR,
            "compliance_chk":    compliance_chk,
            # ── Employee linkage for STP Phase 2 export ───────────────────────
            "employee_id":       EMPLOYEE_MAP.get(asset_id, ("N/A","N/A","N/A"))[0],
            "employee_name":     EMPLOYEE_MAP.get(asset_id, ("N/A","N/A","N/A"))[1],
            "employee_tfn_masked": EMPLOYEE_MAP.get(asset_id, ("N/A","N/A","N/A"))[2],
        })

    return pd.DataFrame(rows)


# ─────────────────────────────────────────────────────────────────────────────
# CORPORATE TAX PROVISION
# Current-period tax provision: DR 5400 Income Tax Expense / CR 2203 Income Tax Payable
# Rate: 30% (s66-5 ITAA 1997 — standard company rate).
# Scope: current tax only. Deferred tax (temporary differences on depreciation,
#        provisions) is recognised in statutory accounts but excluded here for clarity.
# FBT deductibility: s8-1 ITAA 1997 — FBT expense (5205) reduces taxable income.
#   The FBT accrual JE is in the GL before this function runs → automatically included.
# ─────────────────────────────────────────────────────────────────────────────

def gen_corporate_tax_provision(gl_df):
    """
    Calculate and post a monthly current-period income tax provision.

    Taxable income per period (simplified current-tax basis):
        Revenue (credits on 4xxx) − All Expenses (debits on 5xxx incl. 5205 FBT)
        → Tax provision = max(0, taxable_income) × CORP_TAX_RATE
        → Loss periods: no provision (no refund recognised in this model)

    Note: Intercompany charges (5300) are assumed non-deductible for tax purposes
    (transfer pricing / related-party rules) — excluded from the tax base below.
    In a full tax return this would require an addback schedule.

    Returns: DataFrame of GL rows in general_ledger schema.
    """
    CORP_TAX_RATE   = TAX_CONFIG["CORP_TAX_RATE"]    # 0.30 — s66-5 ITAA 1997
    ACC_TAX_EXP     = "5400"   # DR — Income Tax Expense
    ACC_TAX_PAY     = "2203"   # CR — Income Tax Payable
    COST_CENTRE     = "CC003"
    POSTED_BY       = "Finance_Team"

    # Accounts excluded from taxable income: intercompany charges are a timing
    # / transfer-pricing item — addback required in actual tax return
    ADDBACK_ACCOUNTS = {"5300"}

    rows    = []
    je_num  = 9500   # Tax provision JE range — no conflict with GL (1xxx) or FBT (9xxx)

    periods = sorted(gl_df["period"].unique())
    for period in periods:
        p_gl = gl_df[gl_df["period"] == period]
        me   = p_gl["journal_date"].max()  # month-end date for this period

        # Revenue: credits on 4xxx accounts
        revenue = p_gl[p_gl["account_code"].str.startswith("4")]["credit"].sum()

        # Deductible expenses: debits on 5xxx, excluding non-deductible addbacks
        expenses = p_gl[
            p_gl["account_code"].str.startswith("5") &
            ~p_gl["account_code"].isin(ADDBACK_ACCOUNTS) &
            (p_gl["account_code"] != ACC_TAX_EXP)   # prevent double-count if called twice
        ]["debit"].sum()

        taxable_income = revenue - expenses
        if taxable_income <= 0:
            continue   # Loss period — no current-tax provision recognised

        provision = round(taxable_income * CORP_TAX_RATE, 2)
        je_num += 1
        jeid    = f"JE{je_num:05d}"
        narr    = (
            f"Income tax provision {period} — "
            f"taxable income ${taxable_income:,.0f} × {CORP_TAX_RATE:.0%} = ${provision:,.0f}"
        )

        rows.append({
            "journal_id":   jeid, "period": period, "journal_date": me,
            "account_code": ACC_TAX_EXP, "cost_centre": COST_CENTRE,
            "description":  narr, "debit": provision, "credit": 0.0,
            "journal_type": "Tax_Provision", "posted_by": POSTED_BY,
        })
        rows.append({
            "journal_id":   jeid, "period": period, "journal_date": me,
            "account_code": ACC_TAX_PAY, "cost_centre": COST_CENTRE,
            "description":  narr, "debit": 0.0, "credit": provision,
            "journal_type": "Tax_Provision", "posted_by": POSTED_BY,
        })

    return pd.DataFrame(rows)


# ─────────────────────────────────────────────────────────────────────────────
# FBT JOURNAL ENTRY GENERATOR
# Converts fbt_register sub-ledger rows into double-entry GL journals.
# Two journal types:
#   FBT_Accrual  — 31 Mar 2026 (period 2026-03): DR 5205 / CR 2202 per benefit
#   FBT_Payment  — 21 May 2026 (period 2026-05): DR 2202 / CR 1002 (settlement)
# ─────────────────────────────────────────────────────────────────────────────

def gen_fbt_journal_entries(fbt_reg_df):
    """
    Generate FBT journal entries from the fbt_register sub-ledger.

    Returns two DataFrames:
        je_df   : Full journal_entries table (richer schema — sub-ledger reconciliation)
        gl_rows : GL-compatible rows ready to concat into general_ledger
    """
    ACCRUAL_DATE   = "2026-03-31"
    ACCRUAL_PERIOD = "2026-03"
    PAYMENT_DATE   = "2026-05-21"
    PAYMENT_PERIOD = "2026-05"
    POSTED_BY      = "Finance_Team"

    # Account codes
    ACC_FBT_EXP     = "5205"   # FBT Expense (DR)
    ACC_FBT_PAY     = "2202"   # FBT Payable (CR accrual / DR settlement)
    ACC_BANK        = "1002"   # Cash at Bank (CR on payment)
    COST_CENTRE     = "CC003"  # Finance & Administration

    je_rows = []    # journal_entries table
    gl_rows = []    # general_ledger-compatible rows
    je_num  = 9000  # FBT JE range — no conflict with GL (starts at 1000)

    # ── JE 1: FBT Accrual — one DR line per benefit, one CR total ─────────────
    je_num += 1
    jeid_accrual = f"JE{je_num:05d}"
    total_fbt = 0.0

    for _, row in fbt_reg_df.iterrows():
        fbt_amt = float(row["fbt_payable"])
        total_fbt += fbt_amt
        asset_id   = row["asset_id"]
        desc_short = row["description"]
        is_exempt  = bool(row["bev_exempt"])

        narration = (
            f"FBT accrual FY2026 – {desc_short} – "
            + ("s58P exempt: taxable value $0" if is_exempt
               else f"taxable value ${float(row['taxable_value']):,.2f} "
                    f"× {row['gross_up_type']} × 47%")
        )

        # DR 5205 per benefit line (zero-value for exempt — preserves audit trail)
        for tbl, acct, dr, cr in [
            ("je",  ACC_FBT_EXP, fbt_amt, 0.0),
            ("gl",  ACC_FBT_EXP, fbt_amt, 0.0),
        ]:
            rec = {
                "journal_id":     jeid_accrual,
                "period":         ACCRUAL_PERIOD,
                "journal_date":   ACCRUAL_DATE,
                "account_code":   acct,
                "cost_centre":    COST_CENTRE,
                "description":    narration,
                "debit":          round(dr, 2),
                "credit":         round(cr, 2),
                "journal_type":   "FBT_Accrual",
                "posted_by":      POSTED_BY,
            }
            if tbl == "je":
                rec.update({
                    "line_no":           len([r for r in je_rows if r["journal_id"] == jeid_accrual]) + 1,
                    "sub_ledger_ref":    asset_id,
                    "sub_ledger_table":  "fbt_register",
                    "status":            "Posted",
                    "due_date":          "2026-05-21",
                    "legislative_ref":   "s5 FBTAA 1986",
                })
                je_rows.append(rec)
            else:
                gl_rows.append(rec)

    # CR 2202 — single balancing line for total FBT payable
    cr_narration = f"FBT Payable accrual FY2026 — total ${total_fbt:,.2f} | Due 21 May 2026"
    for tbl, acct, dr, cr in [
        ("je", ACC_FBT_PAY, 0.0, total_fbt),
        ("gl", ACC_FBT_PAY, 0.0, total_fbt),
    ]:
        rec = {
            "journal_id":   jeid_accrual,
            "period":       ACCRUAL_PERIOD,
            "journal_date": ACCRUAL_DATE,
            "account_code": acct,
            "cost_centre":  COST_CENTRE,
            "description":  cr_narration,
            "debit":        0.0,
            "credit":       round(total_fbt, 2),
            "journal_type": "FBT_Accrual",
            "posted_by":    POSTED_BY,
        }
        if tbl == "je":
            rec.update({
                "line_no":          len([r for r in je_rows if r["journal_id"] == jeid_accrual]) + 1,
                "sub_ledger_ref":   "ALL",
                "sub_ledger_table": "fbt_register",
                "status":           "Posted",
                "due_date":         "2026-05-21",
                "legislative_ref":  "s5 FBTAA 1986",
            })
            je_rows.append(rec)
        else:
            gl_rows.append(rec)

    # ── JE 2: FBT Payment — settlement on due date ────────────────────────────
    je_num += 1
    jeid_payment = f"JE{je_num:05d}"
    pay_narration = f"ATO FBT payment FY2026 — ${total_fbt:,.2f} | Lodge via Tax Agent Portal"

    for acct, dr, cr, status in [
        (ACC_FBT_PAY, total_fbt, 0.0,        "Pending"),   # DR 2202 (clears liability)
        (ACC_BANK,    0.0,       total_fbt,   "Pending"),   # CR 1002 (reduces cash)
    ]:
        rec_base = {
            "journal_id":   jeid_payment,
            "period":       PAYMENT_PERIOD,
            "journal_date": PAYMENT_DATE,
            "account_code": acct,
            "cost_centre":  COST_CENTRE,
            "description":  pay_narration,
            "debit":        round(dr, 2),
            "credit":       round(cr, 2),
            "journal_type": "FBT_Payment",
            "posted_by":    POSTED_BY,
        }
        je_only = {
            "line_no":          len([r for r in je_rows if r["journal_id"] == jeid_payment]) + 1,
            "sub_ledger_ref":   "ALL",
            "sub_ledger_table": "fbt_register",
            "status":           status,       # Pending — outside dataset period
            "due_date":         PAYMENT_DATE,
            "legislative_ref":  "s69 FBTAA 1986",
        }
        je_rows.append({**rec_base, **je_only})
        # Payment JE NOT added to GL — outside the 9-month dataset period (2026-05)

    je_df  = pd.DataFrame(je_rows)
    gl_df  = pd.DataFrame(gl_rows)
    return je_df, gl_df


# ─────────────────────────────────────────────────────────────────────────────
# DATABASE BUILDER
# ─────────────────────────────────────────────────────────────────────────────

def build_database():
    os.makedirs(DATA_DIR, exist_ok=True)

    print("Generating synthetic data...")
    coa        = gen_chart_of_accounts()
    ccs        = gen_cost_centres()
    gst_types  = gen_gst_supply_types()          # new reference table
    custs      = gen_customers()
    gl         = gen_general_ledger()
    ar         = gen_accounts_receivable()
    bank       = gen_bank_transactions()
    fa         = gen_fixed_assets()
    dep_sched  = gen_depreciation_schedule(fa)
    ptax       = gen_payroll_tax()
    gst        = gen_gst_transactions()           # now includes supply_type_code
    bas        = gen_bas_returns(gst, ptax)       # derived from actual gst + payroll data
    checklist  = gen_month_end_checklist()
    ic         = gen_intercompany()
    tax_cfg    = gen_tax_config()
    budget     = gen_monthly_budget()

    suppliers  = gen_suppliers()
    ap         = gen_accounts_payable()
    fbt_reg    = gen_fbt_register()
    fbt_je, fbt_gl_rows = gen_fbt_journal_entries(fbt_reg)

    # Append FBT accrual rows into the GL so trial balance / P&L pick them up
    gl = pd.concat([gl, fbt_gl_rows], ignore_index=True)

    # Corporate tax provision — runs AFTER FBT is in the GL so FBT deductibility flows through
    tax_provision = gen_corporate_tax_provision(gl)
    gl = pd.concat([gl, tax_provision], ignore_index=True)

    print(f"Writing database to {DB_PATH}...")
    conn = sqlite3.connect(DB_PATH)

    tables = {
        "chart_of_accounts":      coa,
        "cost_centres":           ccs,
        "gst_supply_types":       gst_types,      # reference table for supply classifications
        "customers":              custs,
        "suppliers":              suppliers,
        "general_ledger":         gl,
        "accounts_receivable":    ar,
        "accounts_payable":       ap,
        "bank_transactions":      bank,
        "fixed_assets":           fa,
        "depreciation_schedule":  dep_sched,
        "payroll_tax":            ptax,
        "gst_transactions":       gst,
        "bas_returns":            bas,
        "month_end_checklist":    checklist,
        "intercompany":           ic,
        "tax_compliance_config":  tax_cfg,
        "monthly_budget":         budget,
        "fbt_register":           fbt_reg,
        "journal_entries":        fbt_je,
    }

    for tbl, df in tables.items():
        df.to_sql(tbl, conn, if_exists="replace", index=False)
        # Export CSV for reference
        df.to_csv(os.path.join(DATA_DIR, f"{tbl}.csv"), index=False)
        print(f"  ✓ {tbl:30s}  ({len(df):,} rows)")

    conn.close()
    print("\nDatabase build complete.")
    return tables


if __name__ == "__main__":
    build_database()
