#!/usr/bin/env python3
"""
RMIT UP Financial Data Generator
==================================
Generates synthetic but realistic financial data for RMIT UP Pty Ltd.
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

TAX_CONFIG = {
    "SGC_RATE":                      0.12,       # Superannuation Guarantee (12% from 1 Jul 2025)
    "VIC_PAYROLL_TAX_RATE":          0.0485,     # Victorian payroll tax rate (4.85%)
    "VIC_PAYROLL_TAX_THRESHOLD_ANN": 1_000_000,  # Annual wage threshold (VIC — increased to $1M from 1 Jul 2025)
    "GST_RATE":                      0.10,       # Goods & Services Tax (10%)
    "FBT_GROSS_UP_TYPE1":            2.0802,     # FBT Type 1 gross-up (GST-creditable benefits)
    "FBT_GROSS_UP_TYPE2":            1.8868,     # FBT Type 2 gross-up (non-GST-creditable benefits)
    "FBT_RATE":                      0.47,       # FBT rate (47% — top marginal + Medicare levy)
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
    ("2300", "Deferred Revenue",                       "Liability", "Current Liabilities",       "Credit"),
    ("2400", "Intercompany Payable – RMIT University", "Liability", "Current Liabilities",       "Credit"),
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
    ("5300", "Intercompany Charges – RMIT University", "Expense",   "Intercompany",              "Debit"),
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
                "description": "Intercompany charge – RMIT University shared services",
                "debit": line[1], "credit": line[2],
                "journal_type": "Intercompany", "posted_by": "Finance_Team",
            })

    df = pd.DataFrame(rows)
    df["journal_date"] = df["journal_date"].astype(str)
    return df


def gen_accounts_receivable():
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
            gst_amt = round(amount * 0.10, 2) if region == "Domestic" else 0.0
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
                "invoice_date":      str(inv_date),
                "due_date":          str(due_date),
                "period":            f"{year}-{month:02d}",
                "amount_excl_gst":   amount,
                "gst_amount":        gst_amt,
                "total_inc_gst":     total_inc_gst,
                "status":            status,
                "paid_date":         str(paid_date) if paid_date else None,
                "paid_amount":        paid_amount,
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
    """GST on revenue (collected) and expenses (input tax credits)."""
    rows = []
    txn_num = 8000
    for year, month in FY_MONTHS:
        # GST collected on domestic revenue
        taxable_rev = round(random.gauss(600_000, 30_000), 2)
        gst_collected = round(taxable_rev * 0.10, 2)
        txn_num += 1
        rows.append({
            "gst_id": f"GST{txn_num:05d}",
            "period": f"{year}-{month:02d}",
            "transaction_date": str(month_end(year, month)),
            "description": "GST collected on taxable supplies",
            "transaction_type": "Output Tax",
            "amount_excl_gst": taxable_rev,
            "gst_amount": gst_collected,
            "account_code": "2200",
        })
        # Input tax credits on expenses
        taxable_exp = round(random.gauss(180_000, 15_000), 2)
        gst_itc = round(taxable_exp * 0.10, 2)
        txn_num += 1
        rows.append({
            "gst_id": f"GST{txn_num:05d}",
            "period": f"{year}-{month:02d}",
            "transaction_date": str(month_end(year, month)),
            "description": "Input tax credits on acquisitions",
            "transaction_type": "Input Tax Credit",
            "amount_excl_gst": taxable_exp,
            "gst_amount": gst_itc,
            "account_code": "1201",
        })
    return pd.DataFrame(rows)


def gen_bas_returns():
    """Quarterly BAS lodgements."""
    # Quarters: Q1=Jul-Sep, Q2=Oct-Dec, Q3=Jan-Mar
    quarters = [
        ("Q1 FY2026", "2025-07", "2025-09", date(2025, 10, 28), "2025-10-25"),
        ("Q2 FY2026", "2025-10", "2025-12", date(2026, 1, 28),  "2026-01-25"),
        ("Q3 FY2026", "2026-01", "2026-03", date(2026, 4, 28),  None),
    ]
    rows = []
    for qname, period_from, period_to, due_dt, lodged_dt in quarters:
        gst_collected = round(random.gauss(185_000, 10_000), 2)
        gst_itc       = round(random.gauss(55_000, 5_000), 2)
        net_gst       = round(gst_collected - gst_itc, 2)
        wtax          = round(random.gauss(12_000, 1_500), 2)
        total_payable = round(net_gst + wtax, 2)
        is_lodged     = lodged_dt is not None
        rows.append({
            "quarter":           qname,
            "period_from":       period_from,
            "period_to":         period_to,
            "gst_collected":     gst_collected,
            "gst_itc":           gst_itc,
            "net_gst":           net_gst,
            "withheld_tax_payg": wtax,
            "total_payable":     total_payable,
            "due_date":          str(due_dt),
            "lodged_date":       lodged_dt,
            "lodgement_status":  "Lodged" if is_lodged else "Due",
            "payment_status":    "Paid" if is_lodged else "Pending",
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
            "entity_from":    "RMIT University",
            "entity_to":      "RMIT UP Pty Ltd",
            "description":    "Shared services recharge – Finance, HR, IT, Legal",
            "amount":         charge,
            "account_dr":     "5300",
            "account_cr":     "2400",
            "status":         "Matched" if random.random() > 0.05 else "Unmatched",
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
        # Expense budget: tighter than actuals (payroll capped, opex restrained)
        budget_exp  = round((620_000 * 1.12 + 620_000 * 0.0485 + 65_000 + 42_000
                             + 28_000 + 35_000 + 12_000 + 83_333) * 1.02, 0)
        rows.append({
            "period":       period,
            "budget_revenue":  budget_rev,
            "budget_expenses": budget_exp,
            "budget_net":      round(budget_rev - budget_exp, 0),
        })
    return pd.DataFrame(rows)


def gen_tax_config():
    """Export TAX_CONFIG as a database table — single source of truth for all rates."""
    descriptions = {
        "SGC_RATE":                      ("Superannuation Guarantee Rate",          "Payroll", "1 Jul 2025 — 12%"),
        "VIC_PAYROLL_TAX_RATE":          ("Victorian Payroll Tax Rate",              "Payroll", "Ongoing"),
        "VIC_PAYROLL_TAX_THRESHOLD_ANN": ("Victorian Payroll Tax Annual Threshold",   "Payroll", "1 Jul 2025 — increased to $1M"),
        "GST_RATE":                      ("Goods & Services Tax Rate",               "GST",     "Ongoing"),
        "FBT_GROSS_UP_TYPE1":            ("FBT Gross-Up Rate – Type 1 Benefits",     "FBT",     "FY2026"),
        "FBT_GROSS_UP_TYPE2":            ("FBT Gross-Up Rate – Type 2 Benefits",     "FBT",     "FY2026"),
        "FBT_RATE":                      ("Fringe Benefits Tax Rate",                "FBT",     "FY2026"),
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
# DATABASE BUILDER
# ─────────────────────────────────────────────────────────────────────────────

def build_database():
    os.makedirs(DATA_DIR, exist_ok=True)

    print("Generating synthetic data...")
    coa        = gen_chart_of_accounts()
    ccs        = gen_cost_centres()
    custs      = gen_customers()
    gl         = gen_general_ledger()
    ar         = gen_accounts_receivable()
    bank       = gen_bank_transactions()
    fa         = gen_fixed_assets()
    dep_sched  = gen_depreciation_schedule(fa)
    ptax       = gen_payroll_tax()
    gst        = gen_gst_transactions()
    bas        = gen_bas_returns()
    checklist  = gen_month_end_checklist()
    ic         = gen_intercompany()
    tax_cfg    = gen_tax_config()
    budget     = gen_monthly_budget()

    print(f"Writing database to {DB_PATH}...")
    conn = sqlite3.connect(DB_PATH)

    tables = {
        "chart_of_accounts":      coa,
        "cost_centres":           ccs,
        "customers":              custs,
        "general_ledger":         gl,
        "accounts_receivable":    ar,
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
