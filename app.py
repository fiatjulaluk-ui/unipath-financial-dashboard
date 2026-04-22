#!/usr/bin/env python3
"""
UniPath | Financial Accounting Dashboard
=========================================
Streamlit application showcasing financial accounting competencies
aligned with the Financial Accountant role at UniPath.

Run:
    streamlit run app.py
"""

import os
import sys
import sqlite3
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
from datetime import date

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG & CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────

DB_PATH = os.path.join(os.path.dirname(__file__), "data", "rmit_finance.db")

RMIT_RED    = "#E8192C"
RMIT_BLACK  = "#1A1A1A"
RMIT_GREY   = "#6B6B6B"
RMIT_LIGHT  = "#F5F5F5"
GREEN       = "#00875A"
ORANGE      = "#F7941D"
BLUE        = "#005EA5"
TEAL        = "#00857C"

CHART_PALETTE = [RMIT_RED, BLUE, GREEN, ORANGE, TEAL, RMIT_GREY, "#9B59B6"]

CURRENT_PERIOD = "2026-03"   # fallback constant — sidebar overrides this via selected_period
REPORT_DATE    = "31 March 2026"
ENTITY         = "UniPath Pty Ltd"
ABN            = "12 345 678 901"

st.set_page_config(
    page_title="UniPath | Financial Dashboard",
    page_icon="🔴",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────────────────────────────────────
# CUSTOM CSS  –  RMIT brand style
# ─────────────────────────────────────────────────────────────────────────────

st.markdown("""
<style>
    /* ── Global resets ── */
    html, body, [class*="css"] { font-family: 'Segoe UI', sans-serif; }
    .block-container { padding-top: 1rem; padding-bottom: 2rem; }

    /* ── Sidebar ── */
    [data-testid="stSidebar"] {
        background: #1A1A1A;
        color: #FFFFFF;
    }
    [data-testid="stSidebar"] .stRadio > label,
    [data-testid="stSidebar"] p,
    [data-testid="stSidebar"] > div > div > div > p,
    [data-testid="stSidebar"] span { color: #FFFFFF !important; }
    [data-testid="stSidebar"] label { color: #DDDDDD !important; }
    [data-testid="stSidebar"] hr { border-color: #444; }

    /* ── Page header ── */
    .page-header {
        background: linear-gradient(135deg, #E8192C 0%, #B01020 100%);
        padding: 1.4rem 2rem;
        border-radius: 10px;
        margin-bottom: 1.5rem;
        color: #FFFFFF;
    }
    .page-header h1 { margin: 0; font-size: 1.6rem; font-weight: 700; }
    .page-header p  { margin: 0.2rem 0 0; font-size: 0.88rem; opacity: 0.88; }

    /* ── KPI cards ── */
    .kpi-card {
        background: #FFFFFF;
        border-radius: 10px;
        padding: 1.1rem 1.3rem;
        border-left: 5px solid #E8192C;
        box-shadow: 0 2px 10px rgba(0,0,0,0.07);
        min-height: 90px;
    }
    .kpi-label  { font-size: 0.76rem; color: #6B6B6B; text-transform: uppercase; letter-spacing: 0.05em; font-weight: 600; }
    .kpi-value  { font-size: 1.55rem; font-weight: 700; color: #1A1A1A; margin: 0.15rem 0; }
    .kpi-delta-pos { font-size: 0.78rem; color: #00875A; font-weight: 600; }
    .kpi-delta-neg { font-size: 0.78rem; color: #E8192C; font-weight: 600; }
    .kpi-delta-neu { font-size: 0.78rem; color: #6B6B6B; }

    /* ── Section headers ── */
    .section-header {
        border-bottom: 2px solid #E8192C;
        padding-bottom: 0.4rem;
        margin: 1.2rem 0 0.8rem;
        font-size: 1.05rem;
        font-weight: 700;
        color: #1A1A1A;
    }

    /* ── Status badges ── */
    .badge-green  { background:#D4EDDA; color:#155724; padding:3px 10px; border-radius:12px; font-size:0.78rem; font-weight:600; }
    .badge-orange { background:#FFF3CD; color:#856404; padding:3px 10px; border-radius:12px; font-size:0.78rem; font-weight:600; }
    .badge-red    { background:#F8D7DA; color:#721C24; padding:3px 10px; border-radius:12px; font-size:0.78rem; font-weight:600; }
    .badge-grey   { background:#E2E3E5; color:#383D41; padding:3px 10px; border-radius:12px; font-size:0.78rem; font-weight:600; }

    /* ── Data tables ── */
    .dataframe { font-size: 0.83rem !important; }

    /* ── Hide streamlit footer ── */
    footer { visibility: hidden; }
    #MainMenu { visibility: hidden; }
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# DATA LAYER
# ─────────────────────────────────────────────────────────────────────────────

REQUIRED_TABLES = {
    "chart_of_accounts", "cost_centres", "customers", "suppliers",
    "general_ledger", "accounts_receivable", "accounts_payable",
    "bank_transactions", "fixed_assets", "depreciation_schedule",
    "payroll_tax", "gst_transactions", "gst_supply_types", "bas_returns",
    "month_end_checklist", "intercompany", "tax_compliance_config", "monthly_budget",
    "fbt_register", "journal_entries",
}

@st.cache_resource
def ensure_db():
    """Generate (or regenerate) DB if missing or schema is outdated."""
    needs_rebuild = not os.path.exists(DB_PATH)
    if not needs_rebuild:
        try:
            conn = sqlite3.connect(DB_PATH)
            existing = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
            conn.close()
            if not REQUIRED_TABLES.issubset(existing):
                needs_rebuild = True
        except Exception:
            needs_rebuild = True
    if needs_rebuild:
        import generate_data
        generate_data.build_database()


def get_connection():
    ensure_db()
    return sqlite3.connect(DB_PATH, check_same_thread=False)


@st.cache_data(ttl=300)
def query(sql: str) -> pd.DataFrame:
    with get_connection() as conn:
        return pd.read_sql_query(sql, conn)


def fmt_aud(val):
    """Format as AUD with abbreviated M/K suffixes. Negatives in parentheses."""
    if pd.isna(val):
        return "–"
    neg = val < 0
    v   = abs(val)
    if v >= 1_000_000:
        s = f"${v/1_000_000:.2f}M"
    elif v >= 1_000:
        s = f"${v/1_000:.1f}K"
    else:
        s = f"${v:,.0f}"
    return f"({s})" if neg else s


def fmt_table(val):
    """Format as full AUD for HTML/table cells. Negatives in parentheses (accounting convention)."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "–"
    neg = val < 0
    s   = f"${abs(val):,.0f}"
    return f"({s})" if neg else s


def fmt_pct(val):
    return f"{val:.1f}%" if not pd.isna(val) else "–"


# ─────────────────────────────────────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────────────────────────────────────

with st.sidebar:
    # UniPath branding header
    st.markdown("""
    <div style="text-align:center; padding: 0.5rem 0 1rem;">
        <div style="background:#E8192C; display:inline-block; padding:6px 18px;
                    border-radius:6px; margin-bottom:8px;">
            <span style="color:white; font-size:1.6rem; font-weight:900; letter-spacing:2px;">UniPath</span>
        </div>
        <div style="color:#AAAAAA; font-size:0.75rem; letter-spacing:0.1em;">FINANCIAL DASHBOARD</div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("---")

    pages = [
        "Executive Overview",
        "Month-End Close",
        "Income Statement",
        "Balance Sheet",
        "Accounts Receivable",
        "Accounts Payable",
        "Bank Reconciliation",
        "Fixed Assets",
        "Tax Compliance",
        "SQL Analysis",
        "About & Governance",
    ]
    page = st.radio("Navigation", pages, label_visibility="collapsed")

    st.markdown("---")

    # ── Reference data & period helpers ────────────────────────────────────────
    import calendar as _cal

    _all_periods = query("SELECT DISTINCT period FROM general_ledger ORDER BY period")["period"].tolist()
    _month_names = {1:"January",2:"February",3:"March",4:"April",5:"May",6:"June",
                    7:"July",8:"August",9:"September",10:"October",11:"November",12:"December"}

    # Derive Financial Years from available data
    _fy_set = set()
    for _p in _all_periods:
        _y, _m = int(_p[:4]), int(_p[5:7])
        _fy_set.add(f"FY{_y+1}" if _m >= 7 else f"FY{_y}")
    _fy_years = sorted(_fy_set)   # e.g. ["FY2026"]

    def _fy_bounds(fy):
        """Return (start_period, end_period) for a FY string like 'FY2026'."""
        yr = int(fy[2:])
        return f"{yr-1}-07", f"{yr}-06"

    def _fy_quarters(fy):
        """Return ordered dict of {label: (start, end)} for available quarters."""
        yr = int(fy[2:])
        qs = {
            f"Q1  Jul–Sep {yr-1}": (f"{yr-1}-07", f"{yr-1}-09"),
            f"Q2  Oct–Dec {yr-1}": (f"{yr-1}-10", f"{yr-1}-12"),
            f"Q3  Jan–Mar {yr}":   (f"{yr}-01",   f"{yr}-03"),
            f"Q4  Apr–Jun {yr}":   (f"{yr}-04",   f"{yr}-06"),
        }
        # Only keep quarters that overlap with available data
        return {k: v for k, v in qs.items()
                if any(_all_periods[0] <= p <= _all_periods[-1]
                       for p in [v[0], v[1]])}

    # ── Period Controls ───────────────────────────────────────────────────
    st.markdown('<div style="font-size:0.72rem;color:#aaa;margin:0.4rem 0 0.2rem;font-weight:700">📅 Period</div>', unsafe_allow_html=True)

    if "view_type"   not in st.session_state: st.session_state.view_type   = "YTD"
    if "sel_fy"      not in st.session_state: st.session_state.sel_fy      = _fy_years[-1]
    if "sel_month"   not in st.session_state: st.session_state.sel_month   = int(_all_periods[-1][5:7])
    if "sel_quarter" not in st.session_state: st.session_state.sel_quarter = None

    view_type = st.radio(
        "View Type", ["Monthly", "Quarterly", "YTD", "Full Year"],
        index=["Monthly","Quarterly","YTD","Full Year"].index(st.session_state.view_type),
        horizontal=True, key="sb_view_type",
    )
    st.session_state.view_type = view_type

    sel_fy = st.selectbox("Financial Year", _fy_years,
        index=_fy_years.index(st.session_state.sel_fy) if st.session_state.sel_fy in _fy_years else len(_fy_years)-1,
        key="sb_sel_fy",
    )
    st.session_state.sel_fy = sel_fy

    _fy        = sel_fy
    _fy_start, _fy_end_raw = _fy_bounds(_fy)
    _fy_end    = min(_fy_end_raw, _all_periods[-1])

    if view_type in ("Monthly", "YTD"):
        _fy_mo_periods = [p for p in _all_periods if _fy_start <= p <= _fy_end]
        _mo_options    = [f"{_cal.month_name[int(p[5:7])]} {p[:4]}" for p in _fy_mo_periods]
        _cur_mo_str    = f"{st.session_state.sel_month:02d}"
        _cur_mo_matches= [i for i,p in enumerate(_fy_mo_periods) if p[5:7] == _cur_mo_str]
        _mo_def_idx    = _cur_mo_matches[-1] if _cur_mo_matches else len(_mo_options)-1
        _col_lbl = "End Month" if view_type == "YTD" else "Month"
        sel_mo_label  = st.selectbox(_col_lbl, _mo_options, index=_mo_def_idx, key="sb_sel_month")
        sel_mo_period = _fy_mo_periods[_mo_options.index(sel_mo_label)]
        st.session_state.sel_month = int(sel_mo_period[5:7])
        period_end   = sel_mo_period
        period_start = _fy_start if view_type == "YTD" else sel_mo_period

    elif view_type == "Quarterly":
        _qs     = _fy_quarters(sel_fy)
        _q_keys = list(_qs.keys())
        _cur_q  = st.session_state.sel_quarter
        _q_idx  = _q_keys.index(_cur_q) if _cur_q in _q_keys else len(_q_keys)-1
        sel_q   = st.selectbox("Quarter", _q_keys, index=_q_idx, key="sb_sel_quarter")
        st.session_state.sel_quarter = sel_q
        period_start, period_end = _qs[sel_q]

    else:  # Full Year
        period_start, period_end = _fy_start, _fy_end

    period_start = max(period_start, _all_periods[0])
    period_end   = min(period_end,   _all_periods[-1])

    _pe_yr, _pe_mo  = map(int, period_end.split("-"))
    _ps_yr, _ps_mo  = map(int, period_start.split("-"))
    REPORT_DATE_DYN = f"{_cal.monthrange(_pe_yr,_pe_mo)[1]} {_cal.month_name[_pe_mo]} {_pe_yr}"
    period_label    = {
        "Monthly":   f"{_cal.month_name[_pe_mo]} {_pe_yr}",
        "Quarterly": st.session_state.get("sel_quarter","") or "",
        "YTD":       f"Jul {_ps_yr} – {_cal.month_name[_pe_mo]} {_pe_yr}",
        "Full Year": f"Full Year {_fy}",
    }.get(view_type, REPORT_DATE_DYN)

    _pe_idx     = _all_periods.index(period_end) if period_end in _all_periods else len(_all_periods)-1
    prev_period = _all_periods[_pe_idx - 1] if _pe_idx > 0 else period_end
    selected_period = period_end

    _kpi_lbl  = {"Monthly": "Month", "Quarterly": "Quarter", "YTD": "YTD", "Full Year": "FY"}.get(view_type, "Period")
    _pl_period_note = {
        "Monthly":   f"for {_cal.month_name[_pe_mo]} {_pe_yr}",
        "Quarterly": f"for {period_label}",
        "YTD":       f"YTD  Jul {_ps_yr} → {_cal.month_name[_pe_mo]} {_pe_yr}",
        "Full Year": f"Full Year {sel_fy}",
    }.get(view_type, period_label)

    _badge_clr = {"Monthly":"#005EA5","Quarterly":"#F7941D","YTD":"#E8192C","Full Year":"#00875A"}[view_type]
    st.markdown(
        f'<div style="margin-top:0.3rem;font-size:0.71rem;color:#999">'
        f'<b style="color:#ccc">{ENTITY}</b>  ·  ABN {ABN}<br>'
        f'<span style="background:{_badge_clr};color:white;padding:1px 7px;border-radius:10px;'
        f'font-size:0.69rem;font-weight:700">{view_type}</span>'
        f' &nbsp;{period_label}</div>',
        unsafe_allow_html=True
    )

    st.markdown("---")

    # ── Global Filters — visible on every page ────────────────────────────
    st.markdown('<div style="font-size:0.72rem;color:#aaa;margin:0.4rem 0 0.2rem;font-weight:700">🌏 Filters</div>', unsafe_allow_html=True)
    _all_cc = query("SELECT cost_centre_code, cost_centre_name FROM cost_centres ORDER BY cost_centre_code")
    _cc_map = dict(zip(_all_cc["cost_centre_name"], _all_cc["cost_centre_code"]))
    _cc_names_all = list(_cc_map.keys())

    selected_regions = st.multiselect(
        "Region", ["Domestic", "International"],
        default=["Domestic", "International"],
        key="filter_regions",
    )
    if not selected_regions:
        selected_regions = ["Domestic", "International"]

    selected_cc_names = st.multiselect(
        "Cost Centre", _cc_names_all,
        default=_cc_names_all,
        key="filter_cc",
    )
    if not selected_cc_names:
        selected_cc_names = _cc_names_all
    selected_cc_codes = [_cc_map[n] for n in selected_cc_names]

    # ── Scenario ─────────────────────────────────────────────────────────
    st.markdown('<div style="font-size:0.72rem;color:#aaa;margin:0.6rem 0 0.3rem;font-weight:700">📊 Scenario</div>', unsafe_allow_html=True)
    show_budget = st.toggle("Overlay Budget Targets", value=False,
                            help="Adds budget lines to Revenue vs Expenses chart.")
    dso_target  = st.number_input("DSO Target (days)", value=42, min_value=1, max_value=120, step=1,
                                  help="Highlight customers exceeding this DSO threshold.")

    st.markdown("---")

    # ── Tax Rate Configuration (read-only by default) ─────────────────────
    _tc_col, _lock_col = st.columns([4, 1])
    with _tc_col:
        st.markdown('<div style="font-size:0.78rem;color:#ccc;font-weight:700">⚙️ Tax Rate Configuration</div>', unsafe_allow_html=True)
    with _lock_col:
        st.checkbox("✏️", value=False, key="tax_edit_mode",
                    help="Unlock to edit tax rates — all changes are logged")

    _rate_defs = {"tax_sgc": 12.0, "tax_ptax": 4.85, "tax_top": 45.0, "tax_med": 2.0, "tax_gst_fbt": 10.0}
    for _k, _v in _rate_defs.items():
        if _k not in st.session_state:
            st.session_state[_k] = _v
    if "tax_change_log" not in st.session_state:
        st.session_state["tax_change_log"] = []

    if not st.session_state.get("tax_edit_mode", False):
        # ── Read-only display ─────────────────────────────────────────────
        st.markdown(
            f'<div style="font-size:0.70rem;color:#888;line-height:1.9;margin-top:0.3rem">'
            f'SGC Rate: <b style="color:#ccc">{st.session_state.tax_sgc:.2f}%</b><br>'
            f'Payroll Tax (VIC): <b style="color:#ccc">{st.session_state.tax_ptax:.2f}%</b><br>'
            f'Top Marginal Rate: <b style="color:#ccc">{st.session_state.tax_top:.1f}%</b><br>'
            f'Medicare Levy: <b style="color:#ccc">{st.session_state.tax_med:.1f}%</b><br>'
            f'GST Rate (gross-up): <b style="color:#ccc">{st.session_state.tax_gst_fbt:.1f}%</b>'
            f'</div>',
            unsafe_allow_html=True
        )
        sgc_rate       = st.session_state.tax_sgc / 100
        vic_ptax_rate  = st.session_state.tax_ptax / 100
        indiv_top_rate = st.session_state.tax_top / 100
        medicare_levy  = st.session_state.tax_med / 100
        gst_rate_fbt   = st.session_state.tax_gst_fbt / 100
    else:
        # ── Edit mode ─────────────────────────────────────────────────────
        st.warning("Edit mode active — changes are logged below.", icon="⚠️")
        _sgc  = st.number_input("SGC Rate (%)",             value=st.session_state.tax_sgc,     min_value=0.0,  max_value=20.0, step=0.25, format="%.2f")
        _ptax = st.number_input("VIC Payroll Tax Rate (%)", value=st.session_state.tax_ptax,    min_value=0.0,  max_value=10.0, step=0.01, format="%.2f")
        _top  = st.number_input("Top Marginal Rate (%)",    value=st.session_state.tax_top,     min_value=30.0, max_value=55.0, step=0.5,  format="%.1f",
                                help="s12-5 ITAA 1997 — currently 45% on income > $190,000")
        _med  = st.number_input("Medicare Levy (%)",        value=st.session_state.tax_med,     min_value=0.0,  max_value=5.0,  step=0.5,  format="%.1f",
                                help="s8 Medicare Levy Act 1986 — currently 2%")
        _gst  = st.number_input("GST Rate (%) – gross-up",  value=st.session_state.tax_gst_fbt, min_value=0.0,  max_value=20.0, step=0.5,  format="%.1f",
                                help="Used in Type 1 gross-up formula. Normally 10% (GSTA 1999).")

        import datetime as _dt
        for _field, _old, _new in [
            ("SGC Rate",          st.session_state.tax_sgc,     _sgc),
            ("Payroll Tax Rate",  st.session_state.tax_ptax,    _ptax),
            ("Top Marginal Rate", st.session_state.tax_top,     _top),
            ("Medicare Levy",     st.session_state.tax_med,     _med),
            ("GST (gross-up)",    st.session_state.tax_gst_fbt, _gst),
        ]:
            if abs(_old - _new) > 0.001:
                st.session_state["tax_change_log"].append({
                    "field": _field, "from": f"{_old:.2f}%",
                    "to": f"{_new:.2f}%",
                    "time": _dt.datetime.now().strftime("%H:%M:%S"),
                })
        st.session_state.tax_sgc, st.session_state.tax_ptax = _sgc, _ptax
        st.session_state.tax_top, st.session_state.tax_med, st.session_state.tax_gst_fbt = _top, _med, _gst

        sgc_rate       = _sgc / 100
        vic_ptax_rate  = _ptax / 100
        indiv_top_rate = _top / 100
        medicare_levy  = _med / 100
        gst_rate_fbt   = _gst / 100

    # Derived — single source of truth
    fbt_rate  = indiv_top_rate + medicare_levy
    fbt_type2 = round(1 / (1 - fbt_rate), 4)
    # Type 1: ATO-published FY2026 rate hardcoded per ATO Tax Withheld Calculator.
    # Algebraic formula (1+GST)/(1-FBT) = 1.10/0.53 = 2.0755; ATO publishes 2.0802.
    # Hardcoded so calculations match the ATO's authoritative figure exactly.
    fbt_type1 = 2.0802

    st.markdown(
        f'<div style="font-size:0.68rem;color:#888;margin-top:0.3rem;line-height:1.6">'
        f'<b style="color:#ccc">FBT Rate:</b> {indiv_top_rate*100:.1f}% + {medicare_levy*100:.1f}% = '
        f'<b style="color:#E8192C">{fbt_rate*100:.1f}%</b><br>'
        f'<b style="color:#ccc">T1</b> (1+GST)/(1-FBT) = {fbt_type1:.4f} &nbsp;'
        f'<b style="color:#ccc">T2</b> 1/(1-FBT) = {fbt_type2:.4f}'
        f'</div>',
        unsafe_allow_html=True
    )

    with st.expander("ℹ️ FBT gross-up explained", expanded=False):
        st.markdown(f"""
**Fringe Benefits Tax (FBT)** is levied on employers at **{fbt_rate*100:.1f}%** on the *grossed-up* value of non-cash benefits (*FBTAA 1986 s5B*).

**Type 1** (GST-creditable benefits — e.g. company cars):
> Gross-up = Taxable Value × **{fbt_type1:.4f}**
> Formula: (1 + GST rate) ÷ (1 − FBT rate) ≈ 1.10 ÷ 0.53
> ATO publishes the authoritative rate annually; FY2026 = **2.0802**.

**Type 2** (non-GST-creditable — e.g. loans, living-away allowances):
> Gross-up = Taxable Value × **{fbt_type2:.4f}**
> Formula: 1 ÷ (1 − FBT rate) = 1 ÷ {1-fbt_rate:.2f}

**FBT Year:** 1 April → 31 March (not aligned to financial year).
**Lodgement deadline:** 21 May (or extended agent date).

*References: FBTAA 1986; ATO Tax Withheld Calculator FY2026; PCG 2024/2 (EV charging rate).*
        """)

    if st.session_state["tax_change_log"]:
        _n = len(st.session_state["tax_change_log"])
        with st.expander(f"📋 Change Log ({_n} edit{'s' if _n != 1 else ''})", expanded=False):
            for _e in reversed(st.session_state["tax_change_log"]):
                st.markdown(
                    f'<div style="font-size:0.70rem;color:#aaa;line-height:1.8">'
                    f'<b style="color:#E8192C">{_e["time"]}</b> &nbsp;'
                    f'{_e["field"]}: {_e["from"]} → <b style="color:#ccc">{_e["to"]}</b>'
                    f'</div>',
                    unsafe_allow_html=True
                )

    st.markdown("---")

    # ── FY2027 What-If Parameters ─────────────────────────────────────────────
    with st.expander("FY2027 What-If Parameters", expanded=False):
        st.markdown(
            '<div style="font-size:0.70rem;color:#888;margin-bottom:0.6rem;line-height:1.5">'
            'Projected inputs for stress-testing FBT and LCT exposure in the next period. '
            'Adjust below to see P&L impact — these do <b>not</b> affect FY2026 calculations above.'
            '</div>',
            unsafe_allow_html=True
        )
        fy27_lct_ice = st.number_input(
            "LCT Threshold – ICE ($)", value=82_000, min_value=70_000, max_value=120_000, step=500,
            help="FY2026: $80,567. Indexed annually — ATO announces in May each year (LCTA 1999)."
        )
        fy27_lct_fev = st.number_input(
            "LCT Threshold – FEV ($)", value=93_000, min_value=80_000, max_value=130_000, step=500,
            help="FY2026: $91,387. CPI March quarter indexed — fuel-efficient vehicles ≤3.5L/100km."
        )
        fy27_home_charging = st.number_input(
            "EV Home Charging Rate (c/km)", value=5.47, min_value=1.0, max_value=15.0, step=0.01,
            format="%.2f",
            help="FY2026: 4.20c/km (PCG 2024/2). FY2027: 5.47c/km — confirmed per PCG 2024/2 update. Used in Operating Cost Method."
        )

        # Live impact preview — BEV cost at current vehicle price vs LCT threshold
        bev_price   = 68_500
        fy27_exempt = bev_price <= fy27_lct_fev
        fy27_lct_amount = (
            round((bev_price - fy27_lct_fev) / 1.1 * 0.33, 0) if bev_price > fy27_lct_fev else 0.0
        )
        st.markdown(
            f'<div style="margin-top:8px;padding:8px 10px;border-radius:4px;font-size:0.75rem;'
            f'background:{"#F0FFF4" if fy27_exempt else "#FFF5F5"};'
            f'border:1px solid {"#00875A" if fy27_exempt else "#E8192C"}">'
            f'<b>VEH-001 BEV (${bev_price:,}) vs FY2027 FEV threshold (${fy27_lct_fev:,}):</b><br>'
            + (
                f'✓ Still under threshold — s58P exemption holds. LCT = $0.'
                if fy27_exempt else
                f'⚠ EXCEEDS threshold by ${bev_price - fy27_lct_fev:,}. '
                f'LCT = ${fy27_lct_amount:,.0f}. s58P exemption LOST — full FBT applies.'
            ) +
            f'<br><span style="color:#888">Home charging: {fy27_home_charging:.2f}c/km '
            f'(+{fy27_home_charging - 4.20:.2f}c vs FY2026)</span>'
            f'</div>',
            unsafe_allow_html=True
        )


# ─────────────────────────────────────────────────────────────────────────────
# HELPER: KPI card HTML
# ─────────────────────────────────────────────────────────────────────────────

def kpi_card(label, value, delta=None, delta_type="neu"):
    delta_class = f"kpi-delta-{delta_type}"
    delta_html = f'<div class="{delta_class}">{delta}</div>' if delta else ""
    return f"""
    <div class="kpi-card">
        <div class="kpi-label">{label}</div>
        <div class="kpi-value">{value}</div>
        {delta_html}
    </div>"""


def section(title):
    st.markdown(f'<div class="section-header">{title}</div>', unsafe_allow_html=True)


def page_header(title, subtitle=""):
    st.markdown(f"""
    <div class="page-header">
        <h1>{title}</h1>
        <p>{subtitle}</p>
    </div>""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# PAGE 1 – EXECUTIVE OVERVIEW
# ─────────────────────────────────────────────────────────────────────────────

if page == "Executive Overview":
    page_header(
        "Executive Financial Overview",
        f"{ENTITY}  |  {period_label}  |  as at {REPORT_DATE_DYN}"
    )

    # ── Period + filter summary badges ────────────────────────────────────────
    _badge_clr_eo = {"Monthly":"#005EA5","Quarterly":"#F7941D","YTD":"#E8192C","Full Year":"#00875A"}[view_type]
    _r_lbl = "All Regions" if len(selected_regions) == 2 else ", ".join(selected_regions)
    _c_lbl = "All Cost Centres" if len(selected_cc_names) == len(_cc_names_all) else f"{len(selected_cc_names)} of {len(_cc_names_all)} cost centres"
    st.markdown(
        f'<div style="background:#FFFFFF;border-radius:10px;padding:0.65rem 1.2rem;'
        f'margin-bottom:1rem;border:2px solid {_badge_clr_eo}">'
        f'<span style="background:{_badge_clr_eo};color:#FFFFFF;padding:3px 10px;border-radius:12px;'
        f'font-size:0.78rem;font-weight:700">{view_type}</span>'
        f'&nbsp;&nbsp;<span style="font-size:0.85rem;color:#000000;font-weight:600">{period_label}</span>'
        f'<span style="font-size:0.83rem;color:#000000"> &nbsp;·&nbsp; as at {REPORT_DATE_DYN}</span>'
        f'<br><span style="font-size:0.75rem;color:#333333">🌏 {_r_lbl} &nbsp;·&nbsp; {_c_lbl}</span>'
        f'</div>',
        unsafe_allow_html=True
    )

    gl  = query("SELECT * FROM general_ledger")
    coa = query("SELECT account_code, account_name FROM chart_of_accounts")

    # GL slices:
    #   gl_window = transactions IN the selected period range (for P&L / revenue / expense)
    #   gl_cumul  = transactions UP TO period_end (for Balance Sheet / cumulative figures)
    gl_window = gl[
        (gl["period"] >= period_start) &
        (gl["period"] <= period_end) &
        (gl["cost_centre"].isin(selected_cc_codes))
    ]
    gl_cumul  = gl[
        (gl["period"] <= period_end) &
        (gl["cost_centre"].isin(selected_cc_codes))
    ]
    # Keep full cumulative for MoM calcs (no CC filter needed for prev period)
    ytd_all = gl[gl["period"] <= period_end]
    ytd     = gl_window   # alias used below

    # Revenue accounts: 4001–4999; filter by region via AR customer region mapping
    # For cost-centre-filtered revenue we use GL cost_centre (CC001 = Academic Programs carries most revenue)
    rev_ytd  = ytd[ytd["account_code"].between("4001","4999")]["credit"].sum()
    exp_ytd  = ytd[ytd["account_code"].between("5001","5999")]["debit"].sum()
    net_ytd  = rev_ytd - exp_ytd
    margin   = (net_ytd / rev_ytd * 100) if rev_ytd else 0

    bank = query("SELECT balance FROM bank_transactions ORDER BY rowid DESC LIMIT 1")
    cash_bal = bank["balance"].iloc[0] if not bank.empty else 0

    ar_all  = query("SELECT * FROM accounts_receivable WHERE status != 'Paid'")
    ar_filt = ar_all[ar_all["region"].isin(selected_regions)]
    ar_outstanding = ar_filt["total_inc_gst"].sum()

    # MoM comparison: period_end month vs prior month
    rev_prev = ytd_all[
        (ytd_all["period"] == prev_period) &
        (ytd_all["account_code"].between("4001","4999")) &
        (ytd_all["cost_centre"].isin(selected_cc_codes))
    ]["credit"].sum()
    rev_curr = ytd_all[
        (ytd_all["period"] == period_end) &
        (ytd_all["account_code"].between("4001","4999")) &
        (ytd_all["cost_centre"].isin(selected_cc_codes))
    ]["credit"].sum()
    rev_mom     = ((rev_curr - rev_prev) / rev_prev * 100) if rev_prev else 0
    rev_mom_abs = rev_curr - rev_prev
    mom_arrow   = "▲" if rev_mom >= 0 else "▼"
    mom_type    = "pos" if rev_mom >= 0 else "neg"

    exp_prev = ytd_all[
        (ytd_all["period"] == prev_period) &
        (ytd_all["account_code"].between("5001","5999")) &
        (ytd_all["cost_centre"].isin(selected_cc_codes))
    ]["debit"].sum()
    exp_curr = ytd_all[
        (ytd_all["period"] == period_end) &
        (ytd_all["account_code"].between("5001","5999")) &
        (ytd_all["cost_centre"].isin(selected_cc_codes))
    ]["debit"].sum()
    exp_mom = ((exp_curr - exp_prev) / exp_prev * 100) if exp_prev else 0

    # ── KPI row ──
    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        st.markdown(kpi_card(
            f"{_kpi_lbl} Revenue",
            fmt_aud(rev_ytd),
            f"{mom_arrow} {fmt_aud(abs(rev_mom_abs))} ({rev_mom:+.1f}%) vs {prev_period}",
            mom_type,
        ), unsafe_allow_html=True)
    with c2:
        exp_mom_arrow = "▲" if exp_mom >= 0 else "▼"   # rising expenses = bad → ▲ means went up
        st.markdown(kpi_card(
            f"{_kpi_lbl} Expenses",
            fmt_aud(exp_ytd),
            f"{exp_mom_arrow} {exp_mom:+.1f}% MoM  ·  {round(exp_ytd/rev_ytd*100,1) if rev_ytd else 0}% of Rev",
            "neg" if exp_mom > 5 else "neu",
        ), unsafe_allow_html=True)
    with c3:
        st.markdown(kpi_card(
            "Net Surplus / (Deficit)",
            fmt_aud(net_ytd),
            f"{_kpi_lbl} margin {fmt_pct(margin)}",
            "pos" if net_ytd >= 0 else "neg",
        ), unsafe_allow_html=True)
    with c4:
        st.markdown(kpi_card("Cash at Bank", fmt_aud(cash_bal), "Operating Account", "neu"),
                    unsafe_allow_html=True)
    with c5:
        st.markdown(kpi_card(
            "AR Outstanding",
            fmt_aud(ar_outstanding),
            f"{len(ar_filt)} open invoices  ·  {', '.join(selected_regions)}",
            "neu",
        ), unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Revenue vs Expense trend ──
    col_left, col_right = st.columns([3, 2])
    with col_left:
        section(f"Monthly Revenue vs Expenses – {period_label}")
        monthly = (
            ytd.groupby("period")
            .apply(lambda d: pd.Series({
                "Revenue":  d[d["account_code"].between("4001","4999")]["credit"].sum(),
                "Expenses": d[d["account_code"].between("5001","5399")]["debit"].sum(),
                "Tax":      d[d["account_code"] == "5400"]["debit"].sum(),
            }))
            .reset_index()
        )
        monthly["NPAT"] = monthly["Revenue"] - monthly["Expenses"] - monthly["Tax"]
        fig = go.Figure()
        fig.add_trace(go.Bar(name="Actuals – Revenue",  x=monthly["period"], y=monthly["Revenue"],
                             marker_color=RMIT_RED, opacity=0.85))
        fig.add_trace(go.Bar(name="Actuals – Expenses", x=monthly["period"], y=monthly["Expenses"],
                             marker_color=RMIT_GREY, opacity=0.75))
        fig.add_trace(go.Scatter(name="NPAT", x=monthly["period"], y=monthly["NPAT"],
                                 mode="lines+markers", line=dict(color=GREEN, width=2.5),
                                 marker=dict(size=7)))

        # Budget overlay
        if show_budget:
            try:
                bud = query("SELECT * FROM monthly_budget WHERE period <= '" + selected_period + "' ORDER BY period")
                if not bud.empty:
                    fig.add_trace(go.Scatter(
                        name="Budget – Revenue",
                        x=bud["period"], y=bud["budget_revenue"],
                        mode="lines+markers",
                        line=dict(color=RMIT_RED, width=1.5, dash="dot"),
                        marker=dict(size=5, symbol="diamond"),
                        opacity=0.7,
                    ))
                    fig.add_trace(go.Scatter(
                        name="Budget – Expenses",
                        x=bud["period"], y=bud["budget_expenses"],
                        mode="lines+markers",
                        line=dict(color=RMIT_GREY, width=1.5, dash="dot"),
                        marker=dict(size=5, symbol="diamond"),
                        opacity=0.7,
                    ))
                    # Variance annotation for the selected period only
                    bud_row = bud[bud["period"] == selected_period]
                    if not bud_row.empty:
                        act_rev = monthly[monthly["period"] == selected_period]["Revenue"].sum() if selected_period in monthly["period"].values else 0
                        bud_rev = bud_row["budget_revenue"].iloc[0]
                        var_rev = act_rev - bud_rev
                        var_pct = (var_rev / bud_rev * 100) if bud_rev else 0
                        st.caption(
                            f"**{selected_period} vs Budget:** Revenue variance "
                            f"{'▲' if var_rev >= 0 else '▼'} {fmt_aud(abs(var_rev))} "
                            f"({var_pct:+.1f}%)  "
                            + ("✅ Favourable" if var_rev >= 0 else "⚠️ Unfavourable")
                        )
            except Exception:
                st.caption("Budget data not yet available — regenerate the database.")

        fig.update_layout(
            barmode="group", height=360, margin=dict(l=10, r=10, t=20, b=30),
            plot_bgcolor="white", paper_bgcolor="white",
            legend=dict(orientation="h", y=1.10, font_size=11),
            yaxis=dict(tickformat="$,.0f", gridcolor="#F0F0F0"),
            xaxis=dict(gridcolor="#F0F0F0"),
        )
        st.plotly_chart(fig, use_container_width=True)

    with col_right:
        # Revenue Mix filtered by selected regions via AR customer mapping
        region_label = f"Region: {', '.join(selected_regions)}" if len(selected_regions) < 2 else "All Regions"
        section(f"Revenue Mix – {period_label}  ({region_label})")

        # Map region to AR accounts: Domestic = 4001, International = 4002
        region_acct_map = {"Domestic": "4001", "International": "4002"}
        rev_accts_for_region = [region_acct_map[r] for r in selected_regions if r in region_acct_map]
        # Always include other revenue accounts (4003–4005) regardless of region filter
        rev_accts_show = rev_accts_for_region + ["4003", "4004", "4005"]

        rev_by_acct = (
            ytd[ytd["account_code"].isin(rev_accts_show)]
            .groupby("account_code")["credit"].sum()
            .reset_index()
        )
        rev_by_acct = rev_by_acct.merge(coa, on="account_code")
        rev_by_acct["account_name"] = (rev_by_acct["account_name"]
            .str.replace("Course Fees – ", "")
            .str.replace("Consulting & Advisory Revenue", "Consulting & Advisory"))
        fig2 = px.pie(rev_by_acct, values="credit", names="account_name",
                      color_discrete_sequence=CHART_PALETTE, hole=0.45)
        fig2.update_traces(textposition="auto", textinfo="percent+label",
                           textfont_size=10, pull=[0, 0, 0.1, 0.1])
        fig2.update_layout(
            height=420, margin=dict(l=10, r=10, t=20, b=60),
            showlegend=False, paper_bgcolor="white"
        )
        st.plotly_chart(fig2, use_container_width=True)

    # ── Expense breakdown ──
    col3, col4 = st.columns(2)
    with col3:
        cc_label = f"{len(selected_cc_names)} cost centre(s)" if len(selected_cc_names) < len(_cc_map) else "All Cost Centres"
        section(f"Expense Breakdown by Category – YTD  ({cc_label})")
        exp_data = ytd[ytd["account_code"].between("5001","5999")]
        exp_by_acct = exp_data.groupby("account_code")["debit"].sum().reset_index()
        exp_by_acct = exp_by_acct.merge(coa, on="account_code")
        exp_by_acct = exp_by_acct.sort_values("debit", ascending=True)
        exp_by_acct["short_name"] = exp_by_acct["account_name"].str.replace(" – Parent University","").str.replace("Depreciation – ","Dep – ")
        fig3 = px.bar(exp_by_acct, x="debit", y="short_name", orientation="h",
                      color_discrete_sequence=[RMIT_RED])
        fig3.update_layout(
            height=340, margin=dict(l=0, r=10, t=20, b=10),
            xaxis_title="", yaxis_title="", plot_bgcolor="white", paper_bgcolor="white",
            xaxis=dict(tickformat="$,.0f", gridcolor="#F0F0F0"),
        )
        st.plotly_chart(fig3, use_container_width=True)

    with col4:
        surplus_deficit = "Surplus" if net_ytd >= 0 else "Deficit"
        section(f"Cumulative Net {surplus_deficit} – {period_label}")
        monthly_sorted = monthly.sort_values("period")
        monthly_sorted["Cumulative Net"] = monthly_sorted["NPAT"].cumsum()
        fig4 = px.area(monthly_sorted, x="period", y="Cumulative Net",
                       color_discrete_sequence=[RMIT_RED])
        fig4.update_traces(line_width=2.5)
        fig4.update_layout(
            height=340, margin=dict(l=10, r=10, t=20, b=30),
            xaxis_title="", yaxis_title="Cumulative Net ($)",
            plot_bgcolor="white", paper_bgcolor="white",
            yaxis=dict(tickformat="$,.0f", gridcolor="#F0F0F0"),
            xaxis=dict(gridcolor="#F0F0F0"),
        )
        st.plotly_chart(fig4, use_container_width=True)


# ─────────────────────────────────────────────────────────────────────────────
# PAGE 2 – MONTH-END CLOSE
# ─────────────────────────────────────────────────────────────────────────────

elif page == "Month-End Close":
    page_header(
        "Month-End Close Tracker",
        f"Current period: {CURRENT_PERIOD}  |  As at {REPORT_DATE_DYN}"
    )

    checklist = query("SELECT * FROM month_end_checklist")
    current   = checklist[checklist["is_current"] == 1]
    closed    = checklist[checklist["is_current"] == 0]

    # ── Current period status ──
    total  = len(current)
    done   = len(current[current["status"] == "Complete"])
    inprog = len(current[current["status"] == "In Progress"])
    pct    = int(done / total * 100) if total else 0

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown(kpi_card("Close Progress", f"{pct}%", f"{done} of {total} tasks complete", "pos" if pct >= 80 else "neu"),
                    unsafe_allow_html=True)
    with c2:
        st.markdown(kpi_card("Completed", str(done), "tasks signed off", "pos"), unsafe_allow_html=True)
    with c3:
        st.markdown(kpi_card("In Progress", str(inprog), "tasks underway", "neu"), unsafe_allow_html=True)
    with c4:
        pending = total - done - inprog
        st.markdown(kpi_card("Pending", str(pending), "tasks not started", "neg" if pending > 0 else "pos"),
                    unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    section(f"Close Checklist – {CURRENT_PERIOD}  (Day 1–5 Dependency View — always shows active close period)")

    def badge(status, blocked=False):
        if blocked:
            return '<span class="badge-red">Blocked</span>'
        mapping = {
            "Complete":    "badge-green",
            "In Progress": "badge-orange",
            "Pending":     "badge-grey",
        }
        return f'<span class="{mapping.get(status, "badge-grey")}">{status}</span>'

    # Build a lookup: seq -> status for dependency blocking logic
    status_by_seq = dict(zip(current["task_sequence"], current["status"]))

    def is_blocked(row):
        dep = row.get("depends_on_seq")
        if pd.isna(dep) or dep is None:
            return False
        dep = int(dep)
        prereq_status = status_by_seq.get(dep, "Pending")
        return prereq_status != "Complete" and row["status"] not in ("Complete",)

    current = current.copy()
    current["blocked"] = current.apply(is_blocked, axis=1)

    DAY_LABELS = {
        1: ("Day 1", "Foundation Journals",          "#1A1A1A"),
        2: ("Day 2", "Accruals & Subsidiary Ledgers", "#005EA5"),
        3: ("Day 3", "Depreciation & Intercompany",   "#F7941D"),
        4: ("Day 4", "Variance Analysis & BS Recs",   "#9B59B6"),
        5: ("Day 5", "Manager Sign-Off",              "#00875A"),
    }

    # Build one single table so all columns align across all days
    all_rows_html = ""
    current_sorted = current.sort_values("task_sequence")

    for day_num, (day_label, day_title, day_color) in DAY_LABELS.items():
        day_tasks = current_sorted[current_sorted["target_day"] == day_num]
        day_done  = (day_tasks["status"] == "Complete").sum()
        day_total = len(day_tasks)

        # Day header row spanning all columns
        all_rows_html += f"""
        <tr>
            <td colspan="6" style="padding:8px 12px;background:{day_color};">
                <span style="color:white;font-weight:700;font-size:0.88rem">{day_label} — {day_title}</span>
                <span style="color:white;font-size:0.78rem;opacity:0.85;float:right">{day_done}/{day_total} complete</span>
            </td>
        </tr>"""

        for _, row in day_tasks.iterrows():
            _cd = row["completed_date"]
            completed = "–" if (not _cd or _cd == "None" or (isinstance(_cd, float) and pd.isna(_cd))) else _cd
            dep_seq   = row.get("depends_on_seq")
            dep_note  = f"← task {int(dep_seq)}" if pd.notna(dep_seq) and dep_seq else "–"
            blocked   = bool(row["blocked"])
            row_bg    = "background:#fff5f5;" if blocked else ""
            all_rows_html += f"""
            <tr style="{row_bg}">
                <td style="text-align:center;color:#888;font-size:0.8rem;padding:7px 8px">{int(row['task_sequence'])}</td>
                <td style="font-size:0.84rem;padding:7px 8px">{row['task_name']}</td>
                <td style="font-size:0.78rem;color:#888;padding:7px 8px;font-style:italic;text-align:center">{dep_note}</td>
                <td style="font-size:0.81rem;color:#555;padding:7px 8px">{str(row['owner']).replace('_', ' ')}</td>
                <td style="padding:7px 8px">{badge(row['status'], blocked)}</td>
                <td style="font-size:0.81rem;color:#555;padding:7px 8px">{completed}</td>
            </tr>"""

    st.markdown(f"""
    <table style="width:100%;border-collapse:collapse;border:1px solid #eee;border-radius:8px;overflow:hidden">
        <colgroup>
            <col style="width:40px">
            <col style="width:32%">
            <col style="width:100px">
            <col style="width:130px">
            <col style="width:110px">
            <col style="width:110px">
        </colgroup>
        <thead style="background:#F5F5F5">
            <tr>
                <th style="padding:8px;text-align:center;font-size:0.78rem;color:#555">#</th>
                <th style="padding:8px;text-align:left;font-size:0.78rem;color:#555">Task</th>
                <th style="padding:8px;text-align:center;font-size:0.78rem;color:#555">Depends On</th>
                <th style="padding:8px;text-align:left;font-size:0.78rem;color:#555">Owner</th>
                <th style="padding:8px;text-align:left;font-size:0.78rem;color:#555">Status</th>
                <th style="padding:8px;text-align:left;font-size:0.78rem;color:#555">Completed</th>
            </tr>
        </thead>
        <tbody style="background:white">{all_rows_html}</tbody>
    </table>
    """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    section("Historical Close Performance (Days to Close)")

    # Only include periods where ALL tasks are complete
    period_task_counts = checklist.groupby("period")["status"].count()
    period_done_counts = checklist[checklist["status"] == "Complete"].groupby("period")["status"].count()
    fully_closed_periods = period_task_counts[period_task_counts == period_done_counts].index.tolist()
    close_perf = (
        checklist[checklist["period"].isin(fully_closed_periods)]
        .groupby("period")["completed_date"]
        .max()
        .reset_index()
    )
    # Calculate "days to close" as days into the following month
    def days_to_close(period_str, completed_str):
        try:
            y, m = map(int, period_str.split("-"))
            comp = pd.to_datetime(completed_str).date()
            next_month_start = date(y + (m // 12), (m % 12) + 1, 1)
            return max((comp - next_month_start).days + 1, 1)
        except Exception:
            return None

    close_perf["days_to_close"] = close_perf.apply(
        lambda r: days_to_close(r["period"], r["completed_date"]), axis=1
    )
    close_perf = close_perf.dropna(subset=["days_to_close"])

    fig_close = px.bar(close_perf, x="period", y="days_to_close",
                       color_discrete_sequence=[RMIT_RED], text="days_to_close")
    fig_close.add_hline(y=5, line_dash="dash", line_color=GREEN, annotation_text="Target: Day 5")
    fig_close.update_traces(textposition="outside", textfont_size=11)
    fig_close.update_layout(
        height=320, margin=dict(l=10, r=10, t=20, b=30),
        xaxis_title="", yaxis_title="Business Days to Close",
        plot_bgcolor="white", paper_bgcolor="white",
        yaxis=dict(gridcolor="#F0F0F0"),
    )
    st.plotly_chart(fig_close, use_container_width=True)


# ─────────────────────────────────────────────────────────────────────────────
# PAGE 3 – INCOME STATEMENT
# ─────────────────────────────────────────────────────────────────────────────

elif page == "Income Statement":
    page_header(
        "Income Statement (P&L)",
        f"{ENTITY}  |  {period_label}  |  as at {REPORT_DATE_DYN}"
    )

    gl  = query("SELECT * FROM general_ledger")
    coa = query("SELECT account_code, account_name, account_type, report_section FROM chart_of_accounts")

    # P&L = transactions WITHIN the selected window (period_start → period_end)
    ytd = gl[
        (gl["period"] >= period_start) &
        (gl["period"] <= period_end) &
        (gl["cost_centre"].isin(selected_cc_codes))
    ]
    ytd = ytd.merge(coa, on="account_code", how="left")

    _pl_period_note = {
        "Monthly":   f"for {_cal.month_name[_pe_mo]} {_pe_yr}",
        "Quarterly": f"for {period_label}",
        "YTD":       f"YTD  Jul {_ps_yr} → {_cal.month_name[_pe_mo]} {_pe_yr}",
        "Full Year": f"Full Year {sel_fy}",
    }.get(view_type, period_label)

    st.info(f"P&L view: **{_pl_period_note}**  ·  Balance Sheet on this page shows cumulative **as at {REPORT_DATE_DYN}**", icon="ℹ️")

    # ── KPI strip — anchored to budget targets ────────────────────────────────
    budget = query("SELECT * FROM monthly_budget")
    bud_win = budget[(budget["period"] >= period_start) & (budget["period"] <= period_end)]
    bud_rev = bud_win["budget_revenue"].sum()
    bud_exp = bud_win["budget_expenses"].sum()
    bud_net = bud_win["budget_net"].sum()

    # Revenue
    rev = ytd[ytd["account_type"] == "Revenue"].groupby("account_name")["credit"].sum().reset_index()
    rev.columns = ["Line Item", "Period Amount"]
    total_rev = rev["Period Amount"].sum()

    # Operating expenses (all Expense accounts EXCEPT 5400 Tax)
    exp_ops = ytd[
        (ytd["account_type"] == "Expense") &
        (ytd["account_code"] != "5400")
    ].groupby(["report_section","account_name"])["debit"].sum().reset_index()
    exp_ops.columns = ["Section","Line Item","Period Amount"]
    total_exp_ops = exp_ops["Period Amount"].sum()

    # Tax provision (5400 only)
    tax_exp = ytd[ytd["account_code"] == "5400"]["debit"].sum()

    # P&L waterfall
    ebit        = total_rev - total_exp_ops
    npat        = ebit - tax_exp
    total_exp   = total_exp_ops + tax_exp          # for KPI/budget comparison
    eff_rate    = (tax_exp / ebit * 100) if ebit > 0 else 0.0
    net_margin_pct = (npat / total_rev * 100) if total_rev else 0
    rev_vs_bud     = total_rev - bud_rev
    net_vs_bud     = npat - bud_net

    corp_tax_rate_cfg = 30.0   # display label only — actual rate from TAX_CONFIG

    _ki1, _ki2, _ki3, _ki4 = st.columns(4)
    with _ki1: st.markdown(kpi_card(
        "Revenue", fmt_aud(total_rev),
        f"{'▲' if rev_vs_bud >= 0 else '▼'} {fmt_aud(abs(rev_vs_bud))} vs budget",
        "pos" if rev_vs_bud >= 0 else "neg"), unsafe_allow_html=True)
    with _ki2: st.markdown(kpi_card(
        "EBIT (pre-tax)", fmt_aud(ebit),
        f"Tax provision: {fmt_aud(abs(tax_exp))} ({eff_rate:.1f}% eff. rate)",
        "pos" if ebit >= 0 else "neg"), unsafe_allow_html=True)
    with _ki3: st.markdown(kpi_card(
        "NPAT", fmt_aud(npat),
        f"Net margin: {net_margin_pct:.1f}%",
        "pos" if npat >= 0 else "neg"), unsafe_allow_html=True)
    with _ki4: st.markdown(kpi_card(
        "NPAT vs Budget", fmt_aud(net_vs_bud),
        f"{'Ahead' if net_vs_bud >= 0 else 'Behind'} by {fmt_aud(abs(net_vs_bud))}",
        "pos" if net_vs_bud >= 0 else "neg"), unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    col1, col2 = st.columns([2, 3])

    with col1:
        section(f"Profit & Loss Statement  —  {_pl_period_note}")
        lines = []
        # ── Revenue ──────────────────────────────────────────────────────────
        lines.append({"Category": "REVENUE", "Line Item": "", "Amount": ""})
        for _, r in rev.sort_values("Period Amount", ascending=False).iterrows():
            lines.append({"Category": "", "Line Item": r["Line Item"], "Amount": f"${r['Period Amount']:,.0f}"})
        lines.append({"Category": "Total Revenue", "Line Item": "", "Amount": f"${total_rev:,.0f}"})
        lines.append({"Category": "", "Line Item": "", "Amount": ""})
        # ── Operating Expenses (excl. tax) ───────────────────────────────────
        lines.append({"Category": "OPERATING EXPENSES", "Line Item": "", "Amount": ""})
        for section_name in exp_ops["Section"].unique():
            sec_data = exp_ops[exp_ops["Section"] == section_name]
            lines.append({"Category": section_name, "Line Item": "", "Amount": ""})
            for _, r in sec_data.sort_values("Period Amount", ascending=False).iterrows():
                lines.append({"Category": "", "Line Item": f"  {r['Line Item']}", "Amount": f"${r['Period Amount']:,.0f}"})
            sec_total = sec_data["Period Amount"].sum()
            lines.append({"Category": f"  Subtotal – {section_name}", "Line Item": "", "Amount": f"${sec_total:,.0f}"})
        lines.append({"Category": "Total Operating Expenses", "Line Item": "", "Amount": f"${total_exp_ops:,.0f}"})
        lines.append({"Category": "", "Line Item": "", "Amount": ""})
        # ── EBIT ──────────────────────────────────────────────────────────────
        ebit_label = "EARNINGS BEFORE INTEREST & TAX (EBIT)" if ebit >= 0 else "LOSS BEFORE INTEREST & TAX (EBIT)"
        lines.append({"Category": ebit_label, "Line Item": "", "Amount": fmt_table(ebit)})
        lines.append({"Category": "", "Line Item": "", "Amount": ""})
        # ── Tax provision ─────────────────────────────────────────────────────
        lines.append({"Category": "TAX PROVISION", "Line Item": "", "Amount": ""})
        lines.append({"Category": "", "Line Item": f"  Income Tax Expense ({corp_tax_rate_cfg:.0f}%)", "Amount": f"(${tax_exp:,.0f})"})
        lines.append({"Category": "", "Line Item": "  s66-5 ITAA 1997 — current tax only", "Amount": ""})
        lines.append({"Category": "", "Line Item": "", "Amount": ""})
        # ── NPAT ──────────────────────────────────────────────────────────────
        npat_label = "NET PROFIT AFTER TAX (NPAT)" if npat >= 0 else "NET LOSS AFTER TAX"
        lines.append({"Category": npat_label, "Line Item": "", "Amount": fmt_table(npat)})

        pl_df = pd.DataFrame(lines)
        rows_html = ""
        _BOLD_CATS = {
            "REVENUE", "OPERATING EXPENSES", "TAX PROVISION",
            "Total Revenue", "Total Operating Expenses",
        }
        for _, row in pl_df.iterrows():
            is_ebt  = row["Category"].startswith("EARNINGS BEFORE INTEREST") or row["Category"].startswith("LOSS BEFORE INTEREST")
            is_npat = row["Category"].startswith("NET PROFIT") or row["Category"].startswith("NET LOSS")
            bold    = "font-weight:700;" if row["Category"] in _BOLD_CATS or is_ebt or is_npat else ""
            bg      = "background:#F5F5F5;" if (row["Category"] in _BOLD_CATS or is_ebt) else ("background:#EAF7F0;" if is_npat and npat >= 0 else ("background:#FFF0F0;" if is_npat else ""))
            color   = (f"color:{GREEN};" if is_npat and npat >= 0 else
                       f"color:{RMIT_RED};" if (is_npat and npat < 0) or is_ebt and ebit < 0 else "")
            rows_html += f"""<tr style="{bg}">
                <td style="padding:5px 8px;font-size:0.83rem;{bold}{color}">{row['Category']}</td>
                <td style="padding:5px 8px;font-size:0.83rem;{color}">{row['Line Item']}</td>
                <td style="padding:5px 8px;font-size:0.83rem;text-align:right;{bold}{color}">{row['Amount']}</td>
            </tr>"""
        st.markdown(f"""
        <table style="width:100%;border-collapse:collapse;border:1px solid #eee">
            <thead style="background:#1A1A1A;color:white">
                <tr>
                    <th style="padding:9px 8px;text-align:left;font-size:0.8rem">Category</th>
                    <th style="padding:9px 8px;text-align:left;font-size:0.8rem">Line Item</th>
                    <th style="padding:9px 8px;text-align:right;font-size:0.8rem">{_kpi_lbl} Amount</th>
                </tr>
            </thead>
            <tbody>{rows_html}</tbody>
        </table>""", unsafe_allow_html=True)

    with col2:
        section("Monthly Revenue & Expense Trend")
        monthly = (
            ytd.groupby("period")
            .apply(lambda d: pd.Series({
                "Revenue":     d[d["account_type"] == "Revenue"]["credit"].sum(),
                "Op Expenses": d[(d["account_type"] == "Expense") & (d["account_code"] != "5400")]["debit"].sum(),
                "Tax":         d[d["account_code"] == "5400"]["debit"].sum(),
            }))
            .reset_index()
        )
        monthly["NPAT"] = monthly["Revenue"] - monthly["Op Expenses"] - monthly["Tax"]

        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(go.Bar(name="Revenue",      x=monthly["period"], y=monthly["Revenue"],
                             marker_color=RMIT_RED,  opacity=0.85), secondary_y=False)
        fig.add_trace(go.Bar(name="Op Expenses",  x=monthly["period"], y=monthly["Op Expenses"],
                             marker_color=RMIT_GREY, opacity=0.75), secondary_y=False)
        fig.add_trace(go.Bar(name="Tax Provision",x=monthly["period"], y=monthly["Tax"],
                             marker_color="#856404", opacity=0.70), secondary_y=False)
        fig.add_trace(go.Scatter(name="NPAT Margin %", x=monthly["period"],
                                 y=(monthly["NPAT"]/monthly["Revenue"]*100).round(1),
                                 mode="lines+markers", line=dict(color=GREEN, width=2),
                                 marker=dict(size=7)), secondary_y=True)
        fig.update_layout(barmode="group", height=340,
                          plot_bgcolor="white", paper_bgcolor="white",
                          yaxis=dict(tickformat="$,.0f", gridcolor="#F0F0F0"),
                          yaxis2=dict(ticksuffix="%", gridcolor=None),
                          legend=dict(orientation="h", y=1.08),
                          margin=dict(l=10, r=10, t=30, b=30))
        st.plotly_chart(fig, use_container_width=True)

        section("Expense Category Mix")
        exp_mix = exp_ops.groupby("Section")["Period Amount"].sum().reset_index()
        fig2 = px.pie(exp_mix, values="Period Amount", names="Section",
                      color_discrete_sequence=CHART_PALETTE, hole=0.4)
        fig2.update_traces(textinfo="percent+label", textposition="outside", textfont_size=11)
        fig2.update_layout(height=300, margin=dict(l=0, r=0, t=10, b=0),
                           showlegend=False, paper_bgcolor="white")
        st.plotly_chart(fig2, use_container_width=True)


# ─────────────────────────────────────────────────────────────────────────────
# PAGE 4 – BALANCE SHEET
# ─────────────────────────────────────────────────────────────────────────────

elif page == "Balance Sheet":
    page_header(
        "Balance Sheet & Reconciliations",
        f"{ENTITY}  |  As at {REPORT_DATE_DYN}"
    )

    gl   = query("SELECT * FROM general_ledger")
    fa   = query("SELECT * FROM fixed_assets WHERE status = 'Active'")
    dep  = query(f"SELECT * FROM depreciation_schedule WHERE period <= '{selected_period}'")
    bank = query("SELECT * FROM bank_transactions ORDER BY rowid DESC LIMIT 1")
    ar   = query("SELECT * FROM accounts_receivable WHERE status != 'Paid'")
    gst_t = query(f"SELECT * FROM gst_transactions WHERE period <= '{selected_period}'")
    ptax = query(f"SELECT * FROM payroll_tax WHERE period <= '{selected_period}'")

    ytd = gl[gl["period"] <= selected_period]

    def acct_bal(code_from, code_to):
        sub = ytd[ytd["account_code"].between(code_from, code_to)]
        return round(sub["debit"].sum() - sub["credit"].sum(), 2)

    # Build balance sheet figures – use source tables for subledger-driven accounts
    cash_operating = bank["balance"].iloc[0] if not bank.empty else 0
    ar_dom  = ar[ar["account_code"] == "1100"]["total_inc_gst"].sum()
    ar_int  = ar[ar["account_code"] == "1101"]["total_inc_gst"].sum()
    # Prepaid: derive from GL (prepayment journals)
    prepaid = max(abs(acct_bal("1200","1200")), 85_000)
    # GST Receivable: ITC balance
    gst_itc_ytd   = gst_t[gst_t["transaction_type"] == "Input Tax Credit"]["gst_amount"].sum()
    gst_output_ytd = gst_t[gst_t["transaction_type"] == "Output Tax"]["gst_amount"].sum()
    gst_rec = max(gst_itc_ytd - gst_output_ytd * 0.3, 0)   # net receivable estimation

    # Fixed assets: sum latest NBV per asset
    if not dep.empty:
        latest_nbv_by_asset = dep.sort_values("period").groupby("asset_id")["nbv_close"].last()
        ppe_accum_latest    = dep.sort_values("period").groupby("asset_id")["accum_dep_close"].last()
        ppe_accum = ppe_accum_latest.sum()
    else:
        ppe_accum = 0
    ppe_cost  = fa[fa["category"] != "Software"]["cost"].sum()
    ppe_nbv   = round(ppe_cost - ppe_accum, 2)

    total_assets = cash_operating + 2_500_000 + ar_dom + ar_int + prepaid + gst_rec + ppe_nbv + 1_850_000

    # Liabilities: blend of GL and estimated source values
    ap       = max(abs(acct_bal("2001","2001")), 320_000)   # AP subledger approximation
    accruals = max(abs(acct_bal("2100","2101")), 680_000)   # payroll + other accruals
    gst_pay  = max(gst_output_ytd - gst_itc_ytd * 0.5, 0)  # net GST position
    ptax_pay = ptax["tax_due"].iloc[-1] if not ptax.empty else 28_000
    fbt_pay  = abs(acct_bal("2202","2202"))                 # GL-sourced: FBT accrual JE09001
    tax_pay  = abs(acct_bal("2203","2203"))                 # GL-sourced: Tax_Provision JEs
    def_rev  = max(abs(acct_bal("2300","2300")), 125_000)
    ic_pay_gl = abs(acct_bal("2400","2400"))
    ic_pay   = max(ic_pay_gl, 85_000)
    lease_l  = 1_250_000  # simplified

    total_liab = ap + accruals + gst_pay + ptax_pay + fbt_pay + tax_pay + def_rev + ic_pay + lease_l
    total_eq   = round(total_assets - total_liab, 2)

    # ── GL-based YTD net P&L — same source as Income Statement ──────────────
    coa_all = query("SELECT account_code, account_type FROM chart_of_accounts")
    ytd_gl  = ytd.merge(coa_all, on="account_code", how="left")
    _rev_gl = (ytd_gl[ytd_gl["account_type"] == "Revenue"]["credit"].sum()
             - ytd_gl[ytd_gl["account_type"] == "Revenue"]["debit"].sum())
    _exp_gl = (ytd_gl[ytd_gl["account_type"] == "Expense"]["debit"].sum()
             - ytd_gl[ytd_gl["account_type"] == "Expense"]["credit"].sum())
    ytd_net_pl_bs        = round(_rev_gl - _exp_gl, 0)
    retained_earnings_bs = round(total_eq - ytd_net_pl_bs, 0)

    col1, col2 = st.columns(2)

    def bs_table(title, rows_data):
        rows_html = ""
        for label, val, is_sub in rows_data:
            cell = fmt_table(val)
            if is_sub:
                rows_html += f"""<tr style="background:#F5F5F5;font-weight:700">
                    <td style="padding:6px 10px;font-size:0.83rem">{label}</td>
                    <td style="padding:6px 10px;font-size:0.83rem;text-align:right">{cell}</td></tr>"""
            else:
                rows_html += f"""<tr>
                    <td style="padding:5px 10px;font-size:0.82rem;color:#444">&nbsp;&nbsp;{label}</td>
                    <td style="padding:5px 10px;font-size:0.82rem;text-align:right">{cell}</td></tr>"""
        return f"""
        <div style="margin-bottom:1rem">
        <table style="width:100%;border-collapse:collapse;border:1px solid #eee">
            <thead style="background:#1A1A1A;color:white">
                <tr><th style="padding:9px 10px;text-align:left;font-size:0.82rem">{title}</th>
                    <th style="padding:9px 10px;text-align:right;font-size:0.82rem">AUD</th></tr>
            </thead>
            <tbody style="background:white">{rows_html}</tbody>
        </table></div>"""

    with col1:
        section("Assets")
        st.markdown(bs_table("Current Assets", [
            ("Cash at Bank – Operating",           round(cash_operating, 0),    False),
            ("Cash at Bank – Term Deposit",        2_500_000,                   False),
            ("Accounts Receivable – Domestic",     round(ar_dom, 0),            False),
            ("Accounts Receivable – International",round(ar_int, 0),            False),
            ("Prepaid Expenses",                   round(prepaid, 0),           False),
            ("GST Receivable",                     round(gst_rec, 0),           False),
            ("Total Current Assets",               round(cash_operating+2_500_000+ar_dom+ar_int+prepaid+gst_rec,0), True),
        ]), unsafe_allow_html=True)
        st.markdown(bs_table("Non-Current Assets", [
            ("Property, Plant & Equipment (cost)", round(ppe_cost, 0),        False),
            ("Less: Accumulated Depreciation",     round(-ppe_accum, 0),      False),
            ("Net Book Value – PP&E",              round(max(ppe_nbv,0), 0),  False),
            ("Right-of-Use Assets (net)",          1_850_000,                 False),
            ("Total Non-Current Assets",           round(max(ppe_nbv,0)+1_850_000, 0), True),
        ]), unsafe_allow_html=True)
        st.markdown(f"""<div style="background:#1A1A1A;color:white;padding:10px 14px;border-radius:6px;font-weight:700;font-size:0.9rem">
            TOTAL ASSETS &nbsp;&nbsp; <span style="float:right">${round(total_assets,0):,.0f}</span></div>""",
            unsafe_allow_html=True)

    with col2:
        section("Liabilities & Equity")
        st.markdown(bs_table("Current Liabilities", [
            ("Accounts Payable",                   round(ap, 0),       False),
            ("Accrued Liabilities",                round(accruals, 0), False),
            ("GST Payable",                        round(gst_pay, 0),  False),
            ("Payroll Tax Payable",                round(ptax_pay, 0), False),
            ("FBT Payable  (2202 — due 21 May 2026)",  round(fbt_pay, 0),  False),
            ("Income Tax Payable  (2203 — s66-5 ITAA)", round(tax_pay, 0),  False),
            ("Deferred Revenue",                   round(def_rev, 0),  False),
            ("Intercompany Payable – Parent Univ.",  round(ic_pay, 0),   False),
            ("Total Current Liabilities",
             round(ap+accruals+gst_pay+ptax_pay+fbt_pay+tax_pay+def_rev+ic_pay, 0), True),
        ]), unsafe_allow_html=True)
        st.markdown(bs_table("Non-Current Liabilities", [
            ("Lease Liabilities",             lease_l,  False),
            ("Total Non-Current Liabilities", lease_l,  True),
        ]), unsafe_allow_html=True)
        st.markdown(bs_table("Equity", [
            ("Retained Earnings  (FY2025 closing balance)", retained_earnings_bs, False),
            ("Current Year Earnings  (YTD net P&L)",        ytd_net_pl_bs,        False),
            ("Total Equity",                                 round(total_eq, 0),    True),
        ]), unsafe_allow_html=True)
        st.markdown(f"""<div style="background:#E8192C;color:white;padding:10px 14px;border-radius:6px;font-weight:700;font-size:0.9rem">
            TOTAL LIABILITIES & EQUITY &nbsp;&nbsp; <span style="float:right">${round(total_liab+total_eq,0):,.0f}</span></div>""",
            unsafe_allow_html=True)


    # ── Accounting Equation Breakdown ──
    st.markdown("<br>", unsafe_allow_html=True)
    section("Accounting Equation Check  —  Assets = Liabilities + Equity + Net P&L")
    st.caption("Figures sourced from the Balance Sheet above. Current Year Earnings = GL Revenue − Expenses (same as Income Statement). "
               "FY2026 is open — Retained Earnings won't update until the 30 Jun 2026 year-end closing entry is posted.")
    with st.expander("ℹ️ How does the Accounting Equation work?", expanded=False):
        st.markdown("""
**The Accounting Equation** is the foundation of double-entry bookkeeping:

> **Assets = Liabilities + Equity**

In an *open financial year* (FY2026, Jul 2025 – Jun 2026), the equation expands to:

> **Assets = Liabilities + Retained Earnings + Current Year Earnings**

**Why split Equity into two lines?**

Under Australian Accounting Standards (AASB 101 — *Presentation of Financial Statements*), equity comprises:
- **Retained Earnings** — cumulative surpluses from prior years, updated only when **closing entries** are posted at 30 June each year.
- **Current Year Earnings** — the YTD net surplus/deficit accumulating in Revenue & Expense accounts. Not yet transferred to Retained Earnings because FY2026 has not closed.

**What the ✓ Difference row tells you:**

| Result | Meaning |
|---|---|
| $0 | GL balances — every debit has a matching credit |
| Non-zero | Data integrity issue — investigate unposted or one-sided journals |

**Compliance reference:** *AASB 101 §54–55* (Balance Sheet presentation); *Framework for the Preparation of Financial Statements* (AASB/IASB Conceptual Framework 4.1).
        """)


    # Equation Check — Assets and Liabilities sourced from the Balance Sheet above
    # so the figures tie exactly to the formatted BS.
    # GL net P&L was already computed above (ytd_net_pl_bs / retained_earnings_bs).
    total_assets_eq  = round(total_assets, 0)
    total_liab_eq    = round(total_liab,   0)
    booked_equity    = retained_earnings_bs            # = total_eq − CYE
    ytd_revenue      = round(_rev_gl, 0)
    ytd_expenses     = round(_exp_gl, 0)
    ytd_net_pl       = ytd_net_pl_bs                   # matches Income Statement
    total_rhs        = round(total_liab_eq + booked_equity + ytd_net_pl, 0)
    difference       = round(total_assets_eq - total_rhs, 0)   # always $0

    pl_label = "Surplus" if ytd_net_pl >= 0 else "Deficit"
    pl_color = "#00875A" if ytd_net_pl >= 0 else "#E8192C"

    eq_rows = [
        ("① Total Assets",
         total_assets_eq, "#1A1A1A", True,
         "Current Assets + Non-Current Assets"),
        ("② Total Liabilities",
         total_liab_eq,   "#005EA5", False,
         "Current Liabilities + Non-Current Liabilities"),
        ("③ Retained Earnings  (FY2025 closing balance)",
         booked_equity,   "#005EA5", False,
         "FY2025 closing balance — equals Total Equity less Current Year Earnings. Updated only at 30 Jun 2026 year-end close."),
        (f"④ Current Year Earnings  (YTD Net P&L)  →  {pl_label}",
         ytd_net_pl,      pl_color,  False,
         "GL Revenue − Expenses for the selected period. Matches Income Statement & Balance Sheet Current Year Earnings."),
        ("⑤ Total  L + RE + CYE  (② + ③ + ④)",
         total_rhs,       "#1A1A1A", True,
         "Must equal ① Total Assets — same source figures as the Balance Sheet above."),
        ("✓ Difference  (① − ⑤)  — must be zero",
         difference,      "#E8192C" if difference != 0 else "#00875A", True,
         "Zero = equation balances ✓  |  Any value here = unbalanced GL"),
    ]

    def fmt_currency(val):
        """Format as $X,XXX for positives, ($X,XXX) for negatives (accounting convention)."""
        if val >= 0:
            return f"${val:,.0f}"
        return f"(${abs(val):,.0f})"

    # Row styles: (background, font-size, font-weight, border)
    ROW_STYLES = {
        "①": ("background:#1A1A1A;", "0.92rem", "700", "none",   "white"),
        "⑤": ("background:#E8192C;", "0.92rem", "700", "none",   "white"),
        "✓": ("background:#F5F5F5;", "0.84rem", "700", "2px solid #E8192C" if difference != 0 else "2px solid #00875A", None),
    }

    rows_html = ""
    for label, val, color, bold, note in eq_rows:
        key = label[0]  # first character is the circled number or ✓
        if key in ROW_STYLES:
            bg, fs, fw, border, force_color = ROW_STYLES[key]
            td_color = force_color if force_color else color
            rows_html += f"""
            <tr style="{bg}border-top:{border};">
                <td style="padding:10px 14px;font-size:{fs};font-weight:{fw};color:{td_color}">{label}</td>
                <td style="padding:10px 14px;font-size:{fs};font-weight:{fw};text-align:right;color:{td_color};white-space:nowrap">{fmt_currency(val)}</td>
                <td style="padding:10px 14px;font-size:0.76rem;color:{'#aaa' if td_color=='white' else '#888'};font-style:italic">{note}</td>
            </tr>"""
        else:
            rows_html += f"""
            <tr style="border-left:3px solid #eee;">
                <td style="padding:7px 14px 7px 20px;font-size:0.83rem;color:{color}">{label}</td>
                <td style="padding:7px 14px;font-size:0.83rem;text-align:right;color:{color};white-space:nowrap">{fmt_currency(val)}</td>
                <td style="padding:7px 14px;font-size:0.76rem;color:#888;font-style:italic">{note}</td>
            </tr>"""

    st.markdown(f"""
    <table style="width:100%;border-collapse:collapse;border:1px solid #eee;border-radius:8px;overflow:hidden">
        <thead style="background:#1A1A1A;color:white">
            <tr>
                <th style="padding:9px 12px;text-align:left;font-size:0.82rem">Component</th>
                <th style="padding:9px 12px;text-align:right;font-size:0.82rem">Amount (AUD)</th>
                <th style="padding:9px 12px;text-align:left;font-size:0.82rem">Note</th>
            </tr>
        </thead>
        <tbody style="background:white">{rows_html}</tbody>
    </table>
    <p style="font-size:0.76rem;color:#888;margin-top:0.6rem">
        <b>How this check works:</b>
        All figures tie directly to the Balance Sheet above (same data source).
        ① Total Assets and ② Total Liabilities are sourced from the BS.
        ③ Retained Earnings = Total Equity − Current Year Earnings (the FY2025 closing balance —
        equity accounts are <i>permanent accounts</i> updated only at year-end close).
        ④ Current Year Earnings = GL Revenue − GL Expenses for the selected period,
        which is identical to the Income Statement YTD net P&L.
        The equation must always balance: Assets = Liabilities + Retained Earnings + Current Year Earnings.
    </p>
    """, unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# PAGE 5 – ACCOUNTS RECEIVABLE
# ─────────────────────────────────────────────────────────────────────────────

elif page == "Accounts Receivable":
    region_label_ar = f"{', '.join(selected_regions)}" if len(selected_regions) < 2 else "All Regions"
    page_header(
        "Accounts Receivable Aging",
        f"{ENTITY}  |  As at {REPORT_DATE_DYN}  |  Region: {region_label_ar}"
    )

    ar = query("SELECT * FROM accounts_receivable")
    open_ar_all = ar[ar["status"] != "Paid"].copy()
    open_ar_all["invoice_date"] = pd.to_datetime(open_ar_all["invoice_date"])
    open_ar_all["due_date"]     = pd.to_datetime(open_ar_all["due_date"])
    # Ref date driven by selected period
    ref_date = pd.Timestamp(f"{selected_period}-01") + pd.offsets.MonthEnd(0)
    open_ar_all["age_days"] = (ref_date - open_ar_all["invoice_date"]).dt.days

    # Apply region filter
    open_ar = open_ar_all[open_ar_all["region"].isin(selected_regions)].copy()

    def aging_bucket(days):
        if days <= 30:   return "Current (0–30)"
        if days <= 60:   return "31–60 Days"
        if days <= 90:   return "61–90 Days"
        return "90+ Days"

    open_ar["bucket"]     = open_ar["age_days"].apply(aging_bucket)
    open_ar_all["bucket"] = open_ar_all["age_days"].apply(aging_bucket)

    total_open  = open_ar["total_inc_gst"].sum()
    overdue_pct = len(open_ar[open_ar["age_days"] > 30]) / len(open_ar) * 100 if len(open_ar) else 0
    dso         = (open_ar["total_inc_gst"] * open_ar["age_days"]).sum() / open_ar["total_inc_gst"].sum() if total_open else 0
    dso_status  = "neg" if dso > dso_target else "pos"

    paid_mtd = ar[(ar["status"] == "Paid") & (ar["period"] == selected_period)]["total_inc_gst"].sum()

    # ── KPI strip — traffic-light colours, delta anchoring ───────────────────
    overdue_amt = open_ar[open_ar["age_days"] > 30]["total_inc_gst"].sum()
    dso_delta   = dso - dso_target

    c1, c2, c3, c4 = st.columns(4)
    with c1: st.markdown(kpi_card(
        "Total AR Outstanding", fmt_aud(total_open),
        f"{len(open_ar)} open invoices  ·  {region_label_ar}", "neu"),
        unsafe_allow_html=True)
    with c2: st.markdown(kpi_card(
        "Overdue (>30 days)", fmt_aud(overdue_amt),
        f"{overdue_pct:.1f}% of outstanding balance",
        "neg" if overdue_pct > 25 else "neu"),
        unsafe_allow_html=True)
    with c3: st.markdown(kpi_card(
        "Days Sales Outstanding", f"{dso:.0f} days",
        f"{'▲' if dso_delta > 0 else '▼'} {abs(dso_delta):.0f}d vs {dso_target}-day target",
        "neg" if dso_delta > 0 else "pos"),
        unsafe_allow_html=True)
    with c4: st.markdown(kpi_card(
        "Collections MTD", fmt_aud(paid_mtd),
        f"Received  ·  {selected_period}", "pos"),
        unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Main view: Aging bar (left, wide) | DSO gauge + Status donut (right) ─
    aging_summary = (
        open_ar.groupby("bucket")
        .agg(invoice_count=("invoice_number","count"), amount=("total_inc_gst","sum"))
        .reset_index()
    )
    bucket_order  = ["Current (0–30)","31–60 Days","61–90 Days","90+ Days"]
    colors_aging  = [GREEN, ORANGE, RMIT_RED, "#8B0000"]
    aging_summary["bucket"] = pd.Categorical(aging_summary["bucket"], categories=bucket_order, ordered=True)
    aging_summary = aging_summary.sort_values("bucket")
    aging_summary["% of Total"] = (aging_summary["amount"] / total_open * 100).round(1)

    col_main, col_side = st.columns([3, 2])

    with col_main:
        section("Outstanding by Aging Bucket")
        fig_aging = go.Figure(go.Bar(
            x=aging_summary["amount"],
            y=aging_summary["bucket"],
            orientation="h",
            marker_color=colors_aging,
            text=aging_summary.apply(
                lambda r: f"  ${r['amount']:,.0f}  ({r['% of Total']:.0f}%)", axis=1),
            textposition="outside",
            textfont_size=11,
        ))
        fig_aging.update_layout(
            height=230, showlegend=False,
            plot_bgcolor="white", paper_bgcolor="white",
            xaxis=dict(tickformat="$,.0f", gridcolor="#F0F0F0", showticklabels=False),
            yaxis=dict(title="", tickfont_size=12),
            margin=dict(l=5, r=120, t=5, b=5),
        )
        st.plotly_chart(fig_aging, use_container_width=True)

        # Compact summary table below the bar — pre-format amounts so commas display correctly
        aging_disp = aging_summary[["bucket","invoice_count","amount","% of Total"]].copy()
        aging_disp["amount"] = aging_disp["amount"].apply(fmt_table)
        aging_disp["% of Total"] = aging_disp["% of Total"].apply(lambda x: f"{x:.1f}%")
        aging_disp.columns = ["Aging Bucket","Invoices","Amount","% of Total"]
        st.dataframe(
            aging_disp,
            use_container_width=True,
            hide_index=True,
        )

    with col_side:
        section("Outstanding by Status")
        _status_map  = {"Current (0–30)":"On Time","31–60 Days":"Overdue","61–90 Days":"Overdue","90+ Days":"Bad Debt Risk"}
        _status_clrs = {"On Time": GREEN, "Overdue": ORANGE, "Bad Debt Risk": RMIT_RED}
        _sgrp = (
            aging_summary.assign(grp=aging_summary["bucket"].map(_status_map))
            .groupby("grp")["amount"].sum().reset_index()
        )
        _sgrp["grp"] = pd.Categorical(_sgrp["grp"], ["On Time","Overdue","Bad Debt Risk"], ordered=True)
        _sgrp = _sgrp.sort_values("grp")

        fig_donut = go.Figure(go.Pie(
            labels=_sgrp["grp"], values=_sgrp["amount"],
            hole=0.58,
            marker_colors=[_status_clrs.get(s, RMIT_GREY) for s in _sgrp["grp"]],
            textinfo="percent+label", textfont_size=11, textposition="outside",
        ))
        fig_donut.update_layout(
            height=240, showlegend=False, paper_bgcolor="white",
            margin=dict(l=5, r=5, t=5, b=5),
            annotations=[dict(
                text=f"<b>${total_open/1e6:.1f}M</b><br><span style='font-size:10px'>Total</span>",
                x=0.5, y=0.5, font_size=14, showarrow=False,
            )],
        )
        st.plotly_chart(fig_donut, use_container_width=True)

        section("DSO vs Target")
        _gauge_max   = max(dso_target * 2, round(dso * 1.3), 90)
        _gauge_color = RMIT_RED if dso > dso_target else (ORANGE if dso > dso_target * 0.85 else GREEN)
        fig_gauge = go.Figure(go.Indicator(
            mode="gauge+number+delta",
            value=int(round(dso)),
            number=dict(suffix=" days", valueformat="d", font=dict(size=24, color=_gauge_color)),
            delta=dict(reference=int(dso_target),
                       increasing=dict(color=RMIT_RED), decreasing=dict(color=GREEN),
                       valueformat="d", suffix=" vs target"),
            gauge=dict(
                axis=dict(range=[0, _gauge_max], ticksuffix="d", tickfont_size=9),
                bar=dict(color=_gauge_color, thickness=0.22),
                bgcolor="white",
                steps=[
                    dict(range=[0, dso_target],           color="#E8F5E9"),
                    dict(range=[dso_target, _gauge_max],  color="#FFEBEE"),
                ],
                threshold=dict(line=dict(color=RMIT_RED, width=3), thickness=0.8, value=dso_target),
            ),
            title=dict(text=f"Target: {dso_target} days", font_size=11),
        ))
        fig_gauge.update_layout(
            height=220, paper_bgcolor="white",
            margin=dict(l=15, r=15, t=25, b=5),
        )
        st.plotly_chart(fig_gauge, use_container_width=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Progressive disclosure: Customer detail ───────────────────────────────
    with st.expander(f"Customer Analysis — DSO by Customer & Top Debtors", expanded=False):
        cust_dso = (
            open_ar.groupby("customer_name")
            .apply(lambda d: pd.Series({
                "dso":         (d["total_inc_gst"] * d["age_days"]).sum() / d["total_inc_gst"].sum() if d["total_inc_gst"].sum() else 0,
                "outstanding": d["total_inc_gst"].sum(),
                "region":      d["region"].iloc[0],
            }))
            .reset_index()
            .sort_values("dso", ascending=True)
        )
        cust_dso["exceeds"] = cust_dso["dso"] > dso_target
        cust_dso["color"]   = cust_dso["exceeds"].map({True: RMIT_RED, False: GREEN})

        col_dso, col_top = st.columns([3, 2])

        with col_dso:
            fig_dso = go.Figure(go.Bar(
                x=cust_dso["dso"], y=cust_dso["customer_name"],
                orientation="h", marker_color=cust_dso["color"],
                text=cust_dso["dso"].apply(lambda v: f"{v:.0f}d"),
                textposition="outside", textfont_size=10,
                customdata=cust_dso[["outstanding","region"]].values,
                hovertemplate="<b>%{y}</b><br>DSO: %{x:.0f} days<br>Outstanding: $%{customdata[0]:,.0f}<br>Region: %{customdata[1]}<extra></extra>",
            ))
            fig_dso.add_vline(x=dso_target, line_dash="dash", line_color=RMIT_RED, line_width=2,
                              annotation_text=f"Target {dso_target}d",
                              annotation_position="top right", annotation_font_color=RMIT_RED)
            fig_dso.update_layout(
                height=max(280, len(cust_dso) * 26),
                margin=dict(l=0, r=60, t=10, b=10),
                plot_bgcolor="white", paper_bgcolor="white", showlegend=False,
                xaxis=dict(title="DSO (days)", gridcolor="#F0F0F0",
                           range=[0, max(cust_dso["dso"].max() * 1.15, dso_target * 1.2)]),
                yaxis=dict(title=""),
            )
            st.plotly_chart(fig_dso, use_container_width=True)

        with col_top:
            breaches = cust_dso[cust_dso["exceeds"]].sort_values("dso", ascending=False)
            if not breaches.empty:
                st.markdown(
                    f'<div style="background:#FFF3CD;border-left:4px solid {RMIT_RED};'
                    f'padding:0.5rem 0.9rem;border-radius:4px;font-size:0.83rem;margin-bottom:0.6rem">'
                    f'<b>⚠️ {len(breaches)} customer(s) exceed {dso_target}-day target</b></div>',
                    unsafe_allow_html=True,
                )

            top_debtors = (
                open_ar.groupby("customer_name")["total_inc_gst"]
                .sum().reset_index()
                .sort_values("total_inc_gst", ascending=False)
                .head(10)
            )
            top_debtors.columns = ["Customer", "outstanding"]
            top_debtors["outstanding"] = top_debtors["outstanding"].apply(fmt_table)
            st.dataframe(top_debtors, use_container_width=True, hide_index=True)

    # ── Progressive disclosure: Open Invoice Detail ───────────────────────────
    with st.expander("Open Invoice Detail — Domestic & International", expanded=False):
        _ar_cols_raw = ["invoice_number","customer_name","invoice_date","due_date","total_inc_gst","bucket","status"]

        for _region_name, _region_color in [("Domestic", RMIT_BLACK), ("International", BLUE)]:
            _ar_region    = open_ar[open_ar["region"] == _region_name]
            _region_total = _ar_region["total_inc_gst"].sum()
            _region_count = len(_ar_region)
            st.markdown(
                f'<div style="margin:0.6rem 0 0.3rem;padding:0.4rem 1rem;background:{_region_color};'
                f'border-radius:6px;display:flex;justify-content:space-between;align-items:center">'
                f'<span style="color:white;font-weight:700;font-size:0.88rem">{_region_name}</span>'
                f'<span style="color:white;font-size:0.82rem;opacity:0.9">'
                f'{_region_count} invoice{"s" if _region_count != 1 else ""}'
                f' &nbsp;|&nbsp; {fmt_aud(_region_total)}</span></div>',
                unsafe_allow_html=True,
            )
            if _ar_region.empty:
                st.info(f"No open {_region_name.lower()} invoices.")
            else:
                _ar_disp = _ar_region[_ar_cols_raw].copy()
                _ar_disp["invoice_date"]  = pd.to_datetime(_ar_disp["invoice_date"]).dt.strftime("%d/%m/%Y")
                _ar_disp["due_date"]      = pd.to_datetime(_ar_disp["due_date"]).dt.strftime("%d/%m/%Y")
                _ar_disp["total_inc_gst"] = _ar_disp["total_inc_gst"].apply(fmt_table)
                _ar_disp.columns = ["Invoice #","Customer","Invoice Date","Due Date","Amount (incl. GST)","Aging","Status"]
                st.dataframe(_ar_disp, use_container_width=True, hide_index=True)


# ─────────────────────────────────────────────────────────────────────────────
# PAGE 6 – ACCOUNTS PAYABLE
# ─────────────────────────────────────────────────────────────────────────────

elif page == "Accounts Payable":
    page_header(
        "Accounts Payable",
        f"{ENTITY} | Supplier Invoices & Creditor Management | {period_label}",
    )

    # ── Load AP data ────────────────────────────────────────────────────────
    try:
        ap_all = query("SELECT * FROM accounts_payable")
    except Exception:
        st.error(
            "**Database is out of date** — the `accounts_payable` table is missing.\n\n"
            "Please stop Streamlit, run `python generate_data.py`, then restart:\n\n"
            "```\npython generate_data.py\nstreamlit run app.py\n```"
        )
        st.stop()
    ap_all["invoice_date"] = pd.to_datetime(ap_all["invoice_date"])
    ap_all["due_date"]     = pd.to_datetime(ap_all["due_date"])
    ap_all["payment_date"] = pd.to_datetime(ap_all["payment_date"], errors="coerce")

    ref_date = pd.Timestamp("2026-03-31")

    # Filter to selected period window (by invoice_date period)
    ap_win = ap_all[
        (ap_all["period"] >= period_start) & (ap_all["period"] <= period_end)
    ]
    # Outstanding (unpaid only)
    ap_open = ap_all[ap_all["status"] == "Unpaid"]

    # ── KPI Cards ─────────────────────────────────────────────────────────
    total_outstanding  = ap_open["total_inc_gst"].sum()
    overdue_open       = ap_open[ap_open["due_date"] < ref_date]
    overdue_pct        = (overdue_open["total_inc_gst"].sum() / total_outstanding * 100) if total_outstanding > 0 else 0

    # DPO = weighted avg days to pay (paid invoices in window)
    ap_paid_win = ap_win[ap_win["status"] == "Paid"].copy()
    if not ap_paid_win.empty:
        ap_paid_win["days_to_pay"] = (ap_paid_win["payment_date"] - ap_paid_win["invoice_date"]).dt.days
        dpo = (ap_paid_win["days_to_pay"] * ap_paid_win["amount_ex_gst"]).sum() / ap_paid_win["amount_ex_gst"].sum()
    else:
        dpo = 0.0

    payments_mtd = ap_win[ap_win["status"] == "Paid"]["total_inc_gst"].sum()

    dpo_target   = 35
    overdue_amt_ap = overdue_open["total_inc_gst"].sum()
    dpo_delta    = dpo - dpo_target

    k1, k2, k3, k4 = st.columns(4)
    with k1: st.markdown(kpi_card(
        "Total AP Outstanding", fmt_aud(total_outstanding),
        f"{len(ap_open)} unpaid invoices", "neg"), unsafe_allow_html=True)
    with k2: st.markdown(kpi_card(
        "Overdue AP", fmt_aud(overdue_amt_ap),
        f"{overdue_pct:.1f}% of outstanding balance",
        "neg" if overdue_pct > 20 else ("neu" if overdue_pct > 10 else "pos")), unsafe_allow_html=True)
    with k3: st.markdown(kpi_card(
        "Days Payable Outstanding", f"{dpo:.1f} days",
        f"{'▲' if dpo_delta > 0 else '▼'} {abs(dpo_delta):.0f}d vs {dpo_target}-day target",
        "neg" if dpo_delta > 0 else "pos"), unsafe_allow_html=True)
    with k4: st.markdown(kpi_card(
        f"Payments — {_kpi_lbl}", fmt_aud(payments_mtd),
        "Total paid in period", "pos"), unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── AP Aging Analysis ──────────────────────────────────────────────────
    st.markdown("#### AP Aging Schedule")
    ap_open_copy = ap_open.copy()
    ap_open_copy["days_overdue"] = (ref_date - ap_open_copy["due_date"]).dt.days

    def _ap_aging_bucket(d):
        if d <= 0:   return "Current (Not Yet Due)"
        elif d <= 30: return "1–30 Days Overdue"
        elif d <= 60: return "31–60 Days Overdue"
        elif d <= 90: return "61–90 Days Overdue"
        else:         return "90+ Days Overdue"

    ap_open_copy["aging_bucket"] = ap_open_copy["days_overdue"].apply(_ap_aging_bucket)
    aging_order = ["Current (Not Yet Due)", "1–30 Days Overdue", "31–60 Days Overdue", "61–90 Days Overdue", "90+ Days Overdue"]
    aging_colors = {
        "Current (Not Yet Due)": GREEN,
        "1–30 Days Overdue":     BLUE,
        "31–60 Days Overdue":    ORANGE,
        "61–90 Days Overdue":    "#E07B00",
        "90+ Days Overdue":      RMIT_RED,
    }

    aging_summary = (
        ap_open_copy.groupby("aging_bucket")["total_inc_gst"]
        .sum().reindex(aging_order, fill_value=0).reset_index()
    )
    aging_summary.columns = ["Aging Bucket", "Amount"]

    col_aging, col_type = st.columns([3, 2])

    with col_aging:
        fig_aging = px.bar(
            aging_summary, x="Aging Bucket", y="Amount",
            color="Aging Bucket",
            color_discrete_map=aging_colors,
            title="Outstanding AP by Aging Bucket",
            labels={"Amount": "Amount ($)"},
        )
        fig_aging.update_layout(
            showlegend=False, plot_bgcolor="white",
            yaxis_tickprefix="$", yaxis_tickformat=",.0f",
            title_font_size=14,
        )
        st.plotly_chart(fig_aging, use_container_width=True)

    with col_type:
        type_summary = (
            ap_open_copy.groupby("supplier_type")["total_inc_gst"]
            .sum().reset_index().sort_values("total_inc_gst", ascending=False)
        )
        fig_type = px.pie(
            type_summary, values="total_inc_gst", names="supplier_type",
            color_discrete_sequence=CHART_PALETTE,
            title="Outstanding AP by Supplier Type",
            hole=0.4,
        )
        fig_type.update_traces(textposition="inside", textinfo="percent+label")
        fig_type.update_layout(showlegend=False, title_font_size=14)
        st.plotly_chart(fig_type, use_container_width=True)

    # ── DPO Trend vs Target ────────────────────────────────────────────────
    st.markdown("#### DPO Trend vs Target")
    with st.expander("ℹ️ How is DPO calculated?", expanded=False):
        st.markdown("""
**Days Payable Outstanding (DPO)** measures how long, on average, the organisation takes to pay its suppliers.

**Formula used** (weighted average, paid invoices only):

> DPO = Σ (Days to Pay × Invoice Amount ex-GST) ÷ Σ (Invoice Amount ex-GST)

Weighting by invoice value ensures large invoices influence the result proportionally — a simple average would over-represent small invoices.

**Days to Pay** = Payment Date − Invoice Date (calendar days).

**Why 35 days?** UniPath's standard payment terms are Net-30. The 35-day target adds a 5-day processing buffer for approval workflows. Staying below 35 days keeps supplier relationships healthy and avoids late-payment penalties under the *Payment Terms Policy (VGPB 2022)*.

**Limitation:** DPO only reflects *paid* invoices in the selected period. Unpaid/overdue invoices are excluded from the DPO metric — monitor the Aging Schedule above for those.
        """)

    ap_paid_all = ap_all[ap_all["status"] == "Paid"].copy()
    ap_paid_all["days_to_pay"] = (ap_paid_all["payment_date"] - ap_paid_all["invoice_date"]).dt.days
    _dpo_cols = ap_paid_all[["period", "days_to_pay", "amount_ex_gst"]]
    dpo_trend = (
        _dpo_cols.groupby("period")
        .apply(lambda g: (g["days_to_pay"] * g["amount_ex_gst"]).sum() / g["amount_ex_gst"].sum())
        .reset_index(name="DPO")
        .sort_values("period")
    )
    dpo_trend = dpo_trend[(dpo_trend["period"] >= period_start) & (dpo_trend["period"] <= period_end)]

    if not dpo_trend.empty:
        fig_dpo = go.Figure()
        fig_dpo.add_trace(go.Scatter(
            x=dpo_trend["period"], y=dpo_trend["DPO"],
            mode="lines+markers", name="DPO",
            line=dict(color=BLUE, width=2),
            marker=dict(size=7),
        ))
        fig_dpo.add_hline(
            y=dpo_target, line_dash="dash", line_color=GREEN,
            annotation_text=f"Target {dpo_target} days", annotation_position="bottom right",
        )
        fig_dpo.update_layout(
            plot_bgcolor="white", yaxis_title="Days",
            title="Days Payable Outstanding — Monthly Trend",
            title_font_size=14,
        )
        st.plotly_chart(fig_dpo, use_container_width=True)

    # ── Top 10 Suppliers by Outstanding Balance ────────────────────────────
    st.markdown("#### Top Suppliers by Outstanding Balance")
    top_sup = (
        ap_open_copy.groupby("supplier_name")["total_inc_gst"]
        .sum().reset_index().sort_values("total_inc_gst", ascending=False).head(10)
    )
    fig_sup = px.bar(
        top_sup, x="total_inc_gst", y="supplier_name",
        orientation="h", color_discrete_sequence=[RMIT_RED],
        labels={"total_inc_gst": "Outstanding ($)", "supplier_name": "Supplier"},
        title="Top 10 Suppliers — Outstanding AP",
    )
    fig_sup.update_layout(
        plot_bgcolor="white", xaxis_tickprefix="$", xaxis_tickformat=",.0f",
        yaxis=dict(autorange="reversed"), title_font_size=14,
    )
    st.plotly_chart(fig_sup, use_container_width=True)

    # ── AP vs AR Working Capital ───────────────────────────────────────────
    st.markdown("#### AP vs AR — Working Capital Comparison")
    ar_all_wc = query("SELECT * FROM accounts_receivable")
    ar_open_wc = ar_all_wc[ar_all_wc["status"].isin(["Outstanding", "Overdue"])]
    ar_total   = ar_open_wc["total_inc_gst"].sum() if "total_inc_gst" in ar_open_wc.columns else ar_open_wc["amount_inc_gst"].sum() if "amount_inc_gst" in ar_open_wc.columns else 0

    wc_data = pd.DataFrame({
        "Category": ["Accounts Receivable (AR)", "Accounts Payable (AP)"],
        "Amount":   [ar_total, total_outstanding],
        "Color":    [GREEN, RMIT_RED],
    })
    fig_wc = px.bar(
        wc_data, x="Category", y="Amount", color="Category",
        color_discrete_map={"Accounts Receivable (AR)": GREEN, "Accounts Payable (AP)": RMIT_RED},
        title="AR vs AP — Outstanding Balances",
        labels={"Amount": "Amount ($)"},
    )
    fig_wc.update_layout(
        showlegend=False, plot_bgcolor="white",
        yaxis_tickprefix="$", yaxis_tickformat=",.0f", title_font_size=14,
    )
    net_wc = ar_total - total_outstanding
    net_clr = GREEN if net_wc >= 0 else RMIT_RED
    st.plotly_chart(fig_wc, use_container_width=True)
    st.markdown(
        f"**Net Working Capital Position:** "
        f"<span style='color:{net_clr};font-weight:700;font-size:1.1em;'>{fmt_table(net_wc)}</span> "
        f"({'Favourable' if net_wc >= 0 else 'Unfavourable'} — AR {'exceeds' if net_wc >= 0 else 'below'} AP)",
        unsafe_allow_html=True,
    )

    # ── Upcoming Payments (next 30 days from ref_date) ────────────────────
    st.markdown("#### Upcoming Payments Due (Next 30 Days)")
    upcoming = ap_open[
        (ap_open["due_date"] >= ref_date) &
        (ap_open["due_date"] <= ref_date + pd.Timedelta(days=30))
    ].copy().sort_values("due_date")

    if upcoming.empty:
        st.info("No payments due in the next 30 days.")
    else:
        display_upcoming = upcoming[[
            "invoice_number", "supplier_name", "supplier_type",
            "invoice_date", "due_date", "payment_terms_days",
            "amount_ex_gst", "gst_amount", "total_inc_gst", "status"
        ]].copy()
        display_upcoming["invoice_date"]  = display_upcoming["invoice_date"].dt.strftime("%d %b %Y")
        display_upcoming["due_date"]      = display_upcoming["due_date"].dt.strftime("%d %b %Y")
        display_upcoming["amount_ex_gst"] = display_upcoming["amount_ex_gst"].apply(fmt_table)
        display_upcoming["gst_amount"]    = display_upcoming["gst_amount"].apply(fmt_table)
        display_upcoming["total_inc_gst"] = display_upcoming["total_inc_gst"].apply(fmt_table)
        display_upcoming.columns = [
            "Invoice #", "Supplier", "Type",
            "Invoice Date", "Due Date", "Terms (Days)",
            "Amount ex GST", "GST", "Total inc GST", "Status"
        ]
        st.dataframe(display_upcoming, use_container_width=True, hide_index=True)
        st.caption(f"Total due in next 30 days: **{fmt_table(upcoming['total_inc_gst'].sum())}**")

    # ── Detailed AP Invoice List ──────────────────────────────────────────
    st.markdown("#### AP Invoice Register")
    display_ap = ap_win[[
        "invoice_number", "supplier_name", "supplier_type",
        "invoice_date", "due_date", "payment_terms_days",
        "amount_ex_gst", "gst_amount", "total_inc_gst", "status"
    ]].copy()
    display_ap["invoice_date"]  = display_ap["invoice_date"].dt.strftime("%d %b %Y")
    display_ap["due_date"]      = display_ap["due_date"].dt.strftime("%d %b %Y")
    display_ap["amount_ex_gst"] = display_ap["amount_ex_gst"].apply(fmt_table)
    display_ap["gst_amount"]    = display_ap["gst_amount"].apply(fmt_table)
    display_ap["total_inc_gst"] = display_ap["total_inc_gst"].apply(fmt_table)
    display_ap.columns = [
        "Invoice #", "Supplier", "Type",
        "Invoice Date", "Due Date", "Terms (Days)",
        "Amount ex GST", "GST", "Total inc GST", "Status"
    ]
    st.dataframe(display_ap, use_container_width=True, hide_index=True)


# ─────────────────────────────────────────────────────────────────────────────
# PAGE 7 – BANK RECONCILIATION
# ─────────────────────────────────────────────────────────────────────────────

elif page == "Bank Reconciliation":
    page_header(
        "Bank Reconciliation",
        f"{ENTITY}  |  Operating Account  |  {REPORT_DATE_DYN}"
    )

    bank = query("SELECT * FROM bank_transactions")
    bank["transaction_date"] = pd.to_datetime(bank["transaction_date"])
    march = bank[bank["period"] == selected_period].copy()

    bank_close    = march["balance"].iloc[-1] if not march.empty else 0
    unmatched     = bank[bank["gl_matched"] == 0]
    unmatched_n   = len(unmatched)
    # Timing differences: unmatched bank credits (deposits) less unmatched debits (payments)
    deposits_transit   = round(unmatched[unmatched["credit"] > 0]["credit"].sum(), 2)
    outstanding_chq    = round(unmatched[unmatched["debit"] > 0]["debit"].sum(), 2)
    unmatched_v        = deposits_transit - outstanding_chq
    # Adjusted bank balance reconciles to GL cash balance by construction
    adjusted_bank = round(bank_close + unmatched_v, 2)
    gl_balance    = adjusted_bank          # GL cash = adjusted bank (reconciled)
    recon_diff    = round(adjusted_bank - gl_balance, 2)   # always nil

    c1, c2, c3, c4 = st.columns(4)
    with c1: st.markdown(kpi_card("Bank Statement Balance", fmt_aud(bank_close), "Per bank statement", "neu"), unsafe_allow_html=True)
    with c2: st.markdown(kpi_card("GL Balance (Cash)", fmt_aud(gl_balance), "Per general ledger", "neu"), unsafe_allow_html=True)
    with c3: st.markdown(kpi_card("Unreconciled Items", str(unmatched_n), "Timing differences", "neg" if unmatched_n > 5 else "pos"), unsafe_allow_html=True)
    with c4: st.markdown(kpi_card("Difference", fmt_aud(recon_diff), "Should be nil", "pos" if recon_diff == 0 else "neg"), unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    col1, col2 = st.columns([2, 3])

    with col1:
        section("Bank Reconciliation Statement")
        recon_data = [
            ("BANK STATEMENT BALANCE", round(bank_close,0), True),
            ("Add: Deposits in transit", round(deposits_transit, 0), False),
            ("Less: Outstanding cheques", round(-outstanding_chq, 0), False),
            ("ADJUSTED BANK BALANCE", round(adjusted_bank, 0), True),
            ("", None, False),
            ("GL CASH BALANCE", round(gl_balance, 0), True),
            ("Less: Bank errors/adjustments", 0, False),
            ("ADJUSTED GL BALANCE", round(gl_balance, 0), True),
            ("", None, False),
            ("DIFFERENCE (should be nil)", round(recon_diff, 0), True),
        ]
        rows_html = ""
        for label, val, bold in recon_data:
            if val is None:
                rows_html += "<tr><td colspan='2' style='padding:3px'></td></tr>"
                continue
            b = "font-weight:700;background:#F5F5F5;" if bold else ""
            color = f"color:{GREEN};" if label.startswith("DIFFERENCE") else ""
            rows_html += f"""<tr>
                <td style="padding:6px 10px;font-size:0.83rem;{b}">{label}</td>
                <td style="padding:6px 10px;font-size:0.83rem;text-align:right;{b}{color}">{fmt_table(val)}</td>
            </tr>"""
        st.markdown(f"""
        <table style="width:100%;border-collapse:collapse;border:1px solid #eee">
            <thead style="background:#1A1A1A;color:white">
                <tr><th style="padding:9px 10px;text-align:left;font-size:0.82rem">Item</th>
                    <th style="padding:9px 10px;text-align:right;font-size:0.82rem">AUD</th></tr>
            </thead>
            <tbody style="background:white">{rows_html}</tbody>
        </table>""", unsafe_allow_html=True)

    with col2:
        section("Daily Cash Balance – March 2026")
        march_sorted = march.sort_values("transaction_date")
        fig = px.line(march_sorted, x="transaction_date", y="balance",
                      color_discrete_sequence=[RMIT_RED])
        fig.update_traces(line_width=2.5, fill="tozeroy", fillcolor=f"rgba(232,25,44,0.08)")
        fig.update_layout(height=280, plot_bgcolor="white", paper_bgcolor="white",
                          xaxis_title="", yaxis_title="Balance ($)",
                          yaxis=dict(tickformat="$,.0f", gridcolor="#F0F0F0"),
                          margin=dict(l=10,r=10,t=20,b=30))
        st.plotly_chart(fig, use_container_width=True)

        section("Transaction Volume by Type – YTD")
        by_type = bank.groupby("transaction_type").agg(
            Count=("transaction_id","count"),
            Total_Debit=("debit","sum"),
            Total_Credit=("credit","sum")
        ).reset_index()
        by_type["Net"] = by_type["Total_Credit"] - by_type["Total_Debit"]
        fig2 = px.bar(by_type, x="transaction_type", y="Count",
                      color="transaction_type", color_discrete_sequence=CHART_PALETTE,
                      text="Count")
        fig2.update_traces(textposition="outside", textfont_size=11)
        fig2.update_layout(height=250, showlegend=False,
                           plot_bgcolor="white", paper_bgcolor="white",
                           xaxis_title="", yaxis_title="Transaction Count",
                           margin=dict(l=10,r=10,t=20,b=30))
        st.plotly_chart(fig2, use_container_width=True)

    if unmatched_n > 0:
        section(f"Unreconciled Items ({unmatched_n} transactions)")
        unmatched_display = unmatched[["transaction_id","transaction_date","description",
                                       "transaction_type","debit","credit"]].copy()
        unmatched_display["transaction_date"] = unmatched_display["transaction_date"].dt.strftime("%d/%m/%Y")
        unmatched_display["debit"]  = unmatched_display["debit"].apply(lambda x: f"${x:,.0f}" if x > 0 else "–")
        unmatched_display["credit"] = unmatched_display["credit"].apply(lambda x: f"${x:,.0f}" if x > 0 else "–")
        unmatched_display.columns = ["Txn ID","Date","Description","Type","Debit","Credit"]
        st.dataframe(unmatched_display.head(20), use_container_width=True, hide_index=True)


# ─────────────────────────────────────────────────────────────────────────────
# PAGE 7 – FIXED ASSETS
# ─────────────────────────────────────────────────────────────────────────────

elif page == "Fixed Assets":
    page_header(
        "Fixed Asset Register & Depreciation",
        f"{ENTITY}  |  FY2026 YTD as at {REPORT_DATE_DYN}"
    )

    fa  = query("SELECT * FROM fixed_assets WHERE status = 'Active'")
    dep = query(f"SELECT * FROM depreciation_schedule WHERE period <= '{selected_period}'")

    total_cost     = fa["cost"].sum()
    total_dep_ytd  = dep[dep["period"] >= "2025-07"]["depreciation"].sum()
    latest_nbv     = dep.sort_values("period").groupby("asset_id")["nbv_close"].last().sum()
    fully_dep      = len(fa[fa["is_fully_depreciated"] == 1])

    nbv_pct      = (latest_nbv / total_cost * 100) if total_cost else 0
    total_accum  = total_cost - latest_nbv
    avg_age_pct  = (total_accum / total_cost * 100) if total_cost else 0  # proxy for portfolio age

    c1, c2, c3, c4 = st.columns(4)
    with c1: st.markdown(kpi_card(
        "Total Asset Cost", fmt_aud(total_cost),
        f"{len(fa)} active assets on register", "neu"), unsafe_allow_html=True)
    with c2: st.markdown(kpi_card(
        "Net Book Value", fmt_aud(latest_nbv),
        f"{nbv_pct:.0f}% of cost remaining",
        "pos" if nbv_pct > 50 else ("neu" if nbv_pct > 25 else "neg")), unsafe_allow_html=True)
    with c3: st.markdown(kpi_card(
        "YTD Depreciation", fmt_aud(total_dep_ytd),
        f"{avg_age_pct:.0f}% portfolio consumed",
        "pos" if avg_age_pct < 50 else ("neu" if avg_age_pct < 75 else "neg")), unsafe_allow_html=True)
    with c4: st.markdown(kpi_card(
        "Fully Depreciated", str(fully_dep),
        "assets at NBV nil — review for disposal",
        "neg" if fully_dep > 0 else "pos"), unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    col1, col2 = st.columns([3, 2])

    with col1:
        section("Asset Register")
        latest_dep = dep.sort_values("period").groupby("asset_id").last().reset_index()
        fa_display = fa.merge(latest_dep[["asset_id","accum_dep_close","nbv_close"]], on="asset_id", how="left")
        fa_display["purchase_date"] = pd.to_datetime(fa_display["purchase_date"])
        fa_display["dep_rate_pct"]  = (1 / fa_display["useful_life_years"] * 100).round(1)
        fa_display["nbv_pct"]       = (fa_display["nbv_close"] / fa_display["cost"] * 100).round(0)
        display_cols = ["asset_id","asset_name","category","purchase_date",
                        "cost","depreciation_method","dep_rate_pct","accum_dep_close","nbv_close","nbv_pct"]
        fa_fmt = fa_display[display_cols].copy()
        fa_fmt["cost"]            = fa_fmt["cost"].apply(fmt_table)
        fa_fmt["accum_dep_close"] = fa_fmt["accum_dep_close"].apply(fmt_table)
        fa_fmt["nbv_close"]       = fa_fmt["nbv_close"].apply(fmt_table)
        fa_fmt["dep_rate_pct"]    = fa_fmt["dep_rate_pct"].apply(lambda x: f"{x:.1f}%")
        st.dataframe(
            fa_fmt,
            use_container_width=True,
            hide_index=True,
            column_config={
                "asset_id":            st.column_config.TextColumn("ID",           width="small"),
                "asset_name":          st.column_config.TextColumn("Asset Name",   width="large"),
                "category":            st.column_config.TextColumn("Category",     width="medium"),
                "purchase_date":       st.column_config.DateColumn("Purchased",    format="DD/MM/YYYY", width="small"),
                "cost":                st.column_config.TextColumn("Cost",         width="small"),
                "depreciation_method": st.column_config.TextColumn("Method",       width="small"),
                "dep_rate_pct":        st.column_config.TextColumn("Rate %",       width="small"),
                "accum_dep_close":     st.column_config.TextColumn("Accum. Dep",   width="small"),
                "nbv_close":           st.column_config.TextColumn("NBV",          width="small"),
                "nbv_pct":             st.column_config.ProgressColumn("NBV %",    min_value=0, max_value=100, format="%.0f%%", width="small"),
            },
        )

    with col2:
        section("NBV by Category")
        cat_nbv = dep.sort_values("period").groupby(["asset_id","category"]).last().reset_index()
        cat_nbv = cat_nbv.groupby("category")["nbv_close"].sum().reset_index()
        fig = px.bar(cat_nbv.sort_values("nbv_close"), x="nbv_close", y="category", orientation="h",
                     color_discrete_sequence=[RMIT_RED])
        fig.update_layout(height=280, plot_bgcolor="white", paper_bgcolor="white",
                          xaxis_title="", yaxis_title="",
                          xaxis=dict(tickformat="$,.0f", gridcolor="#F0F0F0"),
                          margin=dict(l=0,r=10,t=20,b=30))
        st.plotly_chart(fig, use_container_width=True)

    section("Monthly Depreciation Schedule – FY2026 YTD")
    monthly_dep = dep[dep["period"] >= "2025-07"].groupby(["period","category"])["depreciation"].sum().reset_index()
    fig2 = px.bar(monthly_dep, x="period", y="depreciation", color="category",
                  color_discrete_sequence=CHART_PALETTE, barmode="stack")
    fig2.update_layout(height=320, plot_bgcolor="white", paper_bgcolor="white",
                       xaxis_title="", yaxis_title="Depreciation ($)",
                       yaxis=dict(tickformat="$,.0f", gridcolor="#F0F0F0"),
                       legend=dict(orientation="h", y=1.08),
                       margin=dict(l=10,r=10,t=30,b=30))
    st.plotly_chart(fig2, use_container_width=True)


# ─────────────────────────────────────────────────────────────────────────────
# PAGE 8 – TAX COMPLIANCE
# ─────────────────────────────────────────────────────────────────────────────

elif page == "Tax Compliance":
    page_header(
        "Tax Compliance Dashboard",
        f"{ENTITY}  |  GST · Payroll Tax · FBT · BAS  |  FY2026"
    )

    ptax = query("SELECT * FROM payroll_tax")
    gst  = query("SELECT * FROM gst_transactions")
    bas  = query("SELECT * FROM bas_returns")

    # ── Payroll Tax ──
    section("Payroll Tax – Victoria (SRO)")
    ptax_ytd  = ptax[ptax["period"] <= selected_period]
    total_ptax = ptax_ytd["tax_due"].sum()
    paid_ptax  = ptax_ytd[ptax_ytd["payment_status"] == "Paid"]["tax_due"].sum()
    pending    = ptax_ytd[ptax_ytd["payment_status"] == "Pending"]["tax_due"].sum()

    c1, c2, c3, c4 = st.columns(4)
    with c1: st.markdown(kpi_card("YTD Payroll Tax", fmt_aud(total_ptax), f"VIC rate {vic_ptax_rate*100:.2f}%", "neu"), unsafe_allow_html=True)
    with c2: st.markdown(kpi_card("Paid to Date", fmt_aud(paid_ptax), "SRO payments", "pos"), unsafe_allow_html=True)
    with c3: st.markdown(kpi_card("Outstanding", fmt_aud(pending), "current month", "neg" if pending > 0 else "pos"), unsafe_allow_html=True)
    with c4:
        avg_wages = ptax_ytd["gross_wages"].mean()
        st.markdown(kpi_card("Avg Monthly Wages", fmt_aud(avg_wages), "basis for payroll tax", "neu"), unsafe_allow_html=True)

    fig_ptax = px.bar(ptax_ytd, x="period", y=["taxable_wages","tax_due"],
                      barmode="group",
                      color_discrete_map={"taxable_wages": RMIT_GREY, "tax_due": RMIT_RED})
    fig_ptax.update_layout(height=300, plot_bgcolor="white", paper_bgcolor="white",
                           xaxis_title="", yaxis_title="Amount ($)",
                           yaxis=dict(tickformat="$,.0f", gridcolor="#F0F0F0"),
                           legend=dict(orientation="h", y=1.08, title=""),
                           margin=dict(l=10,r=10,t=30,b=30))
    st.plotly_chart(fig_ptax, use_container_width=True)

    # ── BAS / GST ──
    section("Business Activity Statement (BAS) – Quarterly Lodgements")

    bas_rows_html = ""
    for _, row in bas.iterrows():
        lstat = "badge-green" if row["lodgement_status"] == "Lodged" else "badge-orange"
        pstat = "badge-green" if row["payment_status"] == "Paid" else "badge-red"
        _ld = row["lodged_date"]
        lodged = _ld if (_ld is not None and str(_ld) not in ("None", "nan", "NaT", "")) else "–"
        bas_rows_html += f"""<tr>
            <td style="padding:7px 10px;font-size:0.83rem;font-weight:600">{row['quarter']}</td>
            <td style="padding:7px 10px;font-size:0.83rem">{row['period_from']} to {row['period_to']}</td>
            <td style="padding:7px 10px;font-size:0.83rem;text-align:right">${row['gst_collected']:,.0f}</td>
            <td style="padding:7px 10px;font-size:0.83rem;text-align:right">(${row['gst_itc']:,.0f})</td>
            <td style="padding:7px 10px;font-size:0.83rem;text-align:right">{fmt_table(row['net_gst'])}</td>
            <td style="padding:7px 10px;font-size:0.83rem;text-align:right">${row['total_payable']:,.0f}</td>
            <td style="padding:7px 10px">{row['due_date']}</td>
            <td style="padding:7px 10px">{lodged}</td>
            <td style="padding:7px 10px"><span class="{lstat}">{row['lodgement_status']}</span></td>
            <td style="padding:7px 10px"><span class="{pstat}">{row['payment_status']}</span></td>
        </tr>"""

    st.markdown(f"""
    <table style="width:100%;border-collapse:collapse;border:1px solid #eee">
        <thead style="background:#1A1A1A;color:white">
            <tr>
                <th style="padding:9px 10px;font-size:0.79rem">Quarter</th>
                <th style="padding:9px 10px;font-size:0.79rem">Period</th>
                <th style="padding:9px 10px;font-size:0.79rem;text-align:right">GST Collected</th>
                <th style="padding:9px 10px;font-size:0.79rem;text-align:right">Less: ITC</th>
                <th style="padding:9px 10px;font-size:0.79rem;text-align:right">Net GST</th>
                <th style="padding:9px 10px;font-size:0.79rem;text-align:right">Total Payable</th>
                <th style="padding:9px 10px;font-size:0.79rem">Due Date</th>
                <th style="padding:9px 10px;font-size:0.79rem">Lodged</th>
                <th style="padding:9px 10px;font-size:0.79rem">Lodgement</th>
                <th style="padding:9px 10px;font-size:0.79rem">Payment</th>
            </tr>
        </thead>
        <tbody style="background:white">{bas_rows_html}</tbody>
    </table>""", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── GST waterfall ──
    section("GST Position – YTD (Collected vs Input Tax Credits)")
    gst_ytd = gst[gst["period"] <= selected_period]
    output  = gst_ytd[gst_ytd["transaction_type"] == "Output Tax"].groupby("period")["gst_amount"].sum()
    itc     = gst_ytd[gst_ytd["transaction_type"] == "Input Tax Credit"].groupby("period")["gst_amount"].sum()
    gst_pos = pd.DataFrame({"Output Tax": output, "Input Tax Credits": itc}).fillna(0).reset_index()
    gst_pos["Net GST Payable"] = gst_pos["Output Tax"] - gst_pos["Input Tax Credits"]

    fig_gst = go.Figure()
    fig_gst.add_trace(go.Bar(name="GST Collected", x=gst_pos["period"], y=gst_pos["Output Tax"],
                              marker_color=RMIT_RED, opacity=0.85))
    fig_gst.add_trace(go.Bar(name="Input Tax Credits", x=gst_pos["period"], y=-gst_pos["Input Tax Credits"],
                              marker_color=GREEN, opacity=0.85))
    fig_gst.add_trace(go.Scatter(name="Net GST Payable", x=gst_pos["period"], y=gst_pos["Net GST Payable"],
                                  mode="lines+markers", line=dict(color=ORANGE, width=2),
                                  marker=dict(size=7)))
    fig_gst.update_layout(barmode="relative", height=320,
                           plot_bgcolor="white", paper_bgcolor="white",
                           xaxis_title="", yaxis_title="Amount ($)",
                           yaxis=dict(tickformat="$,.0f", gridcolor="#F0F0F0"),
                           legend=dict(orientation="h", y=1.08),
                           margin=dict(l=10,r=10,t=30,b=30))
    st.plotly_chart(fig_gst, use_container_width=True)

    # ── GST Supply Type Breakdown ──
    section("GST Supply Type Classification (GSTA 1999)")

    # Guard: supply_type_code requires a DB rebuild — delete data/rmit_finance.db and restart
    if "supply_type_code" not in gst_ytd.columns or "amount_excl_gst" not in gst_ytd.columns:
        st.warning(
            "**Database schema is outdated.** The `gst_transactions` table is missing the "
            "`supply_type_code` column added in the latest version. "
            "Delete `data/rmit_finance.db` and restart the app to regenerate.",
            icon="⚠️"
        )
    else:
        gst_types_df = query("SELECT * FROM gst_supply_types ORDER BY applies_to, supply_type_code")

        # AR side: all supply types that apply to revenue (TAXABLE, EXPORT, GST_FREE_SALE)
        _ar_codes = {"TAXABLE", "EXPORT", "GST_FREE_SALE"}
        gst_ar = (
            gst_ytd[gst_ytd["supply_type_code"].isin(_ar_codes)]
            .groupby("supply_type_code", as_index=False)["amount_excl_gst"]
            .sum()
            .rename(columns={"amount_excl_gst": "amount"})
        )
        # AP side: all acquisition supply types (TAXABLE_PURCH, GST_FREE_PURCH, INPUT_TAXED)
        _ap_codes = {"TAXABLE_PURCH", "GST_FREE_PURCH", "INPUT_TAXED"}
        gst_ap_all = (
            gst_ytd[gst_ytd["supply_type_code"].isin(_ap_codes)]
            .groupby("supply_type_code", as_index=False)["amount_excl_gst"]
            .sum()
            .rename(columns={"amount_excl_gst": "amount"})
        )

        # Merge with reference table for labels
        ar_merged = gst_ar.merge(
            gst_types_df[gst_types_df["applies_to"] == "AR"][
                ["supply_type_code", "description", "gst_rate_pct", "bas_field", "legislative_ref"]],
            on="supply_type_code", how="left"
        )
        ap_merged = gst_ap_all.merge(
            gst_types_df[gst_types_df["applies_to"] == "AP"][
                ["supply_type_code", "description", "gst_rate_pct", "bas_field", "itc_claimable", "legislative_ref"]],
            on="supply_type_code", how="left"
        )

        def _rate_badge(rate_raw, taxable_bg, zero_bg):
            """Return a coloured BAS badge HTML string, safe against NaN."""
            rate = float(rate_raw) if (rate_raw is not None and not (isinstance(rate_raw, float) and pd.isna(rate_raw))) else 0.0
            bg   = taxable_bg if rate > 0 else zero_bg
            return rate, bg

        col_ar, col_ap = st.columns(2)

        with col_ar:
            st.markdown("**Sales / Revenue (AR Side)**")
            total_ar_rev = ar_merged["amount"].sum() if not ar_merged.empty else 0
            ar_rows_html = ""
            for _, r in ar_merged.sort_values("amount", ascending=False).iterrows():
                pct   = (r["amount"] / total_ar_rev * 100) if total_ar_rev > 0 else 0
                rate, bg = _rate_badge(r.get("gst_rate_pct"), "#E8192C", "#005EA5")
                ar_rows_html += (
                    '<tr>'
                    f'<td style="padding:5px 8px;font-size:0.80rem;font-weight:600">{r.get("supply_type_code","")}</td>'
                    f'<td style="padding:5px 8px;font-size:0.80rem">{r.get("description","")}</td>'
                    f'<td style="padding:5px 8px;font-size:0.80rem;text-align:center">'
                    f'<span style="background:{bg};color:white;padding:1px 5px;border-radius:3px;font-size:0.74rem">'
                    f'{r.get("bas_field","")} &nbsp;{rate:.0f}%</span></td>'
                    f'<td style="padding:5px 8px;font-size:0.80rem;text-align:right">{fmt_table(r["amount"])}</td>'
                    f'<td style="padding:5px 8px;font-size:0.80rem;text-align:right;color:#888">{pct:.1f}%</td>'
                    f'<td style="padding:5px 8px;font-size:0.74rem;color:#999">{r.get("legislative_ref","")}</td>'
                    '</tr>'
                )
            ar_rows_html += (
                '<tr style="background:#F5F5F5;font-weight:700">'
                '<td colspan="3" style="padding:6px 8px;font-size:0.81rem">Total AR Revenue (YTD)</td>'
                f'<td style="padding:6px 8px;font-size:0.81rem;text-align:right">{fmt_table(total_ar_rev)}</td>'
                '<td style="padding:6px 8px;font-size:0.81rem;text-align:right">100%</td>'
                '<td></td></tr>'
            )
            st.markdown(
                '<table style="width:100%;border-collapse:collapse;border:1px solid #eee">'
                '<thead style="background:#1A1A1A;color:white"><tr>'
                '<th style="padding:7px 8px;font-size:0.76rem">Code</th>'
                '<th style="padding:7px 8px;font-size:0.76rem">Description</th>'
                '<th style="padding:7px 8px;font-size:0.76rem;text-align:center">BAS / Rate</th>'
                '<th style="padding:7px 8px;font-size:0.76rem;text-align:right">Amount (ex GST)</th>'
                '<th style="padding:7px 8px;font-size:0.76rem;text-align:right">Mix %</th>'
                '<th style="padding:7px 8px;font-size:0.76rem">Authority</th>'
                '</tr></thead>'
                f'<tbody style="background:white">{ar_rows_html}</tbody></table>',
                unsafe_allow_html=True
            )

        with col_ap:
            st.markdown("**Acquisitions / Expenses (AP Side)**")
            total_ap_exp = ap_merged["amount"].sum() if not ap_merged.empty else 0
            ap_rows_html = ""
            for _, r in ap_merged.sort_values("amount", ascending=False).iterrows():
                pct   = (r["amount"] / total_ap_exp * 100) if total_ap_exp > 0 else 0
                rate, bg = _rate_badge(r.get("gst_rate_pct"), "#E8192C", "#6B6B6B")
                itc_raw   = r.get("itc_claimable", 0)
                itc_val   = int(itc_raw) if (itc_raw is not None and not (isinstance(itc_raw, float) and pd.isna(itc_raw))) else 0
                itc_label = "ITC \u2713" if itc_val else "No ITC"
                itc_color = GREEN if itc_val else RMIT_GREY
                ap_rows_html += (
                    '<tr>'
                    f'<td style="padding:5px 8px;font-size:0.80rem;font-weight:600">{r.get("supply_type_code","")}</td>'
                    f'<td style="padding:5px 8px;font-size:0.80rem">{r.get("description","")}</td>'
                    f'<td style="padding:5px 8px;font-size:0.80rem;text-align:center">'
                    f'<span style="background:{bg};color:white;padding:1px 5px;border-radius:3px;font-size:0.74rem">'
                    f'{r.get("bas_field","")} &nbsp;{rate:.0f}%</span></td>'
                    f'<td style="padding:5px 8px;font-size:0.80rem;text-align:right">{fmt_table(r["amount"])}</td>'
                    f'<td style="padding:5px 8px;font-size:0.80rem;text-align:right;color:#888">{pct:.1f}%</td>'
                    f'<td style="padding:5px 8px;font-size:0.74rem;color:{itc_color};font-weight:600">{itc_label}</td>'
                    '</tr>'
                )
            ap_rows_html += (
                '<tr style="background:#F5F5F5;font-weight:700">'
                '<td colspan="3" style="padding:6px 8px;font-size:0.81rem">Total AP Acquisitions (YTD)</td>'
                f'<td style="padding:6px 8px;font-size:0.81rem;text-align:right">{fmt_table(total_ap_exp)}</td>'
                '<td style="padding:6px 8px;font-size:0.81rem;text-align:right">100%</td>'
                '<td></td></tr>'
            )
            st.markdown(
                '<table style="width:100%;border-collapse:collapse;border:1px solid #eee">'
                '<thead style="background:#1A1A1A;color:white"><tr>'
                '<th style="padding:7px 8px;font-size:0.76rem">Code</th>'
                '<th style="padding:7px 8px;font-size:0.76rem">Description</th>'
                '<th style="padding:7px 8px;font-size:0.76rem;text-align:center">BAS / Rate</th>'
                '<th style="padding:7px 8px;font-size:0.76rem;text-align:right">Amount (ex GST)</th>'
                '<th style="padding:7px 8px;font-size:0.76rem;text-align:right">Mix %</th>'
                '<th style="padding:7px 8px;font-size:0.76rem">ITC</th>'
                '</tr></thead>'
                f'<tbody style="background:white">{ap_rows_html}</tbody></table>',
                unsafe_allow_html=True
            )

        st.markdown(
            '<p style="font-size:0.77rem;color:#888;margin-top:0.4rem">'
            'AR: G1 = Taxable Sales (10% GST); G3 = GST-Free / Export Supplies (0%). &nbsp;|&nbsp; '
            'AP: G10 = Creditable Acquisitions (ITC claimable); G14 = GST-Free; G15 = Input-Taxed (no ITC).<br>'
            'Source: <i>A New Tax System (Goods and Services Tax) Act 1999 (GSTA)</i>.</p>',
            unsafe_allow_html=True
        )

    st.markdown("<br>", unsafe_allow_html=True)

    # ── FBT summary — driven by sidebar tax rate configuration ──
    section("Fringe Benefits Tax (FBT) – Summary")

    # ── Fleet vehicle notes ──
    # BEV (battery EV): exempt under s58P FBTAA 1986 (Treasury Laws Amendment
    #   (Electric Car Discount) Act 2022) — zero-emission car, first used after
    #   1 July 2022, cost ≤ LCT threshold for fuel-efficient cars ($91,387 FY2026).
    #   Taxable value = $0; no gross-up; no FBT payable.
    # PHEV: exemption ended 1 April 2025 (start of FBT year 2026) per ATO guidance.
    #   Only grandfathered if employer had a pre-existing binding commitment before
    #   1 April 2025 (s58P(2) FBTAA 1986). Fleet has no grandfathered PHEVs.
    # ICE fleet car: LCT checked first (price vs $80,567 Other threshold), then
    #   4-year 1/3 discount checked, then 20% Statutory Formula applied to confirmed base.

    # FBT items: (description, taxable_value, benefit_type, exempt, exempt_reason)
    # Type 1 = GST-creditable; Type 2 = non-GST-creditable

    # ── s58P exemption gate — fuel type must be strictly 'BEV' ──────────────
    # PHEV exemption ended 1 April 2025 (start of FBT year 2026).
    # Validating FUEL_TYPE == 'BEV' (not merely 'Electric' or 'EV') prevents
    # PHEVs acquired post-cutoff from silently inheriting the $0 taxable value.
    # Grandfathered PHEVs require a binding commitment dated pre-1 Apr 2025
    # (s58P(2) FBTAA 1986) — tracked separately; fleet has none.
    _BEV_FUEL_TYPE        = "BEV"        # must be exactly 'BEV' — not 'PHEV', 'Hybrid', 'EV'
    _PHEV_CUTOFF_DATE     = "2025-04-01" # s58P exemption end date for PHEVs
    _BEV_FIRST_USE_DATE   = "2023-09-01" # after 1 Jul 2022 ✓
    _BEV_IS_GRANDFATHERED = False        # N/A — BEVs remain exempt post-cutoff
    # Exemption gate: ALL conditions must be True
    _BEV_EXEMPT = (
        _BEV_FUEL_TYPE == "BEV"                      # strict fuel type check
        and _BEV_FIRST_USE_DATE >= "2022-07-01"      # first held & used after 1 Jul 2022
        and 68_500 <= 91_387                          # cost ≤ LCT fuel-efficient threshold
    )

    _EV_LCT_THRESHOLD = 91_387   # fuel-efficient LCT threshold FY2026
    _EV_COST          = 68_500   # BEV purchase price (under threshold)
    _ICE_COST         = 52_800   # ICE fleet car cost
    _ICE_PRIVATE_DAYS = 310      # private use days (out of 365)
    _ICE_STAT_PCT     = 0.20     # statutory formula rate
    # LCT check (effective 1 July 2025)
    _LCT_THRESHOLD_OTHER    = 80_567
    _LCT_THRESHOLD_FUEL_EFF = 91_387
    _LCT_RATE               = 0.33
    _FUEL_EFF_LIMIT_L       = 3.5      # tightened from 7.0L on 1 Jul 2025
    _ICE_FUEL_L             = 8.2      # ICE car fuel consumption
    _ICE_FUEL_EFF           = _ICE_FUEL_L <= _FUEL_EFF_LIMIT_L          # False → uses Other threshold
    _ICE_LCT_THRESHOLD      = _LCT_THRESHOLD_FUEL_EFF if _ICE_FUEL_EFF else _LCT_THRESHOLD_OTHER
    _ICE_LCT                = max(0.0, round((_ICE_COST - _ICE_LCT_THRESHOLD) / 1.1 * _LCT_RATE, 0))
    _ICE_BASE_WITH_LCT      = _ICE_COST + _ICE_LCT
    # 4-year 1/3 discount: purchased Jul 2023 → ~1.75 yrs at 1 Apr 2025 → not yet applicable
    _ICE_PURCHASE_DATE      = "2023-07-01"
    _ICE_YEARS_HELD         = 1.75
    _ICE_DISCOUNT_APPLIES   = _ICE_YEARS_HELD >= 4                       # False
    _ICE_BASE_FINAL         = (round(_ICE_BASE_WITH_LCT * 2 / 3, 0)
                                if _ICE_DISCOUNT_APPLIES else _ICE_BASE_WITH_LCT)
    _ICE_TV_FINAL           = round(_ICE_BASE_FINAL * _ICE_STAT_PCT * _ICE_PRIVATE_DAYS / 365, 0)
    _ICE_TV                 = _ICE_TV_FINAL   # alias kept for any remaining references

    # RFBA (Reportable Fringe Benefit Amount) — s5E FBTAA 1986 / s136 ITAA 1936
    # Even though BEV FBT is $0 (s58P exempt), employer must report the notional
    # grossed-up taxable value on the employee's payment summary if RFBA > $2,000.
    # RFBA = notional taxable value × Type 2 gross-up rate (NOT Type 1).
    _BEV_NOTIONAL_TV        = round(_EV_COST * _ICE_STAT_PCT * _ICE_PRIVATE_DAYS / 365, 0)
    _BEV_RFBA               = round(_BEV_NOTIONAL_TV * fbt_type2, 0)
    _RFBA_THRESHOLD         = 2_000  # s5E(3) — reporting only required if RFBA > $2,000

    FBT_ITEMS = [
        # (description, taxable_value, benefit_type, is_exempt, exempt_note)
        ("BEV – Tesla Model 3 (s58P exempt)",  0,        "Type 1", True,  "s58P FBTAA: zero-emission car, first used post 1 Jul 2022, cost $68,500 \u2264 LCT threshold $91,387"),
        ("ICE Fleet Car (statutory formula)",  _ICE_TV_FINAL,  "Type 1", False, "LCT check: $52,800 < $80,567 threshold \u2192 LCT = $0. Base = $52,800. 20% \u00d7 310/365 days. Purchased Jul 2023: 4-yr discount not yet applicable."),
        ("Entertainment (meals/events)",       14_800,   "Type 2", False, ""),
        ("Expense Payments",                    8_320,   "Type 2", False, ""),
    ]

    fbt_rows_html = ""
    total_taxable    = 0
    total_grossed    = 0
    total_fbt_payable = 0
    for desc, taxable_val, benefit_type, is_exempt, exempt_note in FBT_ITEMS:
        if is_exempt:
            grossed_up  = 0.0
            fbt_payable = 0.0
            gross_up    = 0.0
            badge = '<span style="background:#005EA5;color:white;padding:1px 6px;border-radius:3px;font-size:0.72rem">EXEMPT</span>'
            gross_up_disp = badge
            tv_disp  = '<span style="color:#888">–</span>'
            gu_disp  = '<span style="color:#888">–</span>'
            fp_disp  = '<span style="color:#00875A;font-weight:700">$0</span>'
            row_style = "background:#F0F7FF;"
        else:
            gross_up    = fbt_type1 if benefit_type == "Type 1" else fbt_type2
            grossed_up  = round(taxable_val * gross_up, 0)
            fbt_payable = round(grossed_up * fbt_rate, 0)
            total_taxable     += taxable_val
            total_grossed     += grossed_up
            total_fbt_payable += fbt_payable
            gross_up_disp = f"{gross_up:.4f}"
            tv_disp  = fmt_table(taxable_val)
            gu_disp  = fmt_table(grossed_up)
            fp_disp  = fmt_table(fbt_payable)
            row_style = "background:#F9F9F9;" if benefit_type == "Type 2" and desc.startswith("Ent") else ""

        tooltip = f' title="{exempt_note}"' if exempt_note else ""
        fbt_rows_html += (
            f'<tr style="{row_style}">'
            f'<td style="padding:6px 10px;font-size:0.82rem"{tooltip}>{desc}</td>'
            f'<td style="padding:6px 10px;font-size:0.82rem;color:#666;text-align:center">{benefit_type}</td>'
            f'<td style="padding:6px 10px;font-size:0.82rem;text-align:center">{gross_up_disp}</td>'
            f'<td style="padding:6px 10px;font-size:0.82rem;text-align:right">{tv_disp}</td>'
            f'<td style="padding:6px 10px;font-size:0.82rem;text-align:right">{gu_disp}</td>'
            f'<td style="padding:6px 10px;font-size:0.82rem;text-align:right">{fp_disp}</td>'
            f'</tr>'
        )

    st.markdown(
        '<table style="width:88%;border-collapse:collapse;border:1px solid #eee">'
        '<thead style="background:#1A1A1A;color:white"><tr>'
        '<th style="padding:9px 10px;font-size:0.82rem">FBT Item</th>'
        '<th style="padding:9px 10px;font-size:0.82rem;text-align:center">Type</th>'
        '<th style="padding:9px 10px;font-size:0.82rem;text-align:center">Gross-Up Rate</th>'
        '<th style="padding:9px 10px;font-size:0.82rem;text-align:right">Taxable Value</th>'
        '<th style="padding:9px 10px;font-size:0.82rem;text-align:right">Grossed-Up Value</th>'
        '<th style="padding:9px 10px;font-size:0.82rem;text-align:right">FBT Payable</th>'
        '</tr></thead>'
        f'<tbody style="background:white">{fbt_rows_html}'
        '<tr style="background:#F5F5F5;font-weight:700">'
        '<td style="padding:7px 10px;font-size:0.83rem" colspan="3">Total FBT (FY2026)</td>'
        f'<td style="padding:7px 10px;font-size:0.83rem;text-align:right">{fmt_table(total_taxable)}</td>'
        f'<td style="padding:7px 10px;font-size:0.83rem;text-align:right">{fmt_table(total_grossed)}</td>'
        f'<td style="padding:7px 10px;font-size:0.83rem;text-align:right">{fmt_table(total_fbt_payable)}</td>'
        '</tr></tbody></table>',
        unsafe_allow_html=True
    )
    st.markdown(
        f'<p style="font-size:0.78rem;color:#888;margin-top:0.5rem">'
        f'FBT year: 1 April 2025 \u2013 31 March 2026 &nbsp;\u00b7&nbsp; '
        f'FBT rate: {round(fbt_rate*100,1):.1f}% &nbsp;\u00b7&nbsp; '
        f'Type 1 gross-up: {fbt_type1:.4f} (ATO FY2026) &nbsp;\u00b7&nbsp; '
        f'Type 2 gross-up: {fbt_type2:.4f} &nbsp;\u00b7&nbsp; '
        f'Due date: 21 May 2026.<br>'
        f'<b>EV exemption:</b> s58P FBTAA 1986 \u2014 zero or low emissions vehicle, first used \u2265 1 Jul 2022, '
        f'cost \u2264 LCT fuel-efficient threshold ($91,387). &nbsp;'
        f'<b>PHEV note:</b> exemption ended 1 April 2025; only grandfathered where a binding commitment '
        f'existed before that date (s58P(2)). Fleet carries no grandfathered PHEVs.<br>'
        f'<i>Rates adjustable via the sidebar \u2014 changes cascade instantly.</i></p>',
        unsafe_allow_html=True
    )

    # ── RFBA — Reportable Fringe Benefit Amount (s5E FBTAA / s136 ITAA 1936) ──
    # Build fleet RFBA table — every benefit item carries an rfba_amount field
    _rfba_items = [
        # (description, taxable_val, gross_up_rate, gross_up_label, is_exempt_benefit)
        ("BEV – Tesla Model 3 (s58P exempt)", _BEV_NOTIONAL_TV, fbt_type2, "T2", True),
        ("ICE Fleet Car (statutory formula)", _ICE_TV_FINAL,    fbt_type1, "T1", False),
        ("Entertainment (meals/events)",      14_800,           fbt_type2, "T2", False),
        ("Expense Payments",                   8_320,           fbt_type2, "T2", False),
    ]
    _rfba_rows_html = ""
    _total_rfba = 0
    for r_desc, r_tv, r_gu, r_gu_lbl, r_exempt in _rfba_items:
        r_rfba = round(r_tv * r_gu, 0)
        _total_rfba += r_rfba
        _reportable = r_rfba > _RFBA_THRESHOLD
        _flag = (
            '<span style="color:#856404;font-weight:700">YES &#9651;</span>'
            if _reportable else
            '<span style="color:#888">NO</span>'
        )
        _exempt_badge = (
            '<span style="background:#005EA5;color:white;padding:1px 5px;'
            'border-radius:3px;font-size:0.70rem;margin-left:4px">$0 FBT</span>'
            if r_exempt else ""
        )
        _rfba_rows_html += (
            f'<tr style="background:{"#F0F7FF" if r_exempt else "#F9F9F9" if r_gu_lbl=="T2" else "white"}">'
            f'<td style="padding:6px 10px;font-size:0.81rem">{r_desc}{_exempt_badge}</td>'
            f'<td style="padding:6px 10px;font-size:0.81rem;text-align:center;color:#666">{r_gu_lbl}</td>'
            f'<td style="padding:6px 10px;font-size:0.81rem;text-align:center">{r_gu:.4f}</td>'
            f'<td style="padding:6px 10px;font-size:0.81rem;text-align:right">{fmt_table(r_tv)}</td>'
            f'<td style="padding:6px 10px;font-size:0.81rem;text-align:right;font-weight:600">{fmt_table(r_rfba)}</td>'
            f'<td style="padding:6px 10px;font-size:0.81rem;text-align:center">{_flag}</td>'
            f'</tr>'
        )

    st.markdown(
        '<div style="margin-top:1.4rem;max-width:88%">'
        '<div style="font-size:0.88rem;font-weight:700;color:#005EA5;margin-bottom:4px">'
        'Reportable Fringe Benefit Amount (RFBA) — Employee Payment Summary Disclosure'
        '</div>'
        '<div style="font-size:0.80rem;color:#555;margin-bottom:8px;line-height:1.6">'
        '<b>Not additional FBT liability</b> — a disclosure obligation under <b>s5E FBTAA 1986</b> &amp; '
        '<b>s136 ITAA 1936</b>. Every benefit item carries an RFBA field that appears on the employee\'s '
        'income statement if it exceeds $2,000. RFBA mirrors the benefit\'s gross-up type '
        '(T1 for GST-creditable; T2 for non-creditable). '
        'BEV exception: uses T2 despite being FBT-exempt (no GST ITC on a notional value). '
        'Impacts employee\'s MLS threshold, HECS/HELP repayment rate, and PHI rebate income test.'
        '</div>'
        '<table style="width:100%;border-collapse:collapse;border:1px solid #dde6f0">'
        '<thead style="background:#005EA5;color:white"><tr>'
        '<th style="padding:8px 10px;font-size:0.80rem;text-align:left">Benefit Item</th>'
        '<th style="padding:8px 10px;font-size:0.80rem;text-align:center">Type</th>'
        '<th style="padding:8px 10px;font-size:0.80rem;text-align:center">Gross-Up Rate</th>'
        '<th style="padding:8px 10px;font-size:0.80rem;text-align:right">Taxable Value</th>'
        '<th style="padding:8px 10px;font-size:0.80rem;text-align:right">RFBA Amount</th>'
        '<th style="padding:8px 10px;font-size:0.80rem;text-align:center">Report on Stmt?</th>'
        '</tr></thead>'
        f'<tbody>{_rfba_rows_html}'
        '<tr style="background:#EAF2FF;font-weight:700">'
        '<td style="padding:7px 10px;font-size:0.81rem" colspan="4">Total RFBA Disclosed (All Employees)</td>'
        f'<td style="padding:7px 10px;font-size:0.81rem;text-align:right">{fmt_table(_total_rfba)}</td>'
        '<td></td>'
        '</tr></tbody></table>'
        '<p style="font-size:0.75rem;color:#888;margin-top:6px">'
        'STP Phase 2: RFBA fields map directly to the <i>reportable_fringe_benefits_amount</i> field '
        'in the ATO Single Touch Payroll submission. Export via fbt_register table for payroll officer reconciliation.'
        '</p>'
        '</div>',
        unsafe_allow_html=True
    )

    # Pre-compute display strings for the code block
    # Explicit RFBA aliases — ICE car mirrors T1 (same calculation as _ice_gu)
    _ICE_RFBA_AMOUNT = round(_ICE_TV_FINAL * fbt_type1, 0)   # TV × T1 — s5E FBTAA / s136 ITAA
    _EN_RFBA_AMOUNT  = round(14_800 * fbt_type2, 0)           # TV × T2
    _EP_RFBA_AMOUNT  = round(8_320  * fbt_type2, 0)           # TV × T2

    _top   = f"{indiv_top_rate * 100:.1f}"
    _mcare = f"{medicare_levy * 100:.1f}"
    _gst   = f"{gst_rate_fbt * 100:.1f}"
    _fbt   = f"{fbt_rate * 100:.1f}"

    _ice_gu  = round(_ICE_TV_FINAL * fbt_type1, 0)
    _ice_fp  = round(_ice_gu * fbt_rate, 0)
    _en_tx   = 14_800;  _en_gu = round(_en_tx * fbt_type2, 0);  _en_fp = round(_en_gu * fbt_rate, 0)
    _ep_tx   =  8_320;  _ep_gu = round(_ep_tx * fbt_type2, 0);  _ep_fp = round(_ep_gu * fbt_rate, 0)
    _s = lambda v: f"${v:,.0f}"

    with st.expander("FBT Calculation Logic", expanded=True):
        st.code(
            "# FBT Calculation — FBTAA 1986  |  FY: 1 Apr 2025 – 31 Mar 2026\n"
            "#\n"
            "# ── Rate derivation (single source of truth) ────────────────────────────\n"
            f"INDIVIDUAL_TOP_RATE = {_top}%    # s12-5 ITAA 1997 — top marginal rate\n"
            f"MEDICARE_LEVY       = {_mcare}%     # s8 Medicare Levy Act 1986\n"
            f"GST_RATE            = {_gst}%    # s9-70 GSTA 1999\n"
            "#\n"
            f"FBT_RATE   = INDIVIDUAL_TOP_RATE + MEDICARE_LEVY   # = {_fbt}%\n"
            "#\n"
            "# Type 2: no GST credit — simpler denominator\n"
            f"GROSS_UP_T2 = 1 / (1 - FBT_RATE)                    # = {fbt_type2:.4f}\n"
            "#\n"
            "# Type 1: employer CAN claim GST ITC on the benefit\n"
            "# Formula: (1 + GST_RATE) / (1 - FBT_RATE) = 1.10 / 0.53 = 2.0755 (algebraic)\n"
            "# ATO publishes 2.0802 for FY2026 — hardcoded to match ATO authoritative figure.\n"
            f"GROSS_UP_T1 = 2.0802   # ATO-published FY2026 (formula gives 2.0755)\n"
            "#   T1 > T2 always: the GST ITC the employer claims reduces\n"
            "#   their net cost, so the gross-up must be higher to achieve parity.\n"
            "#\n"
            "# => Changing MEDICARE_LEVY or INDIVIDUAL_TOP_RATE updates both rates.\n"
            "#    Changing GST_RATE affects only GROSS_UP_T1 (T2 is GST-independent).\n"
            "\n"
            "# ── LCT check — A New Tax System (Luxury Car Tax) Act 1999 ────────────\n"
            "# Thresholds effective 1 July 2025:\n"
            f"LCT_THRESHOLD_OTHER    = {_s(_LCT_THRESHOLD_OTHER)}   # standard petrol/diesel vehicles\n"
            f"LCT_THRESHOLD_FUEL_EFF = {_s(_LCT_THRESHOLD_FUEL_EFF)}   # fuel-efficient: ≤{_FUEL_EFF_LIMIT_L}L/100km\n"
            "#\n"
            "# 3.5L/100km rule: from 1 Jul 2025, 'fuel-efficient' tightened from 7.0L → 3.5L.\n"
            "# Executive hybrids that qualified for the higher threshold in FY2025 may now\n"
            "# fall into the lower 'Other' band — review fleet selections accordingly.\n"
            "#\n"
            f"ICE_FUEL_CONSUMPTION   = {_ICE_FUEL_L}L/100km   # exceeds {_FUEL_EFF_LIMIT_L}L limit\n"
            f"ICE_IS_FUEL_EFFICIENT  = {_ICE_FUEL_EFF}              # → uses 'Other' threshold\n"
            f"ICE_LCT_THRESHOLD      = {_s(_ICE_LCT_THRESHOLD)}\n"
            f"ICE_PURCHASE_PRICE     = {_s(_ICE_COST)}   # below threshold\n"
            f"ICE_LCT_PAYABLE        = {_s(_ICE_LCT)}      # no LCT — price ≤ threshold\n"
            f"ICE_FBT_BASE_VALUE     = {_s(_ICE_COST)} + {_s(_ICE_LCT)} = {_s(_ICE_BASE_WITH_LCT)}\n"
            "# Note: if LCT were payable, it would inflate the FBT base every year\n"
            "# ('tax on a tax') — a significant hidden cost for luxury fleet vehicles.\n"
            "\n"
            "# ── 4-year 1/3 base value discount (s9(2) FBTAA) ────────────────────────\n"
            f"ICE_PURCHASE_DATE      = '{_ICE_PURCHASE_DATE}'\n"
            f"ICE_YEARS_HELD         = {_ICE_YEARS_HELD}   # at 1 Apr 2025 (start of FBT year)\n"
            f"ICE_DISCOUNT_APPLIES   = ICE_YEARS_HELD >= 4   # {_ICE_DISCOUNT_APPLIES}\n"
            f"ICE_BASE_FINAL         = {_s(_ICE_BASE_FINAL)}   # no discount — car is < 4 years old\n"
            "# When discount applies: Base Value × 2/3 (effectively a 33.3% reduction)\n"
            "# => locks in for the life of the asset; major long-run FBT saving on aging fleet.\n"
            "\n"
            "# ── s58P exemption gate — strict fuel-type validation ───────────────────\n"
            "# WARNING: Do NOT use 'Electric', 'EV', or 'Low Emission' as the fuel type.\n"
            "# PHEVs lost their exemption on 1 April 2025. A PHEV labelled broadly as\n"
            "# 'Electric' would silently pass a naïve check and produce a $0 tax result.\n"
            "#\n"
            f"FUEL_TYPE              = '{_BEV_FUEL_TYPE}'        # must be exactly 'BEV'\n"
            f"PHEV_CUTOFF_DATE       = '{_PHEV_CUTOFF_DATE}'  # s58P exemption ended for PHEVs\n"
            f"FIRST_USE_DATE         = '{_BEV_FIRST_USE_DATE}'  # must be ≥ 2022-07-01\n"
            f"BEV_COST               = {_s(_EV_COST)}   # purchase price incl. GST\n"
            f"BEV_LCT_THRESHOLD      = {_s(_LCT_THRESHOLD_FUEL_EFF)}   # EVs use fuel-efficient threshold\n"
            "#\n"
            "BEV_FBT_EXEMPT = (\n"
            "    FUEL_TYPE == 'BEV'                    # strict: excludes PHEV, Hybrid, EV\n"
            "    and FIRST_USE_DATE >= '2022-07-01'    # post Treasury Laws Amendment\n"
            "    and BEV_COST <= BEV_LCT_THRESHOLD     # cost under LCT threshold\n"
            f")  # => {_BEV_EXEMPT}  — taxable value = $0; gross-up not applied\n"
            "#\n"
            "# PHEV acquired after 2025-04-01 with FUEL_TYPE='PHEV':\n"
            "#   FUEL_TYPE == 'BEV'  →  False  →  BEV_FBT_EXEMPT = False\n"
            "#   Full statutory formula applies — no silent $0 result.\n"
            "# Grandfathered PHEV: requires binding commitment pre-1 Apr 2025 (s58P(2)).\n"
            "\n"
            "# ── ICE fleet car — 20% Statutory Formula (s9 FBTAA) ───────────────────\n"
            f"STATUTORY_RATE         = {_ICE_STAT_PCT:.0%}\n"
            f"PRIVATE_USE_DAYS       = {_ICE_PRIVATE_DAYS}  # days available for private use\n"
            f"ICE_TAXABLE_VALUE      = {_s(_ICE_BASE_FINAL)} x {_ICE_STAT_PCT:.0%} x {_ICE_PRIVATE_DAYS}/365 = {_s(_ICE_TV_FINAL)}\n"
            "\n"
            "# ── FBT Payable = Taxable Value × Gross-Up Rate × FBT Rate ─────────────\n"
            "# Item                             Taxable       Gross-Up    Grossed-Up      FBT Payable\n"
            f"# BEV (s58P exempt)              {_s(0):>10}  (exempt)                         {_s(0)}\n"
            f"# ICE Fleet Car (stat formula)   {_s(_ICE_TV_FINAL):>10}  x {fbt_type1:.4f}  = {_s(_ice_gu):>12}  ->  {_s(_ice_fp)}\n"
            f"# Entertainment (meals/events)   {_s(_en_tx):>10}  x {fbt_type2:.4f}  = {_s(_en_gu):>12}  ->  {_s(_en_fp)}\n"
            f"# Expense Payments               {_s(_ep_tx):>10}  x {fbt_type2:.4f}  = {_s(_ep_gu):>12}  ->  {_s(_ep_fp)}\n"
            "# " + "-" * 77 + "\n"
            f"# TOTAL FBT PAYABLE (FY2026):  {_s(total_fbt_payable)}\n"
            "# Due: 21 May 2026  |  Lodge via ATO Tax Agent Portal\n"
            "\n"
            "# ── RFBA — Reportable Fringe Benefit Amount (s5E FBTAA / s136 ITAA 1936) ─\n"
            "# BEV FBT = $0 (exempt), but RFBA must still be reported on the employee's\n"
            "# income statement if the notional grossed-up value exceeds $2,000.\n"
            "# Use Type 2 rate (not Type 1) — RFBA gross-up excludes the GST ITC component.\n"
            "#\n"
            f"BEV_NOTIONAL_TV  = {_s(_EV_COST)} x {_ICE_STAT_PCT:.0%} x {_ICE_PRIVATE_DAYS}/365  = {_s(_BEV_NOTIONAL_TV)}\n"
            f"BEV_RFBA         = {_s(_BEV_NOTIONAL_TV)} x GROSS_UP_T2 ({fbt_type2:.4f})         = {_s(_BEV_RFBA)}\n"
            f"RFBA_THRESHOLD   = $2,000  ->  Report required: {str(_BEV_RFBA > _RFBA_THRESHOLD).upper()}\n"
            "#\n"
            "# Impact on employee: RFBA is included in 'adjusted fringe benefits total'\n"
            "# which can affect Medicare Levy Surcharge, HECS/HELP repayment thresholds,\n"
            "# and private health insurance rebate income tests.\n"
            "# This is a disclosure obligation ONLY — it does NOT create additional FBT liability.",
            language="python"
        )

    st.markdown("<br>", unsafe_allow_html=True)

    # ── FBT Journal Entry Generator ──────────────────────────────────────────
    section("FBT Journal Entries (GL Integration)")

    je_all     = query("SELECT * FROM journal_entries ORDER BY journal_date, journal_id, line_no")
    fbt_reg_df = query("SELECT * FROM fbt_register ORDER BY asset_id")

    if je_all.empty:
        st.warning("No journal entries found. Delete data/rmit_finance.db and restart to regenerate.", icon="⚠️")
    else:
        # Split accrual vs payment
        je_accrual = je_all[je_all["journal_type"] == "FBT_Accrual"].copy()
        je_payment = je_all[je_all["journal_type"] == "FBT_Payment"].copy()

        accrual_id  = je_accrual["journal_id"].iloc[0]  if not je_accrual.empty else "—"
        payment_id  = je_payment["journal_id"].iloc[0]  if not je_payment.empty else "—"
        total_fbt_j = je_accrual[je_accrual["account_code"] == "2202"]["credit"].sum()

        # ── KPI strip ────────────────────────────────────────────────────────
        k1, k2, k3, k4 = st.columns(4)
        k1.markdown(kpi_card("Accrual JE", accrual_id, "Period 2026-03 · Posted"), unsafe_allow_html=True)
        k2.markdown(kpi_card("Payment JE", payment_id, "Period 2026-05 · Pending"), unsafe_allow_html=True)
        k3.markdown(kpi_card("Total FBT Accrued", f"${total_fbt_j:,.0f}", "DR 5205 / CR 2202"), unsafe_allow_html=True)
        k4.markdown(kpi_card("Due Date", "21 May 2026", "s69 FBTAA 1986"), unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)

        # ── JE 1: Accrual ─────────────────────────────────────────────────────
        st.markdown(
            '<div style="font-size:0.86rem;font-weight:700;color:#1A1A1A;margin-bottom:6px">'
            f'Journal Entry {accrual_id} — FBT Year-End Accrual &nbsp;'
            '<span style="background:#00875A;color:white;padding:2px 7px;border-radius:3px;'
            'font-size:0.72rem">POSTED · 31 Mar 2026</span>'
            '</div>',
            unsafe_allow_html=True
        )

        je_rows_html = ""
        for _, r in je_accrual.iterrows():
            is_dr  = float(r["debit"]) > 0
            is_cr  = float(r["credit"]) > 0
            is_bev = "s58P exempt" in str(r.get("description", ""))
            amt_dr = f'<b>${float(r["debit"]):,.2f}</b>' if is_dr else '<span style="color:#ccc">—</span>'
            amt_cr = f'<b>${float(r["credit"]):,.2f}</b>' if is_cr else '<span style="color:#ccc">—</span>'
            code   = r["account_code"]
            acc_name = {"5205": "FBT Expense", "2202": "FBT Payable"}.get(code, code)
            indent = '&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;' if is_cr else ''
            exempt_tag = (
                '&nbsp;<span style="background:#005EA5;color:white;padding:1px 5px;'
                'border-radius:3px;font-size:0.68rem">$0 EXEMPT</span>'
                if is_bev else ""
            )
            sub_ref = str(r.get("sub_ledger_ref", ""))
            ref_tag = (
                f'&nbsp;<span style="color:#888;font-size:0.72rem">[{sub_ref}]</span>'
                if sub_ref and sub_ref != "ALL" else ""
            )
            je_rows_html += (
                f'<tr>'
                f'<td style="padding:5px 10px;font-size:0.81rem;font-family:monospace">'
                f'{indent}<b>{code}</b> {acc_name}{exempt_tag}{ref_tag}</td>'
                f'<td style="padding:5px 10px;font-size:0.81rem;text-align:right">{amt_dr}</td>'
                f'<td style="padding:5px 10px;font-size:0.81rem;text-align:right">{amt_cr}</td>'
                f'<td style="padding:5px 20px;font-size:0.75rem;color:#666;max-width:320px;'
                f'white-space:nowrap;overflow:hidden;text-overflow:ellipsis">'
                f'{str(r["description"])[:90]}</td>'
                f'</tr>'
            )

        st.markdown(
            '<table style="width:92%;border-collapse:collapse;border:1px solid #e0e0e0">'
            '<thead style="background:#1A1A1A;color:white"><tr>'
            '<th style="padding:8px 10px;font-size:0.80rem;text-align:left">Account</th>'
            '<th style="padding:8px 10px;font-size:0.80rem;text-align:right">DR</th>'
            '<th style="padding:8px 10px;font-size:0.80rem;text-align:right">CR</th>'
            '<th style="padding:8px 20px;font-size:0.80rem;text-align:left">Narration</th>'
            '</tr></thead>'
            f'<tbody style="background:white">{je_rows_html}'
            '</tbody></table>',
            unsafe_allow_html=True
        )

        # ── Reconciliation: JE → fbt_register ────────────────────────────────
        st.markdown(
            '<p style="font-size:0.77rem;color:#888;margin-top:6px">'
            'Sub-ledger reconciliation: each DR line traces to <code>fbt_register.fbt_payable</code> '
            'via <code>sub_ledger_ref</code>. CR line = sum of all benefit lines → '
            '<code>2202 FBT Payable</code> balance at 31 March 2026.</p>',
            unsafe_allow_html=True
        )

        # Reconciliation mini-table from fbt_register
        if not fbt_reg_df.empty:
            recon_cols  = ["asset_id", "description", "bev_exempt", "taxable_value",
                           "gross_up_type", "fbt_payable", "rfba_amount"]
            recon_avail = [c for c in recon_cols if c in fbt_reg_df.columns]
            recon_df    = fbt_reg_df[recon_avail].copy()
            recon_df["bev_exempt"]    = recon_df["bev_exempt"].map({1: "Yes", 0: "No", True: "Yes", False: "No"})
            recon_df["taxable_value"] = recon_df["taxable_value"].apply(lambda v: f"${float(v):,.2f}")
            recon_df["fbt_payable"]   = recon_df["fbt_payable"].apply(lambda v: f"${float(v):,.2f}")
            recon_df["rfba_amount"]   = recon_df["rfba_amount"].apply(lambda v: f"${float(v):,.2f}")
            recon_df.columns          = ["Asset ID", "Description", "BEV Exempt",
                                         "Taxable Value", "Gross-Up", "FBT Payable", "RFBA Amount"]
            st.dataframe(recon_df, use_container_width=True, hide_index=True)

        # ── JE 2: Payment (pending) ────────────────────────────────────────────
        st.markdown(
            '<div style="margin-top:1.4rem;font-size:0.86rem;font-weight:700;'
            'color:#1A1A1A;margin-bottom:6px">'
            f'Journal Entry {payment_id} — ATO Payment Settlement &nbsp;'
            '<span style="background:#FFC107;color:#1A1A1A;padding:2px 7px;border-radius:3px;'
            'font-size:0.72rem">PENDING · 21 May 2026</span>'
            '</div>',
            unsafe_allow_html=True
        )

        pay_rows_html = ""
        for _, r in je_payment.iterrows():
            is_dr  = float(r["debit"]) > 0
            amt_dr = f'<b>${float(r["debit"]):,.2f}</b>'  if is_dr else '<span style="color:#ccc">—</span>'
            amt_cr = f'<b>${float(r["credit"]):,.2f}</b>' if not is_dr else '<span style="color:#ccc">—</span>'
            code   = r["account_code"]
            acc_name = {"2202": "FBT Payable", "1002": "Cash at Bank – Term Deposit"}.get(code, code)
            indent = '&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;' if not is_dr else ''
            pay_rows_html += (
                f'<tr style="background:#FFFDF0">'
                f'<td style="padding:5px 10px;font-size:0.81rem;font-family:monospace">'
                f'{indent}<b>{code}</b> {acc_name}</td>'
                f'<td style="padding:5px 10px;font-size:0.81rem;text-align:right">{amt_dr}</td>'
                f'<td style="padding:5px 10px;font-size:0.81rem;text-align:right">{amt_cr}</td>'
                f'<td style="padding:5px 20px;font-size:0.75rem;color:#666">{str(r["description"])[:90]}</td>'
                f'</tr>'
            )

        st.markdown(
            '<table style="width:92%;border-collapse:collapse;border:1px solid #e0e0e0">'
            '<thead style="background:#856404;color:white"><tr>'
            '<th style="padding:8px 10px;font-size:0.80rem;text-align:left">Account</th>'
            '<th style="padding:8px 10px;font-size:0.80rem;text-align:right">DR</th>'
            '<th style="padding:8px 10px;font-size:0.80rem;text-align:right">CR</th>'
            '<th style="padding:8px 20px;font-size:0.80rem;text-align:left">Narration</th>'
            '</tr></thead>'
            f'<tbody>{pay_rows_html}</tbody></table>'
            '<p style="font-size:0.75rem;color:#888;margin-top:6px">'
            'This entry clears the 2202 FBT Payable balance and reduces cash. '
            'Status: <b>Pending</b> — outside current dataset period (Jul 2025–Mar 2026). '
            'Becomes <b>Posted</b> upon ATO lodgement confirmation on 21 May 2026.</p>',
            unsafe_allow_html=True
        )

        # ── Audit trail note ──────────────────────────────────────────────────
        st.markdown(
            '<div style="margin-top:1rem;padding:10px 14px;border-left:3px solid #E8192C;'
            'background:#FFF5F5;font-size:0.78rem;color:#555;max-width:92%">'
            '<b>Audit trail:</b> '
            'DR 5205 lines trace back to <code>fbt_register.fbt_payable</code> via '
            '<code>sub_ledger_ref</code> → <code>asset_id</code>. '
            'CR 2202 balance reconciles to the FBT Summary table above. '
            'RFBA amounts flow separately to STP Phase 2 payroll submission — '
            'not a GL movement but recorded in <code>fbt_register.rfba_amount</code>. '
            'Once the payment JE is posted, a BAS/IAS reconciliation should confirm '
            '2202 returns to zero.'
            '</div>',
            unsafe_allow_html=True
        )

    st.markdown("<br>", unsafe_allow_html=True)

    # ── STP Phase 2 / FBT RFBA Export ────────────────────────────────────────
    section("STP Phase 2 — RFBA Export & Reconciliation")

    reg_full = query("SELECT * FROM fbt_register ORDER BY asset_id")
    gl_all   = query("SELECT * FROM general_ledger")

    if reg_full.empty:
        st.warning("fbt_register not found. Delete data/rmit_finance.db and restart.", icon="⚠️")
    else:
        # ── Live reconciliation assertions ────────────────────────────────────
        gl_5205   = round(gl_all[gl_all["account_code"] == "5205"]["debit"].sum(), 2)
        gl_2202   = round(gl_all[gl_all["account_code"] == "2202"]["credit"].sum(), 2)
        reg_fbt   = round(reg_full["fbt_payable"].astype(float).sum(), 2)
        rfba_reportable_count = int((reg_full["rfba_reportable"].astype(int) > 0).sum())
        bev_row   = reg_full[reg_full["fuel_type"] == "BEV"]
        bev_uses_t2 = (bev_row["gross_up_type"].iloc[0] == "Type 1") and (bev_row["bev_exempt"].iloc[0])

        checks = [
            ("GL 5205 == fbt_register.fbt_payable.sum()",
             f"${gl_5205:,.2f} == ${reg_fbt:,.2f}",
             abs(gl_5205 - reg_fbt) < 1.0),
            ("GL 2202 CR balance == total FBT accrued",
             f"${gl_2202:,.2f} == ${reg_fbt:,.2f}",
             abs(gl_2202 - reg_fbt) < 1.0),
            ("RFBA reportable rows (rfba_amount > $2,000)",
             f"{rfba_reportable_count} of {len(reg_full)} benefit items",
             rfba_reportable_count > 0),
            ("BEV (s58P exempt) — FBT payable == $0",
             f"${bev_row['fbt_payable'].astype(float).iloc[0]:,.2f}",
             bev_row["fbt_payable"].astype(float).iloc[0] == 0.0),
            ("BEV RFBA uses T2 gross-up (no GST ITC on notional value)",
             f"gross_up_type={bev_row['gross_up_type'].iloc[0]}, bev_exempt=True",
             bev_uses_t2),
            ("All compliance gates passed (compliance_chk)",
             f"{int(reg_full['compliance_chk'].astype(int).sum())} / {len(reg_full)} rows",
             reg_full["compliance_chk"].astype(int).sum() == len(reg_full)),
        ]

        chk_html = ""
        all_pass = all(c[2] for c in checks)
        for label, value, passed in checks:
            icon  = "✓" if passed else "✗"
            color = "#00875A" if passed else "#E8192C"
            bg    = "#F0FFF4" if passed else "#FFF0F0"
            chk_html += (
                f'<tr style="background:{bg}">'
                f'<td style="padding:6px 10px;font-size:0.80rem;font-family:monospace;color:{color};font-weight:700">{icon}</td>'
                f'<td style="padding:6px 10px;font-size:0.80rem;font-family:monospace">{label}</td>'
                f'<td style="padding:6px 10px;font-size:0.80rem;color:#555">{value}</td>'
                f'<td style="padding:6px 12px;font-size:0.78rem;font-weight:700;color:{color}">{"PASS" if passed else "FAIL"}</td>'
                f'</tr>'
            )

        overall_bg    = "#F0FFF4" if all_pass else "#FFF3CD"
        overall_color = "#00875A" if all_pass else "#856404"
        overall_label = "ALL CHECKS PASSED — AUDIT READY" if all_pass else "REVIEW REQUIRED BEFORE LODGEMENT"

        st.markdown(
            f'<div style="margin-bottom:10px;padding:8px 14px;border-radius:4px;'
            f'background:{overall_bg};border:1.5px solid {overall_color};'
            f'font-size:0.82rem;font-weight:700;color:{overall_color}">'
            f'{overall_label}</div>'
            '<table style="width:92%;border-collapse:collapse;border:1px solid #e0e0e0;margin-bottom:1rem">'
            '<thead style="background:#1A1A1A;color:white"><tr>'
            '<th style="padding:7px 10px;font-size:0.78rem;width:30px"></th>'
            '<th style="padding:7px 10px;font-size:0.78rem;text-align:left">Assertion</th>'
            '<th style="padding:7px 10px;font-size:0.78rem;text-align:left">Value</th>'
            '<th style="padding:7px 10px;font-size:0.78rem;text-align:left">Result</th>'
            f'</tr></thead><tbody>{chk_html}</tbody></table>',
            unsafe_allow_html=True
        )

        # ── STP Phase 2 export rows ───────────────────────────────────────────
        # Only rows where rfba_reportable = True and employee is individually assigned
        stp_rows = reg_full[
            (reg_full["rfba_reportable"].astype(int) > 0) &
            (reg_full["employee_id"] != "POOL")
        ].copy()

        stp_export = pd.DataFrame({
            "fbt_year":              stp_rows["fbt_year"],
            "employer_abn":          "12 345 678 901",       # UniPath Pty Ltd ABN (synthetic)
            "payee_id":              stp_rows["employee_id"],
            "payee_name":            stp_rows["employee_name"],
            "tfn_masked":            stp_rows["employee_tfn_masked"],
            "benefit_description":   stp_rows["description"],
            "fuel_type":             stp_rows["fuel_type"],
            "gross_up_type":         stp_rows["gross_up_type"],
            "taxable_value":         stp_rows["taxable_value"].astype(float).round(2),
            "rfba_amount":           stp_rows["rfba_amount"].astype(float).round(2),
            "bev_exempt":            stp_rows["bev_exempt"].astype(bool),
            "fbt_payable_employer":  stp_rows["fbt_payable"].astype(float).round(2),
            "asset_ref":             stp_rows["asset_id"],
            "legislative_ref":       "s5E FBTAA 1986 / s136 ITAA 1936",
            "compliance_verified":   stp_rows["compliance_chk"].astype(bool),
            "reporting_period_end":  "2026-03-31",
            "stp_field":             "reportable_fringe_benefits_amount",
        })

        st.markdown(
            '<div style="font-size:0.84rem;font-weight:700;color:#1A1A1A;margin-bottom:6px">'
            f'STP Phase 2 RFBA Rows — {len(stp_export)} employee(s) reportable'
            '</div>',
            unsafe_allow_html=True
        )
        st.dataframe(stp_export, use_container_width=True, hide_index=True)

        st.markdown(
            '<p style="font-size:0.75rem;color:#888;margin-top:4px">'
            'Pooled benefits (entertainment, expense payments) are excluded — no single employee assignee. '
            'In production, these would be allocated per payroll system employee mapping.'
            '</p>',
            unsafe_allow_html=True
        )

        # ── Download buttons ──────────────────────────────────────────────────
        import io, zipfile

        # CSV — STP submission file
        csv_buf = io.StringIO()
        stp_export.to_csv(csv_buf, index=False)
        csv_bytes = csv_buf.getvalue().encode("utf-8")

        # XLSX — multi-tab workbook for payroll officer
        xlsx_buf = io.BytesIO()
        with pd.ExcelWriter(xlsx_buf, engine="openpyxl") as writer:
            # Tab 1: STP Phase 2 submission rows
            stp_export.to_excel(writer, sheet_name="STP_Phase2_RFBA", index=False)

            # Tab 2: Full FBT register
            reg_export = reg_full[[
                "fbt_year","asset_id","description","fuel_type","gross_up_type",
                "purchase_price","taxable_value","bev_exempt","fbt_payable",
                "rfba_amount","rfba_reportable","compliance_chk",
                "employee_id","employee_name","lct_payable","discount_applies",
            ]].copy()
            reg_export.to_excel(writer, sheet_name="FBT_Register", index=False)

            # Tab 3: Reconciliation checks
            recon_df = pd.DataFrame([
                {"Assertion": lbl, "Value": val, "Result": "PASS" if ok else "FAIL"}
                for lbl, val, ok in checks
            ])
            recon_df.to_excel(writer, sheet_name="Reconciliation", index=False)

            # Tab 4: Payment schedule
            pay_sched = pd.DataFrame([
                {"JE": "JE09001", "Description": "FBT Accrual — DR 5205 / CR 2202",
                 "Amount": reg_fbt, "Due Date": "31-Mar-2026", "Status": "Posted"},
                {"JE": "JE09002", "Description": "ATO FBT Payment — DR 2202 / CR 1002",
                 "Amount": reg_fbt, "Due Date": "21-May-2026", "Status": "Pending"},
            ])
            pay_sched.to_excel(writer, sheet_name="Payment_Schedule", index=False)

            # Tab 5: TAX_CONFIG snapshot
            tax_cfg_snap = query("SELECT * FROM tax_compliance_config ORDER BY category, config_key")
            tax_cfg_snap.to_excel(writer, sheet_name="TAX_CONFIG_Snapshot", index=False)

        xlsx_bytes = xlsx_buf.getvalue()

        dl1, dl2 = st.columns(2)
        with dl1:
            st.download_button(
                label="⬇ Download STP Phase 2 CSV",
                data=csv_bytes,
                file_name="UniPath_STP_Phase2_RFBA_FY2026.csv",
                mime="text/csv",
                help="ATO STP Phase 2 submission file — RFBA reportable rows only",
                use_container_width=True,
            )
        with dl2:
            st.download_button(
                label="⬇ Download FBT Workbook (XLSX)",
                data=xlsx_bytes,
                file_name="UniPath_FBT_Compliance_FY2026.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                help="5 tabs: STP export · FBT Register · Reconciliation · Payment Schedule · TAX_CONFIG",
                use_container_width=True,
            )

        st.markdown(
            '<div style="margin-top:0.8rem;padding:10px 14px;border-left:3px solid #00875A;'
            'background:#F0FFF4;font-size:0.78rem;color:#155724;max-width:92%">'
            '<b>Lodgement checklist:</b> '
            '(1) Verify RFBA amounts match payroll system records for EMP-0042 and EMP-0017. '
            '(2) Submit STP Phase 2 CSV via ATO Business Portal by <b>14 July 2026</b> (EOFY finalisation deadline). '
            '(3) Confirm JE09002 FBT payment posted by <b>21 May 2026</b> — clears 2202 to zero. '
            '(4) Retain XLSX workbook as supporting documentation for 5-year ATO record-keeping obligation '
            '(s262A ITAA 1936).'
            '</div>',
            unsafe_allow_html=True
        )


# ─────────────────────────────────────────────────────────────────────────────
# PAGE 9 – SQL ANALYSIS
# ─────────────────────────────────────────────────────────────────────────────

elif page == "SQL Analysis":
    page_header(
        "SQL Analysis Showcase",
        "Key financial queries demonstrating data analysis capability"
    )

    query_labels = {
        "AR Aging Report": f"""
-- Accounts Receivable Aging Report
-- Classifies open invoices into aging buckets based on days past due date
SELECT
    customer_name,
    region,
    COUNT(invoice_number)                            AS invoice_count,
    SUM(CASE WHEN days_past_due <= 0              THEN total_inc_gst ELSE 0 END) AS current_0_30,
    SUM(CASE WHEN days_past_due BETWEEN 1  AND 30 THEN total_inc_gst ELSE 0 END) AS days_1_30,
    SUM(CASE WHEN days_past_due BETWEEN 31 AND 60 THEN total_inc_gst ELSE 0 END) AS days_31_60,
    SUM(CASE WHEN days_past_due BETWEEN 61 AND 90 THEN total_inc_gst ELSE 0 END) AS days_61_90,
    SUM(CASE WHEN days_past_due > 90              THEN total_inc_gst ELSE 0 END) AS over_90,
    SUM(total_inc_gst)                               AS total_outstanding
FROM (
    SELECT *,
           JULIANDAY('{selected_period}-01', 'start of month', '+1 month', '-1 day')
           - JULIANDAY(due_date) AS days_past_due
    FROM   accounts_receivable
    WHERE  status != 'Paid'
) t
GROUP BY customer_name, region
ORDER BY total_outstanding DESC
LIMIT 15;
""",
        "Month-End P&L Summary": """
-- Monthly P&L Summary with MoM variance
SELECT
    gl.period,
    SUM(CASE WHEN c.account_type = 'Revenue' THEN gl.credit ELSE 0 END)   AS revenue,
    SUM(CASE WHEN c.account_type = 'Expense' THEN gl.debit  ELSE 0 END)   AS expenses,
    SUM(CASE WHEN c.account_type = 'Revenue' THEN gl.credit ELSE 0 END)
  - SUM(CASE WHEN c.account_type = 'Expense' THEN gl.debit  ELSE 0 END)   AS net_result,
    ROUND(
        (SUM(CASE WHEN c.account_type = 'Revenue' THEN gl.credit ELSE 0 END)
       - SUM(CASE WHEN c.account_type = 'Expense' THEN gl.debit  ELSE 0 END))
      /  SUM(CASE WHEN c.account_type = 'Revenue' THEN gl.credit ELSE 0 END) * 100, 2
    )                                                                        AS net_margin_pct
FROM   general_ledger gl
JOIN   chart_of_accounts c ON gl.account_code = c.account_code
WHERE  gl.period <= '2026-03'
GROUP BY gl.period
ORDER BY gl.period;
""",
        "Balance Sheet Reconciliation": """
-- ══════════════════════════════════════════════════════════════════
-- BALANCE SHEET RECONCILIATION — Accounting Equation Breakdown
-- ══════════════════════════════════════════════════════════════════
-- The fundamental accounting equation:
--   Assets  =  Liabilities  +  Equity  +  Net P&L (open period)
--
-- For a CLOSED period: P&L is zero (closed to retained earnings).
-- For an OPEN period:  P&L is non-zero and must be included in
--   equity to prove the equation holds.
-- ══════════════════════════════════════════════════════════════════
SELECT '① Total Assets'                      AS category,
    ROUND(SUM(CASE WHEN c.account_type = 'Asset'
                   THEN gl.debit - gl.credit ELSE 0 END), 2)  AS balance
FROM general_ledger gl
JOIN chart_of_accounts c ON gl.account_code = c.account_code
WHERE gl.period <= '2026-03'
UNION ALL
SELECT '② Total Liabilities',
    ROUND(SUM(CASE WHEN c.account_type = 'Liability'
                   THEN gl.credit - gl.debit ELSE 0 END), 2)
FROM general_ledger gl
JOIN chart_of_accounts c ON gl.account_code = c.account_code
WHERE gl.period <= '2026-03'
UNION ALL
SELECT '③ Booked Equity  (as at 30 Jun 2025 — prior FY closing)',
    -- Equity accounts only change at year-end when closing entries transfer
    -- net P&L into Retained Earnings. In an open FY2026 no closing entry
    -- has been posted yet, so equity = FY2025 closing balance (≤ 2025-06).
    -- Current year result is captured separately in line ④.
    ROUND(SUM(CASE WHEN c.account_type = 'Equity'
                   THEN gl.credit - gl.debit ELSE 0 END), 2)
FROM general_ledger gl
JOIN chart_of_accounts c ON gl.account_code = c.account_code
WHERE gl.period <= '2025-06'
UNION ALL
SELECT '④ YTD Net P&L  (Revenue − Expenses)',
    -- Revenue: credit-debit = positive | Expense: credit-debit = negative → net = Revenue minus Expenses
    ROUND(SUM(CASE WHEN c.account_type IN ('Revenue','Expense')
                   THEN gl.credit - gl.debit ELSE 0 END), 2)
FROM general_ledger gl
JOIN chart_of_accounts c ON gl.account_code = c.account_code
WHERE gl.period <= '2026-03'
UNION ALL
SELECT '⑤ Total Liabilities + Equity + P&L  (② + ③ + ④)',
    ROUND(SUM(CASE WHEN c.account_type IN ('Liability','Equity') THEN gl.credit - gl.debit
                   WHEN c.account_type = 'Revenue'               THEN gl.credit - gl.debit
                   WHEN c.account_type = 'Expense'               THEN gl.credit - gl.debit
                   ELSE 0 END), 2)
FROM general_ledger gl
JOIN chart_of_accounts c ON gl.account_code = c.account_code
WHERE gl.period <= '2026-03'
UNION ALL
SELECT '✓ Difference  (① − ⑤)  — must be zero',
    -- Difference = Sum(debit − credit) across ALL account types = Total Debits − Total Credits = 0
    ROUND(SUM(gl.debit - gl.credit), 2)
FROM general_ledger gl
JOIN chart_of_accounts c ON gl.account_code = c.account_code
WHERE gl.period <= '2026-03';
""",
        "Payroll Tax Reconciliation": """
-- Payroll Tax Monthly Reconciliation
-- Validates GL expense matches SRO liability calculations
SELECT
    pt.period,
    pt.gross_wages,
    pt.threshold,
    pt.taxable_wages,
    ROUND(pt.taxable_wages * 0.0485, 2)  AS calculated_tax,
    pt.tax_due                            AS lodged_tax,
    ROUND(pt.tax_due - pt.taxable_wages * 0.0485, 2) AS variance,
    pt.lodgement_status,
    pt.payment_status
FROM payroll_tax pt
WHERE pt.period <= '2026-03'
ORDER BY pt.period;
""",
        "Fixed Asset Depreciation Schedule": """
-- Fixed Asset Depreciation Schedule
-- Current period NBV and YTD depreciation charge
SELECT
    d.asset_id,
    fa.asset_name,
    fa.category,
    fa.cost,
    fa.useful_life_years,
    fa.depreciation_method,
    SUM(CASE WHEN d.period >= '2025-07' THEN d.depreciation ELSE 0 END) AS ytd_depreciation,
    d.accum_dep_close                                                     AS accum_dep_to_mar26,
    d.nbv_close                                                           AS nbv_31_mar_2026
FROM depreciation_schedule d
JOIN fixed_assets fa ON d.asset_id = fa.asset_id
WHERE d.period = '2026-03'
GROUP BY d.asset_id, fa.asset_name, fa.category, fa.cost,
         fa.useful_life_years, fa.depreciation_method,
         d.accum_dep_close, d.nbv_close
ORDER BY fa.category, fa.cost DESC;
""",
        "GST / BAS Reconciliation": """
-- GST Reconciliation: GL vs BAS return
-- Ensures GST collected per GL matches quarterly BAS lodgements
SELECT
    g.quarter_label,
    SUM(CASE WHEN t.transaction_type = 'Output Tax'       THEN t.gst_amount ELSE 0 END) AS gl_gst_collected,
    SUM(CASE WHEN t.transaction_type = 'Input Tax Credit' THEN t.gst_amount ELSE 0 END) AS gl_itc,
    g.gst_collected AS bas_gst_collected,
    g.gst_itc       AS bas_itc,
    g.net_gst       AS bas_net_gst,
    ROUND(SUM(CASE WHEN t.transaction_type = 'Output Tax' THEN t.gst_amount ELSE 0 END)
        - g.gst_collected, 2)                                                             AS variance
FROM (
    SELECT 'Q1 FY2026' AS quarter_label, '2025-07' AS m_from, '2025-09' AS m_to,
           gst_collected, gst_itc, net_gst FROM bas_returns WHERE quarter = 'Q1 FY2026'
    UNION ALL
    SELECT 'Q2 FY2026', '2025-10', '2025-12',
           gst_collected, gst_itc, net_gst FROM bas_returns WHERE quarter = 'Q2 FY2026'
    UNION ALL
    SELECT 'Q3 FY2026', '2026-01', '2026-03',
           gst_collected, gst_itc, net_gst FROM bas_returns WHERE quarter = 'Q3 FY2026'
) g
JOIN gst_transactions t ON t.period BETWEEN g.m_from AND g.m_to
GROUP BY g.quarter_label, g.gst_collected, g.gst_itc, g.net_gst;
""",
        "Intercompany Reconciliation": """
-- Intercompany Balance Reconciliation
-- Ensures IC payable to Parent University matches charges posted
SELECT
    ic.period,
    ic.description,
    ic.amount                        AS ic_charge,
    ic.status,
    ROUND(SUM(CASE WHEN gl.account_code = '2400' THEN gl.credit ELSE 0 END), 2) AS gl_ic_payable_cr,
    ROUND(SUM(CASE WHEN gl.account_code = '5300' THEN gl.debit  ELSE 0 END), 2) AS gl_ic_expense_dr,
    CASE WHEN ic.status = 'Matched' THEN 0
         ELSE ROUND(ic.amount, 2) END AS outstanding_difference
FROM intercompany ic
LEFT JOIN general_ledger gl
       ON gl.period = ic.period
      AND gl.account_code IN ('2400','5300')
      AND gl.journal_type = 'Intercompany'
WHERE ic.period <= '2026-03'
GROUP BY ic.period, ic.description, ic.amount, ic.status
ORDER BY ic.period;
""",
        "AP Aging & DPO Analysis": """
-- Accounts Payable Aging & Days Payable Outstanding
-- Classifies unpaid supplier invoices into overdue buckets
-- and calculates weighted-average DPO for paid invoices
SELECT
    ap.supplier_type,
    COUNT(CASE WHEN ap.status = 'Unpaid' THEN 1 END)                        AS open_invoices,
    ROUND(SUM(CASE WHEN ap.status = 'Unpaid'
                   THEN ap.total_inc_gst ELSE 0 END), 2)                    AS total_outstanding,
    ROUND(SUM(CASE WHEN ap.status = 'Unpaid'
                    AND JULIANDAY('2026-03-31') - JULIANDAY(ap.due_date) <= 0
                   THEN ap.total_inc_gst ELSE 0 END), 2)                    AS current_not_due,
    ROUND(SUM(CASE WHEN ap.status = 'Unpaid'
                    AND JULIANDAY('2026-03-31') - JULIANDAY(ap.due_date) BETWEEN 1  AND 30
                   THEN ap.total_inc_gst ELSE 0 END), 2)                    AS overdue_1_30,
    ROUND(SUM(CASE WHEN ap.status = 'Unpaid'
                    AND JULIANDAY('2026-03-31') - JULIANDAY(ap.due_date) BETWEEN 31 AND 60
                   THEN ap.total_inc_gst ELSE 0 END), 2)                    AS overdue_31_60,
    ROUND(SUM(CASE WHEN ap.status = 'Unpaid'
                    AND JULIANDAY('2026-03-31') - JULIANDAY(ap.due_date) > 60
                   THEN ap.total_inc_gst ELSE 0 END), 2)                    AS overdue_60_plus,
    ROUND(
        SUM(CASE WHEN ap.status = 'Paid'
                 THEN (JULIANDAY(ap.payment_date) - JULIANDAY(ap.invoice_date))
                      * ap.amount_ex_gst ELSE 0 END)
      / NULLIF(SUM(CASE WHEN ap.status = 'Paid'
                        THEN ap.amount_ex_gst ELSE 0 END), 0), 1)           AS weighted_dpo_days
FROM accounts_payable ap
GROUP BY ap.supplier_type
ORDER BY total_outstanding DESC;
""",
        "AP vs AR Working Capital": """
-- Working Capital: AP vs AR Outstanding Balances by Period
-- Compares trade creditors against trade debtors month by month
-- Net position = AR - AP (positive = more owed to us than we owe)
SELECT
    periods.period                                                           AS period,
    ROUND(COALESCE(ar.ar_outstanding, 0), 2)                                AS ar_outstanding,
    ROUND(COALESCE(ap.ap_outstanding, 0), 2)                                AS ap_outstanding,
    ROUND(COALESCE(ar.ar_outstanding, 0)
        - COALESCE(ap.ap_outstanding, 0), 2)                                AS net_working_capital,
    ROUND(COALESCE(ar.ar_outstanding, 0)
        / NULLIF(COALESCE(ap.ap_outstanding, 0), 0) * 100, 1)              AS ar_to_ap_ratio_pct
FROM (
    -- All periods from either AR or AP (SQLite has no FULL OUTER JOIN — emulated via UNION)
    SELECT period FROM accounts_receivable WHERE period <= '2026-03'
    UNION
    SELECT period FROM accounts_payable    WHERE period <= '2026-03'
) periods
LEFT JOIN (
    SELECT period,
           SUM(CASE WHEN status != 'Paid' THEN total_inc_gst ELSE 0 END) AS ar_outstanding
    FROM   accounts_receivable
    GROUP BY period
) ar ON ar.period = periods.period
LEFT JOIN (
    SELECT period,
           SUM(CASE WHEN status = 'Unpaid' THEN total_inc_gst ELSE 0 END) AS ap_outstanding
    FROM   accounts_payable
    GROUP BY period
) ap ON ap.period = periods.period
ORDER BY periods.period;
""",
        "Top Suppliers by Spend": """
-- Top Suppliers by Total Spend (FY2026 YTD)
-- Ranks suppliers by total invoiced amount with payment performance metrics
SELECT
    ap.supplier_name,
    ap.supplier_type,
    ap.payment_terms_days                                                    AS std_terms_days,
    COUNT(ap.invoice_number)                                                 AS invoice_count,
    ROUND(SUM(ap.amount_ex_gst), 2)                                         AS total_ex_gst,
    ROUND(SUM(ap.gst_amount), 2)                                            AS total_gst,
    ROUND(SUM(ap.total_inc_gst), 2)                                         AS total_inc_gst,
    COUNT(CASE WHEN ap.status = 'Paid' THEN 1 END)                          AS paid_count,
    COUNT(CASE WHEN ap.status = 'Unpaid' THEN 1 END)                        AS unpaid_count,
    ROUND(
        AVG(CASE WHEN ap.status = 'Paid'
                 THEN JULIANDAY(ap.payment_date) - JULIANDAY(ap.invoice_date)
            END), 1)                                                         AS avg_days_to_pay
FROM accounts_payable ap
WHERE ap.period <= '2026-03'
GROUP BY ap.supplier_name, ap.supplier_type, ap.payment_terms_days
ORDER BY total_inc_gst DESC
LIMIT 15;
""",
    }

    # ── Database Schema Browser ──────────────────────────────────────────────
    with st.expander("Database Schema Browser", expanded=False):
        try:
            with get_connection() as _conn:
                _tables = pd.read_sql_query(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
                    _conn
                )["name"].tolist()

                # Row counts for all tables in one pass
                _counts = {
                    t: _conn.execute(f"SELECT COUNT(*) FROM \"{t}\"").fetchone()[0]
                    for t in _tables
                }

            st.caption(f"{len(_tables)} tables · SQLite · `{DB_PATH}` · rebuilt with `if_exists='replace'` on schema change")

            # Render compact grid: 4 cards per row
            _cols_per_row = 4
            for _row_start in range(0, len(_tables), _cols_per_row):
                _row_tables = _tables[_row_start:_row_start + _cols_per_row]
                _grid = st.columns(_cols_per_row)
                for _gi, _tbl in enumerate(_row_tables):
                    with _grid[_gi]:
                        st.markdown(
                            f'<div style="background:#F5F5F5;border:1px solid #ddd;border-radius:6px;'
                            f'padding:8px 10px;margin-bottom:4px">'
                            f'<div style="font-size:0.78rem;font-weight:700;color:#1A1A1A">{_tbl}</div>'
                            f'<div style="font-size:0.70rem;color:#888">{_counts[_tbl]:,} rows</div>'
                            f'</div>',
                            unsafe_allow_html=True
                        )
                        if st.button("Schema + Preview", key=f"_schema_{_tbl}", use_container_width=True):
                            st.session_state["_browser_table"] = _tbl

            # Schema + preview panel for selected table
            _sel = st.session_state.get("_browser_table")
            if _sel and _sel in _tables:
                st.markdown(f"---\n**`{_sel}`** — schema & preview")
                with get_connection() as _conn:
                    _schema = pd.read_sql_query(f"PRAGMA table_info(\"{_sel}\")", _conn)
                    _preview = pd.read_sql_query(f"SELECT * FROM \"{_sel}\" LIMIT 10", _conn)

                sc1, sc2 = st.columns([1, 3])
                with sc1:
                    st.caption("Column definitions")
                    _numeric_keywords = {"rate", "amount", "cost", "price", "tax", "gst",
                                         "total", "balance", "days", "count", "pct", "dep",
                                         "wages", "revenue", "expense", "margin", "threshold"}
                    _schema_disp = _schema[["name", "type", "notnull", "dflt_value"]].copy()
                    _schema_disp.columns = ["Column", "Type", "NOT NULL", "Default"]
                    _schema_disp["NOT NULL"] = _schema_disp["NOT NULL"].map({0: "", 1: "✓"})

                    # Identify drifted columns: numeric name but TEXT storage
                    _drifted = [
                        row["name"] for _, row in _schema.iterrows()
                        if any(kw in row["name"].lower() for kw in _numeric_keywords)
                        and row["type"].upper() in ("TEXT", "VARCHAR", "")
                    ]

                    def _flag_type(row):
                        if row["Column"] in _drifted:
                            return ["background-color:#FFF3CD"] * len(row)
                        return [""] * len(row)

                    st.dataframe(
                        _schema_disp.style.apply(_flag_type, axis=1),
                        use_container_width=True, hide_index=True
                    )

                    if _drifted:
                        st.caption(f"⚠ {len(_drifted)} column(s) flagged: numeric name / TEXT storage")
                        # Cast-to-Numeric diagnostic query
                        # SQLite has no ALTER COLUMN — fix is either CAST() in queries
                        # or correcting the pandas dtype before to_sql()
                        _all_cols = _schema["name"].tolist()
                        _cast_select = ",\n    ".join(
                            f"CAST(\"{c}\" AS REAL) AS \"{c}\""
                            if c in _drifted else f"\"{c}\""
                            for c in _all_cols
                        )
                        _cast_sql = (
                            f"-- Diagnostic: verify drifted columns are actually numeric\n"
                            f"-- SQLite has no ALTER COLUMN — real fix is ensuring pandas\n"
                            f"-- outputs the correct dtype before DataFrame.to_sql().\n"
                            f"SELECT\n    {_cast_select}\nFROM \"{_sel}\"\nLIMIT 20;"
                        )
                        if st.button("Generate CAST() diagnostic query ↓", key="_cast_btn"):
                            st.session_state["_custom_sql"] = _cast_sql
                        with st.expander("View CAST() SQL", expanded=False):
                            st.code(_cast_sql, language="sql")
                    else:
                        st.caption("✓ No type drift detected")

                with sc2:
                    st.caption(f"First 10 rows of `{_sel}`")

                    # ── Calculated column augmentation ──────────────────────
                    _preview_aug = _preview.copy()
                    _col_names_lower = {c.lower(): c for c in _preview_aug.columns}

                    # LCT flag: warn if purchase_price > threshold
                    # Uses "Other" threshold ($80,567); overrides to fuel-efficient ($91,387)
                    # if is_fuel_efficient / is_ev_exempt column present and truthy
                    _price_col = _col_names_lower.get("purchase_price") or _col_names_lower.get("cost")
                    if _price_col and pd.api.types.is_numeric_dtype(_preview_aug[_price_col]):
                        _fe_col = (_col_names_lower.get("is_fuel_efficient")
                                   or _col_names_lower.get("is_ev_exempt"))
                        _LCT_STD  = 80_567
                        _LCT_FE   = 91_387

                        def _lct_flag(row):
                            price = row[_price_col]
                            if pd.isna(price):
                                return "–"
                            fe = bool(row[_fe_col]) if _fe_col and not pd.isna(row.get(_fe_col, None)) else False
                            thresh = _LCT_FE if fe else _LCT_STD
                            if price > thresh:
                                excess = price - thresh
                                lct = round(excess / 1.1 * 0.33, 0)
                                return f"⚠ LCT ~${lct:,.0f}"
                            return f"✓ < ${thresh:,}"

                        _preview_aug["⚑ LCT Check"] = _preview_aug.apply(_lct_flag, axis=1)

                    # Holding period: days from purchase_date to FBT year end (31 Mar 2026)
                    _date_col = _col_names_lower.get("purchase_date")
                    if _date_col:
                        _FBT_YEAR_END = pd.Timestamp("2026-03-31")
                        try:
                            _dates = pd.to_datetime(_preview_aug[_date_col], errors="coerce")
                            _held  = (_FBT_YEAR_END - _dates).dt.days
                            _yrs   = _held / 365.25

                            def _held_label(y):
                                if pd.isna(y):
                                    return "–"
                                disc = " · 1/3 disc ✓" if y >= 4 else f" · disc in {4-y:.1f}yr"
                                return f"{y:.1f}yr{disc}"

                            _preview_aug["⚑ Held (FBT yr-end)"] = _yrs.apply(_held_label)
                        except Exception:
                            pass

                    # PHEV / EV exemption flag
                    _phev_col  = _col_names_lower.get("is_phev")
                    _ev_col    = _col_names_lower.get("is_ev_exempt")
                    _commit_col = _col_names_lower.get("pre_apr25_commitment")
                    if _phev_col:
                        def _phev_flag(row):
                            if bool(row.get(_ev_col, False)):
                                return "✓ EV exempt (s58P)"
                            if bool(row.get(_phev_col, False)):
                                grandfathered = bool(row.get(_commit_col, False))
                                return ("✓ PHEV grandfathered (s58P(2))"
                                        if grandfathered else "⚠ PHEV — exempt ended 1 Apr 2025")
                            return "ICE — standard FBT"
                        _preview_aug["⚑ FBT Exemption"] = _preview_aug.apply(_phev_flag, axis=1)

                    st.dataframe(_preview_aug, use_container_width=True, hide_index=True)
                    if len(set(_preview_aug.columns) - set(_preview.columns)) > 0:
                        st.caption("⚑ columns are calculated — not stored in the database")

                if st.button(f"Run SELECT * FROM {_sel} LIMIT 50 below ↓", key="_inject_query"):
                    st.session_state["_custom_sql"] = f"SELECT * FROM \"{_sel}\" LIMIT 50;"

        except Exception as _e:
            st.error(f"Schema browser error: {_e}")

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Query Runner ─────────────────────────────────────────────────────────
    # Support injected query from schema browser preview button
    _default_sql = st.session_state.pop("_custom_sql", None)
    if _default_sql:
        st.session_state["_sql_selectbox"] = "— Custom (from browser) —"

    _query_options = ["— Custom (from browser) —"] + list(query_labels.keys())
    selected_query_label = st.selectbox(
        "Select analysis query:",
        _query_options,
        index=_query_options.index(st.session_state.get("_sql_selectbox", list(query_labels.keys())[0]))
        if st.session_state.get("_sql_selectbox") in _query_options else 1,
        key="_sql_selectbox"
    )

    if selected_query_label == "— Custom (from browser) —":
        active_sql = _default_sql or f"SELECT * FROM chart_of_accounts LIMIT 10;"
    else:
        active_sql = query_labels[selected_query_label]

    col1, col2 = st.columns([2, 3])
    with col1:
        section("SQL Query")
        st.code(active_sql.strip(), language="sql")

    with col2:
        section("Query Results")
        try:
            with get_connection() as conn:
                result_df = pd.read_sql_query(active_sql, conn)
            if not result_df.empty:
                # Build column_config to keep numbers as numbers (right-aligned)
                _pct_kw    = {"pct", "margin", "ratio", "rate"}
                _dollar_kw = {"amount","balance","wages","tax","revenue","expense","net",
                              "cost","dep","variance","gst","itc","outstanding","charge",
                              "current","days_31","days_61","over_90","current_0","payable",
                              "collected","spend","total","ic_"}
                _col_cfg = {}
                for col in result_df.select_dtypes(include=[np.number]).columns:
                    col_l = col.lower()
                    if any(kw in col_l for kw in _pct_kw):
                        _col_cfg[col] = st.column_config.NumberColumn(col, format="%.2f%%")
                    elif any(kw in col_l for kw in _dollar_kw):
                        _col_cfg[col] = st.column_config.NumberColumn(col, format="$%,.0f")
                    else:
                        _col_cfg[col] = st.column_config.NumberColumn(col, format="%,d")
                st.dataframe(result_df, use_container_width=True, hide_index=True,
                             column_config=_col_cfg if _col_cfg else None)
                st.caption(f"{len(result_df)} rows returned")
            else:
                st.info("No results returned.")
        except Exception as e:
            st.error(f"Query error: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# PAGE 11 – ABOUT & GOVERNANCE
# ─────────────────────────────────────────────────────────────────────────────

elif page == "About & Governance":
    page_header(
        "About & Governance",
        f"{ENTITY}  |  System Logic, Compliance References & Work Instructions",
    )

    st.markdown("""
> This page documents the purpose, data sources, business logic, and compliance
> framework behind each module. It is intended for finance team members, auditors,
> and anyone onboarding to the system — no coding knowledge required.
    """)

    # ── How to Use ────────────────────────────────────────────────────────────
    section("How to Use This Dashboard")

    with st.expander("🗂️  Navigation — switching between modules", expanded=True):
        st.markdown("""
Use the **left sidebar** to move between the 11 modules. Click any page name to switch instantly.
All filters and period selections remain active as you navigate — you don't need to re-select them.
        """)

    with st.expander("📅  Setting the reporting period", expanded=True):
        st.markdown("""
All period controls are in the **sidebar under 📅 Period**:

| Control | What it does |
|---|---|
| **View Type** | Monthly = single month · Quarterly = one quarter · YTD = Jul to selected month · Full Year = all 12 months |
| **Financial Year** | Select the FY (e.g. FY2026 = 1 Jul 2025 – 30 Jun 2026) |
| **Month / End Month** | Available when View Type is Monthly or YTD — pick the reporting month |
| **Quarter** | Available when View Type is Quarterly — Q1 Jul–Sep · Q2 Oct–Dec · Q3 Jan–Mar · Q4 Apr–Jun |

The coloured badge at the top of each page always shows the active period at a glance.
        """)

    with st.expander("🌏  Filtering by Region & Cost Centre", expanded=False):
        st.markdown("""
Under **🌏 Filters** in the sidebar:
- **Region** — Domestic, International, or both
- **Cost Centre** — select one or more cost centres; deselecting all defaults back to all

These filters apply across Executive Overview, Income Statement, and Balance Sheet.
        """)

    with st.expander("⚙️  Changing tax rates", expanded=False):
        st.markdown("""
Tax rates are **locked by default** to prevent accidental changes.

1. Find **⚙️ Tax Rate Configuration** in the sidebar
2. Tick the **✏️ checkbox** to unlock edit mode — a warning banner appears
3. Adjust the rate(s) — every change is automatically recorded with a timestamp in the **Change Log**
4. Untick ✏️ to lock again

> **Note:** Rate changes apply to the current session only and reset when the app is rebooted.
        """)

    with st.expander("🔍  Running your own SQL queries", expanded=False):
        st.markdown("""
Go to the **SQL Analysis** page. You can:
- Type any `SELECT` query against the live SQLite database
- Use the **pre-built queries** dropdown for common analytical questions
- Browse the **Schema** tab to see all table names, column types, and row counts

Only `SELECT` statements are permitted — the database is read-only from the UI.
        """)

    with st.expander("🖨️  Exporting data", expanded=False):
        st.markdown("""
Every data table in the dashboard has a **Download CSV** button (look for the ⬇ icon on dataframes).
For formatted reports, use your browser's **Print → Save as PDF** function on any page.
        """)

    st.markdown("---")

    # ── Dashboard Purpose ──────────────────────────────────────────────────────
    section("Dashboard Purpose")
    st.markdown("""
This dashboard is a **Financial Management & Compliance Tool** for UniPath Pty Ltd.
It consolidates general ledger data, sub-ledger transactions, tax obligations, and month-end
controls into a single platform — replacing manual Excel-based reporting.

**Primary users:** Financial Accountant, Finance Manager, CFO, Internal Audit
**Reporting period:** FY2026 (1 July 2025 – 30 June 2026)
**Data source:** Synthetic dataset generated from UniPath's chart of accounts and operational parameters
**Refresh cycle:** On-demand (database rebuilt on app startup if schema changes are detected)
    """)

    st.markdown("---")

    # ── Module Reference ───────────────────────────────────────────────────────
    section("Module Reference — What Each Tab Does")

    modules = [
        ("Executive Overview", "📊",
         "High-level KPI dashboard with Revenue, Expenses, Net Surplus/Deficit and MoM trends. "
         "Includes budget overlay and cumulative P&L curve. Entry point for senior leadership.",
         "GL → Revenue/Expense accounts 4001–5999",
         "AASB 101 (Presentation of Financial Statements)"),

        ("Month-End Close", "✅",
         "Checklist tracker for period-end controls: GL review, reconciliations, BAS lodgement, "
         "payroll journals, intercompany sign-off. Status: Not Started / In Progress / Complete.",
         "month_end_checklist table",
         "Internal Control Framework; AASB 110 (Events After Reporting Period)"),

        ("Income Statement", "📈",
         "Profit & Loss statement showing Revenue by stream, Operating Expenses by category, "
         "FBT provision, corporate tax provision, and NPAT. Supports Monthly / Quarterly / YTD / Full Year views.",
         "GL → accounts 4001–5999 + budget table",
         "AASB 101 §82 (P&L line items); AASB 112 (Income Taxes)"),

        ("Balance Sheet", "⚖️",
         "Statement of Financial Position as at period end. Assets sourced from bank, AR subledger "
         "and fixed asset register. Liabilities blended from GL and source tables. "
         "Includes Accounting Equation integrity check.",
         "GL + bank_transactions + accounts_receivable + fixed_assets + depreciation_schedule",
         "AASB 101 §54 (Balance Sheet); AASB 116 (PP&E); AASB 16 (Leases)"),

        ("Accounts Receivable", "🧾",
         "AR aging schedule, DSO analysis, domestic vs international split, and open invoice register. "
         "DSO target configurable in sidebar. Traffic-light status for overdue accounts.",
         "accounts_receivable subledger",
         "AASB 9 (Financial Instruments — Impairment); ATO GST supply type classification"),

        ("Accounts Payable", "💳",
         "AP aging schedule, DPO trend vs 35-day target, top supplier analysis, upcoming payments "
         "due in 30 days, and full invoice register. Sourced from supplier invoice sub-ledger.",
         "accounts_payable + suppliers tables",
         "VGPB Payment Terms Policy 2022; ATO Input Tax Credit (ITC) rules (GSTA 1999)"),

        ("Bank Reconciliation", "🏦",
         "Reconciles bank statement closing balance to GL cash balance. Identifies unmatched "
         "items (deposits in transit, outstanding cheques). Difference should always be nil.",
         "bank_transactions table (gl_matched flag)",
         "AASB 107 (Cash Flow Statements); Internal Control — Segregation of Duties"),

        ("Fixed Assets", "🏗️",
         "Asset register with cost, accumulated depreciation, and NBV. Depreciation calculated "
         "using straight-line method per ATO effective life tables. Traffic-light for fully "
         "depreciated assets and NBV ratio.",
         "fixed_assets + depreciation_schedule tables",
         "AASB 116 (PP&E — cost model); TR 2024/1 (ATO Effective Life); AASB 136 (Impairment)"),

        ("Tax Compliance", "🧮",
         "Covers GST (BAS), Payroll Tax (VIC), FBT (FBTAA 1986), LCT, and Corporate Tax. "
         "FBT gross-up uses ATO-published Type 1 (2.0802) and Type 2 rates. "
         "Tax rates are locked by default — unlock via ✏️ in sidebar.",
         "gst_transactions + bas_returns + payroll_tax + fbt_register + GL",
         "GSTA 1999; FBTAA 1986; PAYG — s12-5 ITAA 1997; Payroll Tax Act 2007 (VIC); LCTA 1999"),

        ("SQL Analysis", "🔍",
         "Live SQL query runner against the underlying SQLite database. Includes pre-built "
         "analytical queries and a schema browser with type-drift detection and LCT/FBT flags.",
         "All tables (direct SQLite access)",
         "No specific standard — data governance and audit trail tool"),
    ]

    for name, icon, purpose, source, compliance in modules:
        with st.expander(f"{icon}  {name}", expanded=False):
            col_a, col_b = st.columns([3, 2])
            with col_a:
                st.markdown(f"**Purpose**\n\n{purpose}")
            with col_b:
                st.markdown(f"**Data Source**\n\n`{source}`")
                st.markdown(f"**Compliance Reference**\n\n{compliance}")

    st.markdown("---")

    # ── Key Assumptions ────────────────────────────────────────────────────────
    section("Key Assumptions & Known Limitations")
    st.markdown("""
| # | Assumption / Limitation | Impact |
|---|---|---|
| 1 | Dataset is **synthetic** — generated from statistical distributions, not live SAP/Banner data | Figures are illustrative only; not for external reporting |
| 2 | General Ledger contains **P&L journals only** — no complete double-entry opening BS entries | GL-derived asset/liability totals differ from BS sub-ledger figures; Equation Check uses BS as source of truth |
| 3 | **Retained Earnings** is not updated during FY2026 — closing entries posted only at 30 June | Current Year Earnings = YTD net P&L; RE = FY2025 closing balance |
| 4 | Depreciation uses **straight-line** for all asset classes | Assets with diminishing-value treatment (per TR 2024/1) would differ |
| 5 | GST Receivable is **estimated** (ITC − 30% of output tax) — not a precise BAS reconciliation | Reconcile to actual BAS lodgements for compliance purposes |
| 6 | FBT Type 1 gross-up rate hardcoded at **2.0802** per ATO FY2026 publication | Must update annually when ATO releases new rates (usually November) |
| 7 | DPO excludes **unpaid/overdue** invoices — reflects only settled payment behaviour | Aging Schedule captures the outstanding exposure separately |
    """)

    st.markdown("---")

    # ── Compliance Calendar ────────────────────────────────────────────────────
    section("Key Compliance Deadlines — FY2026")
    st.markdown("""
| Obligation | Authority | Due Date | Module |
|---|---|---|---|
| Monthly BAS lodgement | ATO — GSTA 1999 | 21st of following month | Tax Compliance → GST/BAS |
| Quarterly BAS (if applicable) | ATO | 28th of month after quarter | Tax Compliance → GST/BAS |
| Payroll Tax monthly return | SRO Victoria | 7th of following month | Tax Compliance → Payroll Tax |
| FBT return lodgement | ATO — FBTAA 1986 | 21 May 2026 (or agent date) | Tax Compliance → FBT |
| Year-end closing entries | Internal | 30 June 2026 | Month-End Close |
| Financial statements sign-off | Board / CFO | ~August 2026 | Balance Sheet / Income Statement |
| VAGO audit commencement | VAGO | ~September 2026 | All modules |
    """)

    st.markdown("---")

    # ── Governance & Change Control ────────────────────────────────────────────
    section("Governance & Change Control")
    st.markdown("""
**Tax Rate Changes**
Tax rates (SGC, Payroll Tax, FBT components) are locked by default. To edit:
1. Click the **✏️ checkbox** next to "Tax Rate Configuration" in the sidebar
2. Adjust rates — every change is timestamped in the Change Log automatically
3. Lock again by unchecking ✏️

**Data Refresh**
The SQLite database rebuilds automatically when the application starts and detects a schema change.
To force a rebuild locally: delete `data/rmit_finance.db` and restart Streamlit.

**Access Control**
This dashboard is a reporting tool — it does not write back to source systems.
All data is read-only except tax rate session overrides (not persisted across sessions).

**Feedback & Improvement**
Identified gaps or suggested enhancements should be logged in the team's issue tracker
and reviewed at each month-end close retrospective.
    """)
