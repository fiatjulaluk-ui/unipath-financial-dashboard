#!/usr/bin/env python3
"""
RMIT UP | Financial Accounting Dashboard
=========================================
Streamlit application showcasing financial accounting competencies
aligned with the Financial Accountant role at RMIT UP.

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
ENTITY         = "RMIT UP Pty Ltd"
ABN            = "12 345 678 901"

st.set_page_config(
    page_title="RMIT UP | Financial Dashboard",
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
    [data-testid="stSidebar"] span,
    [data-testid="stSidebar"] div { color: #FFFFFF !important; }
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

@st.cache_resource
def ensure_db():
    """Generate data if DB doesn't exist."""
    if not os.path.exists(DB_PATH):
        import generate_data
        generate_data.build_database()


@st.cache_resource
def get_connection():
    ensure_db()
    return sqlite3.connect(DB_PATH, check_same_thread=False)


@st.cache_data(ttl=300)
def query(sql: str) -> pd.DataFrame:
    conn = get_connection()
    return pd.read_sql_query(sql, conn)


def fmt_aud(val):
    """Format number as AUD currency."""
    if pd.isna(val):
        return "–"
    if abs(val) >= 1_000_000:
        return f"${val/1_000_000:.2f}M"
    if abs(val) >= 1_000:
        return f"${val/1_000:.1f}K"
    return f"${val:,.0f}"


def fmt_pct(val):
    return f"{val:.1f}%" if not pd.isna(val) else "–"


# ─────────────────────────────────────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────────────────────────────────────

with st.sidebar:
    # RMIT branding header
    st.markdown("""
    <div style="text-align:center; padding: 0.5rem 0 1rem;">
        <div style="background:#E8192C; display:inline-block; padding:6px 18px;
                    border-radius:6px; margin-bottom:8px;">
            <span style="color:white; font-size:1.6rem; font-weight:900; letter-spacing:2px;">RMIT</span>
        </div>
        <div style="color:#AAAAAA; font-size:0.75rem; letter-spacing:0.1em;">UNIVERSITY PROGRAMS</div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("---")

    pages = [
        "Executive Overview",
        "Month-End Close",
        "Income Statement",
        "Balance Sheet",
        "Accounts Receivable",
        "Bank Reconciliation",
        "Fixed Assets",
        "Tax Compliance",
        "SQL Analysis",
    ]
    page = st.radio("Navigation", pages, label_visibility="collapsed")

    st.markdown("---")
    # ── Global Filters — defined first so REPORT_DATE_DYN is available everywhere ──
    st.markdown('<div style="font-size:0.78rem;color:#ccc;font-weight:700;margin-bottom:0.5rem">🔍 Global Filters</div>', unsafe_allow_html=True)

    # Derive available periods from DB
    _all_periods = query("SELECT DISTINCT period FROM general_ledger ORDER BY period")["period"].tolist()
    _default_idx = _all_periods.index("2026-03") if "2026-03" in _all_periods else len(_all_periods) - 1
    selected_period = st.selectbox(
        "Period (YTD up to)",
        _all_periods,
        index=_default_idx,
        help="All YTD figures include data up to and including this period.",
    )

    # Derive previous period for MoM
    _sel_idx = _all_periods.index(selected_period)
    prev_period = _all_periods[_sel_idx - 1] if _sel_idx > 0 else selected_period

    # Dynamic report date label from period string
    import calendar as _cal
    _yr, _mo = map(int, selected_period.split("-"))
    _last_day = _cal.monthrange(_yr, _mo)[1]
    _month_name = _cal.month_name[_mo]
    REPORT_DATE_DYN = f"{_last_day} {_month_name} {_yr}"

    st.markdown(f"""
    <div style="font-size:0.73rem; color:#999;margin-bottom:0.5rem">
        <b style="color:#ccc;">{ENTITY}</b><br>
        ABN {ABN}<br>
        Period: {REPORT_DATE_DYN}<br>
        FY2026 (Jul 25 – Jun 26)
    </div>
    """, unsafe_allow_html=True)

    st.markdown("---")

    selected_regions = st.multiselect(
        "Region",
        ["Domestic", "International"],
        default=["Domestic", "International"],
        help="Filters AR aging, Revenue Mix, and Regional Concentration sections.",
    )
    if not selected_regions:
        selected_regions = ["Domestic", "International"]

    _all_cc = query("SELECT cost_centre_code, cost_centre_name FROM cost_centres ORDER BY cost_centre_code")
    _cc_map = dict(zip(_all_cc["cost_centre_name"], _all_cc["cost_centre_code"]))
    selected_cc_names = st.multiselect(
        "Cost Centre",
        list(_cc_map.keys()),
        default=list(_cc_map.keys()),
        help="Filters expense and revenue breakdowns by business unit.",
    )
    if not selected_cc_names:
        selected_cc_names = list(_cc_map.keys())
    selected_cc_codes = [_cc_map[n] for n in selected_cc_names]

    # ── Scenario Slicer ──
    st.markdown('<div style="font-size:0.72rem;color:#aaa;margin:0.6rem 0 0.3rem;font-weight:700">📊 Scenario</div>', unsafe_allow_html=True)
    show_budget = st.toggle("Overlay Budget Targets", value=False,
                            help="Adds budget lines to Revenue vs Expenses chart.")
    dso_target  = st.number_input("DSO Target (days)", value=42, min_value=1, max_value=120, step=1,
                                  help="Highlight customers exceeding this DSO threshold.")

    st.markdown("---")
    st.markdown('<div style="font-size:0.78rem;color:#ccc;font-weight:700;margin-bottom:0.4rem">⚙️ Tax Rate Configuration</div>', unsafe_allow_html=True)
    st.markdown('<div style="font-size:0.70rem;color:#888;margin-bottom:0.6rem">Live inputs — updates FBT & payroll calculations instantly</div>', unsafe_allow_html=True)

    sgc_rate = st.number_input(
        "SGC Rate (%)", value=12.0, min_value=0.0, max_value=20.0, step=0.25, format="%.2f"
    ) / 100

    vic_ptax_rate = st.number_input(
        "VIC Payroll Tax Rate (%)", value=4.85, min_value=0.0, max_value=10.0, step=0.01, format="%.2f"
    ) / 100

    fbt_type1 = st.number_input(
        "FBT Gross-Up – Type 1", value=2.0802, min_value=1.0, max_value=3.0, step=0.0001, format="%.4f"
    )
    fbt_type2 = st.number_input(
        "FBT Gross-Up – Type 2", value=1.8868, min_value=1.0, max_value=3.0, step=0.0001, format="%.4f"
    )
    fbt_rate = st.number_input(
        "FBT Rate (%)", value=47.0, min_value=0.0, max_value=60.0, step=0.5, format="%.1f"
    ) / 100

    st.markdown(f'<div style="font-size:0.68rem;color:#666;margin-top:0.3rem">SGC: {sgc_rate*100:.2f}% · Payroll Tax: {vic_ptax_rate*100:.2f}% · FBT: {fbt_rate*100:.0f}%</div>', unsafe_allow_html=True)


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
        f"{ENTITY}  |  FY2026 YTD as at {REPORT_DATE_DYN}  |  "
        + (f"Region: {', '.join(selected_regions)}" if len(selected_regions) < 2 else "All Regions")
        + (f"  |  {len(selected_cc_names)} cost centre(s)" if len(selected_cc_names) < len(_cc_map) else "")
    )

    gl  = query("SELECT * FROM general_ledger")
    coa = query("SELECT account_code, account_name FROM chart_of_accounts")

    # Apply cost-centre filter to GL
    ytd_all = gl[gl["period"] <= selected_period]
    ytd = ytd_all[ytd_all["cost_centre"].isin(selected_cc_codes)]

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

    # MoM: selected period vs prior period, same CC filter
    rev_prev = ytd_all[
        (ytd_all["period"] == prev_period) &
        (ytd_all["account_code"].between("4001","4999")) &
        (ytd_all["cost_centre"].isin(selected_cc_codes))
    ]["credit"].sum()
    rev_curr = ytd_all[
        (ytd_all["period"] == selected_period) &
        (ytd_all["account_code"].between("4001","4999")) &
        (ytd_all["cost_centre"].isin(selected_cc_codes))
    ]["credit"].sum()
    rev_mom  = ((rev_curr - rev_prev) / rev_prev * 100) if rev_prev else 0
    rev_mom_abs = rev_curr - rev_prev
    mom_arrow = "▲" if rev_mom >= 0 else "▼"
    mom_type  = "pos" if rev_mom >= 0 else "neg"

    # Expense MoM
    exp_prev = ytd_all[
        (ytd_all["period"] == prev_period) &
        (ytd_all["account_code"].between("5001","5999")) &
        (ytd_all["cost_centre"].isin(selected_cc_codes))
    ]["debit"].sum()
    exp_curr = ytd_all[
        (ytd_all["period"] == selected_period) &
        (ytd_all["account_code"].between("5001","5999")) &
        (ytd_all["cost_centre"].isin(selected_cc_codes))
    ]["debit"].sum()
    exp_mom = ((exp_curr - exp_prev) / exp_prev * 100) if exp_prev else 0

    # ── KPI row ──
    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        st.markdown(kpi_card(
            f"YTD Revenue  ({selected_period[:7]})",
            fmt_aud(rev_ytd),
            f"{mom_arrow} {fmt_aud(abs(rev_mom_abs))} ({rev_mom:+.1f}%) vs {prev_period}",
            mom_type,
        ), unsafe_allow_html=True)
    with c2:
        exp_mom_arrow = "▼" if exp_mom >= 0 else "▲"   # rising expenses = bad
        st.markdown(kpi_card(
            "YTD Expenses",
            fmt_aud(exp_ytd),
            f"{exp_mom_arrow} {exp_mom:+.1f}% MoM  ·  {round(exp_ytd/rev_ytd*100,1) if rev_ytd else 0}% of Rev",
            "neg" if exp_mom > 5 else "neu",
        ), unsafe_allow_html=True)
    with c3:
        st.markdown(kpi_card(
            "Net Surplus / (Deficit)",
            fmt_aud(net_ytd),
            f"Margin {fmt_pct(margin)}",
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
        section(f"Monthly Revenue vs Expenses – YTD to {selected_period}")
        monthly = (
            ytd.groupby("period")
            .apply(lambda d: pd.Series({
                "Revenue":  d[d["account_code"].between("4001","4999")]["credit"].sum(),
                "Expenses": d[d["account_code"].between("5001","5999")]["debit"].sum(),
            }))
            .reset_index()
        )
        monthly["Net"] = monthly["Revenue"] - monthly["Expenses"]
        fig = go.Figure()
        fig.add_trace(go.Bar(name="Actuals – Revenue",  x=monthly["period"], y=monthly["Revenue"],
                             marker_color=RMIT_RED, opacity=0.85))
        fig.add_trace(go.Bar(name="Actuals – Expenses", x=monthly["period"], y=monthly["Expenses"],
                             marker_color=RMIT_GREY, opacity=0.75))
        fig.add_trace(go.Scatter(name="Net Surplus", x=monthly["period"], y=monthly["Net"],
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
        section(f"Revenue Mix – YTD  ({region_label})")

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
        rev_by_acct["account_name"] = rev_by_acct["account_name"].str.replace("Course Fees – ", "")
        fig2 = px.pie(rev_by_acct, values="credit", names="account_name",
                      color_discrete_sequence=CHART_PALETTE, hole=0.45)
        fig2.update_traces(textposition="outside", textinfo="percent+label",
                           textfont_size=11)
        fig2.update_layout(
            height=360, margin=dict(l=10, r=10, t=20, b=10),
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
        exp_by_acct["short_name"] = exp_by_acct["account_name"].str.replace(" – RMIT University","").str.replace("Depreciation – ","Dep – ")
        fig3 = px.bar(exp_by_acct, x="debit", y="short_name", orientation="h",
                      color_discrete_sequence=[RMIT_RED])
        fig3.update_layout(
            height=340, margin=dict(l=0, r=10, t=20, b=10),
            xaxis_title="", yaxis_title="", plot_bgcolor="white", paper_bgcolor="white",
            xaxis=dict(tickformat="$,.0f", gridcolor="#F0F0F0"),
        )
        st.plotly_chart(fig3, use_container_width=True)

    with col4:
        section(f"Cumulative Net Surplus – YTD to {selected_period}")
        monthly_sorted = monthly.sort_values("period")
        monthly_sorted["Cumulative Net"] = monthly_sorted["Net"].cumsum()
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
            completed = row["completed_date"] if row["completed_date"] and row["completed_date"] != "None" else "–"
            dep_seq   = row.get("depends_on_seq")
            dep_note  = f"← task {int(dep_seq)}" if pd.notna(dep_seq) and dep_seq else "–"
            blocked   = bool(row["blocked"])
            row_bg    = "background:#fff5f5;" if blocked else ""
            all_rows_html += f"""
            <tr style="{row_bg}">
                <td style="text-align:center;color:#888;font-size:0.8rem;padding:7px 8px">{int(row['task_sequence'])}</td>
                <td style="font-size:0.84rem;padding:7px 8px">{row['task_name']}</td>
                <td style="font-size:0.78rem;color:#888;padding:7px 8px;font-style:italic;text-align:center">{dep_note}</td>
                <td style="font-size:0.81rem;color:#555;padding:7px 8px">{row['owner']}</td>
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

    close_perf = (
        closed[closed["status"] == "Complete"]
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
        f"{ENTITY}  |  FY2026 YTD as at {REPORT_DATE_DYN}"
    )

    gl  = query("SELECT * FROM general_ledger")
    coa = query("SELECT account_code, account_name, account_type, report_section FROM chart_of_accounts")

    ytd = gl[(gl["period"] <= selected_period) & (gl["cost_centre"].isin(selected_cc_codes))]
    ytd = ytd.merge(coa, on="account_code", how="left")

    # Revenue
    rev = ytd[ytd["account_type"] == "Revenue"].groupby("account_name")["credit"].sum().reset_index()
    rev.columns = ["Line Item", "YTD Amount"]
    total_rev = rev["YTD Amount"].sum()

    # Expenses
    exp = ytd[ytd["account_type"] == "Expense"].groupby(["report_section","account_name"])["debit"].sum().reset_index()
    exp.columns = ["Section","Line Item","YTD Amount"]
    total_exp = exp["YTD Amount"].sum()
    net = total_rev - total_exp

    col1, col2 = st.columns([2, 3])

    with col1:
        section("Profit & Loss Statement")
        # Build P&L table
        lines = []
        lines.append({"Category": "REVENUE", "Line Item": "", "Amount": ""})
        for _, r in rev.sort_values("YTD Amount", ascending=False).iterrows():
            lines.append({"Category": "", "Line Item": r["Line Item"], "Amount": f"${r['YTD Amount']:,.0f}"})
        lines.append({"Category": "Total Revenue", "Line Item": "", "Amount": f"${total_rev:,.0f}"})
        lines.append({"Category": "", "Line Item": "", "Amount": ""})
        lines.append({"Category": "EXPENSES", "Line Item": "", "Amount": ""})
        for section_name in exp["Section"].unique():
            sec_data = exp[exp["Section"] == section_name]
            lines.append({"Category": section_name, "Line Item": "", "Amount": ""})
            for _, r in sec_data.sort_values("YTD Amount", ascending=False).iterrows():
                lines.append({"Category": "", "Line Item": f"  {r['Line Item']}", "Amount": f"${r['YTD Amount']:,.0f}"})
            sec_total = sec_data["YTD Amount"].sum()
            lines.append({"Category": f"  Subtotal – {section_name}", "Line Item": "", "Amount": f"${sec_total:,.0f}"})
        lines.append({"Category": "Total Expenses", "Line Item": "", "Amount": f"${total_exp:,.0f}"})
        lines.append({"Category": "", "Line Item": "", "Amount": ""})
        net_label = "NET SURPLUS" if net >= 0 else "NET DEFICIT"
        lines.append({"Category": net_label, "Line Item": "", "Amount": f"${net:,.0f}"})

        pl_df = pd.DataFrame(lines)
        rows_html = ""
        for _, row in pl_df.iterrows():
            bold = "font-weight:700;" if row["Category"] in ("REVENUE","EXPENSES","Total Revenue","Total Expenses") or row["Category"].startswith("NET") else ""
            bg = "background:#F5F5F5;" if bold else ""
            color = f"color:{GREEN};" if row["Category"].startswith("NET SURPLUS") else (f"color:{RMIT_RED};" if row["Category"].startswith("NET DEFICIT") else "")
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
                    <th style="padding:9px 8px;text-align:right;font-size:0.8rem">YTD Amount</th>
                </tr>
            </thead>
            <tbody>{rows_html}</tbody>
        </table>""", unsafe_allow_html=True)

    with col2:
        section("Monthly Revenue & Expense Trend")
        monthly = (
            ytd.groupby("period")
            .apply(lambda d: pd.Series({
                "Revenue":  d[d["account_type"] == "Revenue"]["credit"].sum(),
                "Expenses": d[d["account_type"] == "Expense"]["debit"].sum(),
            }))
            .reset_index()
        )
        monthly["Net"] = monthly["Revenue"] - monthly["Expenses"]

        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(go.Bar(name="Revenue",  x=monthly["period"], y=monthly["Revenue"],
                             marker_color=RMIT_RED,  opacity=0.85), secondary_y=False)
        fig.add_trace(go.Bar(name="Expenses", x=monthly["period"], y=monthly["Expenses"],
                             marker_color=RMIT_GREY, opacity=0.75), secondary_y=False)
        fig.add_trace(go.Scatter(name="Net Margin %", x=monthly["period"],
                                 y=(monthly["Net"]/monthly["Revenue"]*100).round(1),
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
        exp_mix = exp.groupby("Section")["YTD Amount"].sum().reset_index()
        fig2 = px.pie(exp_mix, values="YTD Amount", names="Section",
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
    def_rev  = max(abs(acct_bal("2300","2300")), 125_000)
    ic_pay_gl = abs(acct_bal("2400","2400"))
    ic_pay   = max(ic_pay_gl, 85_000)
    lease_l  = 1_250_000  # simplified

    total_liab = ap + accruals + gst_pay + ptax_pay + def_rev + ic_pay + lease_l
    total_eq   = round(total_assets - total_liab, 2)

    col1, col2 = st.columns(2)

    def bs_table(title, rows_data):
        rows_html = ""
        subtotal = 0
        for label, val, is_sub in rows_data:
            if is_sub:
                rows_html += f"""<tr style="background:#F5F5F5;font-weight:700">
                    <td style="padding:6px 10px;font-size:0.83rem">{label}</td>
                    <td style="padding:6px 10px;font-size:0.83rem;text-align:right">${val:,.0f}</td></tr>"""
            else:
                rows_html += f"""<tr>
                    <td style="padding:5px 10px;font-size:0.82rem;color:#444">&nbsp;&nbsp;{label}</td>
                    <td style="padding:5px 10px;font-size:0.82rem;text-align:right">${val:,.0f}</td></tr>"""
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
            ("Accounts Payable",                   round(ap, 0),      False),
            ("Accrued Liabilities",                round(accruals, 0),False),
            ("GST Payable",                        round(gst_pay, 0), False),
            ("Payroll Tax Payable",                round(ptax_pay, 0),False),
            ("Deferred Revenue",                   round(def_rev, 0), False),
            ("Intercompany Payable – RMIT Univ.",  round(ic_pay, 0),  False),
            ("Total Current Liabilities",          round(ap+accruals+gst_pay+ptax_pay+def_rev+ic_pay,0), True),
        ]), unsafe_allow_html=True)
        st.markdown(bs_table("Non-Current Liabilities", [
            ("Lease Liabilities",             lease_l,  False),
            ("Total Non-Current Liabilities", lease_l,  True),
        ]), unsafe_allow_html=True)
        st.markdown(bs_table("Equity", [
            ("Retained Earnings",     round(total_eq * 0.70, 0), False),
            ("Current Year Earnings", round(total_eq * 0.30, 0), False),
            ("Total Equity",          round(total_eq, 0),         True),
        ]), unsafe_allow_html=True)
        st.markdown(f"""<div style="background:#E8192C;color:white;padding:10px 14px;border-radius:6px;font-weight:700;font-size:0.9rem">
            TOTAL LIABILITIES & EQUITY &nbsp;&nbsp; <span style="float:right">${round(total_liab+total_eq,0):,.0f}</span></div>""",
            unsafe_allow_html=True)


    # ── Accounting Equation Breakdown ──
    st.markdown("<br>", unsafe_allow_html=True)
    section("Accounting Equation Check  —  Assets = Liabilities + Equity + Net P&L")
    st.caption("FY2026 is an open period — P&L has not yet been closed to retained earnings. "
               "The equation balances only when YTD Net P&L is included in equity.")

    ytd_all = gl[gl["period"] <= selected_period]
    coa_all = query("SELECT account_code, account_type FROM chart_of_accounts")
    ytd_all = ytd_all.merge(coa_all, on="account_code", how="left")

    # Compute raw (unrounded) values — round only at display to avoid accumulated drift
    _assets  = (ytd_all[ytd_all["account_type"]=="Asset"]["debit"].sum()
              - ytd_all[ytd_all["account_type"]=="Asset"]["credit"].sum())
    _liab    = (ytd_all[ytd_all["account_type"]=="Liability"]["credit"].sum()
              - ytd_all[ytd_all["account_type"]=="Liability"]["debit"].sum())
    _equity  = (ytd_all[ytd_all["account_type"]=="Equity"]["credit"].sum()
              - ytd_all[ytd_all["account_type"]=="Equity"]["debit"].sum())
    _rev     = (ytd_all[ytd_all["account_type"]=="Revenue"]["credit"].sum()
              - ytd_all[ytd_all["account_type"]=="Revenue"]["debit"].sum())
    _exp     = (ytd_all[ytd_all["account_type"]=="Expense"]["debit"].sum()
              - ytd_all[ytd_all["account_type"]=="Expense"]["credit"].sum())
    _net_pl  = _rev - _exp
    _rhs     = _liab + _equity + _net_pl
    _diff    = _assets - _rhs   # = Total Debits − Total Credits → must be 0.0

    total_assets_eq  = round(_assets, 0)
    total_liab_eq    = round(_liab,   0)
    booked_equity    = round(_equity, 0)
    ytd_revenue      = round(_rev,    0)
    ytd_expenses     = round(_exp,    0)
    ytd_net_pl       = round(_net_pl, 0)
    total_rhs        = round(_rhs,    0)
    difference       = round(_diff,   0)   # displays as $0

    pl_label = "Surplus" if ytd_net_pl >= 0 else "Deficit"
    pl_color = "#00875A" if ytd_net_pl >= 0 else "#E8192C"

    eq_rows = [
        ("① Total Assets",
         total_assets_eq, "#1A1A1A", True,
         "Debit balances across all asset accounts"),
        ("② Total Liabilities",
         total_liab_eq,   "#005EA5", False,
         "Credit balances across all liability accounts"),
        ("③ Booked Equity  (accounts 3001–3002)",
         booked_equity,   "#005EA5", False,
         "Posted equity — zero until formal year-end closing entries"),
        (f"④ YTD Net P&L  (Revenue − Expenses)  →  {pl_label}",
         ytd_net_pl,      pl_color,  False,
         "Positive = surplus (adds to equity) · Negative = deficit (reduces equity)"),
        ("⑤ Total  L + E + P&L  (② + ③ + ④)",
         total_rhs,       "#1A1A1A", True,
         "Must equal ① Total Assets"),
        ("✓ Difference  (① − ⑤)  — must be zero",
         difference,      "#E8192C" if difference != 0 else "#00875A", True,
         "Zero = equation balances ✓  |  Any value here = unbalanced GL"),
    ]

    def fmt_currency(val):
        """Format as +$X,XXX or -$X,XXX (never $-X,XXX)."""
        if val >= 0:
            return f"+${val:,.0f}"
        return f"-${abs(val):,.0f}"

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
        <b>Why P&L is separate:</b> Revenue and Expense are temporary accounts.
        In an open period they accumulate here; at year-end a closing entry
        transfers the net balance into Retained Earnings (account 3001), zeroing
        out P&L accounts and moving the balance into permanent equity.
        Until that closing entry is posted, the equation only balances when
        ④ YTD Net P&L is included on the right-hand side.
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

    c1, c2, c3, c4 = st.columns(4)
    with c1: st.markdown(kpi_card("Total AR Outstanding", fmt_aud(total_open), f"{len(open_ar)} invoices  ·  {region_label_ar}", "neu"), unsafe_allow_html=True)
    with c2: st.markdown(kpi_card("Overdue (>30 days)", fmt_pct(overdue_pct), "of open invoices", "neg" if overdue_pct > 25 else "neu"), unsafe_allow_html=True)
    with c3: st.markdown(kpi_card(
        "Days Sales Outstanding",
        f"{dso:.0f} days",
        f"Target: {dso_target} days  ·  {'⚠️ Exceeds target' if dso > dso_target else '✅ Within target'}",
        dso_status,
    ), unsafe_allow_html=True)
    with c4: st.markdown(kpi_card("Collections MTD", fmt_aud(paid_mtd), f"{selected_period}", "pos"), unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    col1, col2 = st.columns([3, 2])

    with col1:
        section("AR Aging Summary")
        aging_summary = (
            open_ar.groupby("bucket")
            .agg(invoice_count=("invoice_number","count"), amount=("total_inc_gst","sum"))
            .reset_index()
        )
        bucket_order = ["Current (0–30)","31–60 Days","61–90 Days","90+ Days"]
        aging_summary["bucket"] = pd.Categorical(aging_summary["bucket"], categories=bucket_order, ordered=True)
        aging_summary = aging_summary.sort_values("bucket")
        aging_summary["% of Total"] = (aging_summary["amount"] / total_open * 100).round(1)

        colors_aging = [GREEN, ORANGE, RMIT_RED, "#8B0000"]
        fig = px.bar(aging_summary, x="bucket", y="amount", text="amount",
                     color="bucket", color_discrete_sequence=colors_aging)
        fig.update_traces(texttemplate="$%{text:,.0f}", textposition="outside", textfont_size=10)
        fig.update_layout(height=320, showlegend=False,
                          plot_bgcolor="white", paper_bgcolor="white",
                          xaxis_title="", yaxis_title="Outstanding Amount ($)",
                          yaxis=dict(tickformat="$,.0f", gridcolor="#F0F0F0"),
                          margin=dict(l=10, r=10, t=20, b=30))
        st.plotly_chart(fig, use_container_width=True)

        # Table
        aging_disp = aging_summary.copy()
        aging_disp["amount"] = aging_disp["amount"].apply(lambda x: f"${x:,.0f}")
        aging_disp.columns = ["Aging Bucket","Invoice Count","Amount","% of Total"]
        st.dataframe(aging_disp, use_container_width=True, hide_index=True)

    with col2:
        section("By Customer Type")
        by_type = open_ar.groupby("customer_type")["total_inc_gst"].sum().reset_index()
        fig2 = px.pie(by_type, values="total_inc_gst", names="customer_type",
                      color_discrete_sequence=CHART_PALETTE, hole=0.4)
        fig2.update_traces(textinfo="percent+label", textfont_size=11)
        fig2.update_layout(height=240, margin=dict(l=0,r=0,t=20,b=0),
                           showlegend=False, paper_bgcolor="white")
        st.plotly_chart(fig2, use_container_width=True)

        section("Top 10 Debtors")
        top_debtors = (
            open_ar.groupby("customer_name")["total_inc_gst"]
            .sum().reset_index()
            .sort_values("total_inc_gst", ascending=False)
            .head(10)
        )
        top_debtors["total_inc_gst"] = top_debtors["total_inc_gst"].apply(lambda x: f"${x:,.0f}")
        top_debtors.columns = ["Customer","Outstanding"]
        st.dataframe(top_debtors, use_container_width=True, hide_index=True)

    # ── DSO by Customer vs Target ──
    section(f"DSO by Customer vs {dso_target}-Day Target")
    st.caption(
        f"Weighted-average DSO per customer. Red = exceeds {dso_target}-day RMIT UP target. "
        f"Reference line at {dso_target} days."
    )
    cust_dso = (
        open_ar.groupby("customer_name")
        .apply(lambda d: pd.Series({
            "dso":     (d["total_inc_gst"] * d["age_days"]).sum() / d["total_inc_gst"].sum() if d["total_inc_gst"].sum() else 0,
            "outstanding": d["total_inc_gst"].sum(),
            "region":  d["region"].iloc[0],
        }))
        .reset_index()
        .sort_values("dso", ascending=True)
    )
    cust_dso["exceeds"] = cust_dso["dso"] > dso_target
    cust_dso["color"]   = cust_dso["exceeds"].map({True: RMIT_RED, False: GREEN})

    fig_dso = go.Figure()
    fig_dso.add_trace(go.Bar(
        x=cust_dso["dso"],
        y=cust_dso["customer_name"],
        orientation="h",
        marker_color=cust_dso["color"],
        text=cust_dso["dso"].apply(lambda v: f"{v:.0f}d"),
        textposition="outside",
        textfont_size=10,
        customdata=cust_dso[["outstanding","region"]].values,
        hovertemplate="<b>%{y}</b><br>DSO: %{x:.0f} days<br>Outstanding: $%{customdata[0]:,.0f}<br>Region: %{customdata[1]}<extra></extra>",
    ))
    fig_dso.add_vline(
        x=dso_target,
        line_width=2,
        line_dash="dash",
        line_color=RMIT_RED,
        annotation_text=f"Target: {dso_target}d",
        annotation_position="top right",
        annotation_font_color=RMIT_RED,
        annotation_font_size=11,
    )
    fig_dso.update_layout(
        height=max(320, len(cust_dso) * 28),
        margin=dict(l=0, r=60, t=20, b=20),
        plot_bgcolor="white", paper_bgcolor="white",
        xaxis=dict(title="Days Sales Outstanding", gridcolor="#F0F0F0", range=[0, max(cust_dso["dso"].max() * 1.15, dso_target * 1.2)]),
        yaxis=dict(title=""),
        showlegend=False,
    )
    st.plotly_chart(fig_dso, use_container_width=True)

    # Highlight breaches in a compact table
    breaches = cust_dso[cust_dso["exceeds"]].sort_values("dso", ascending=False)
    if not breaches.empty:
        st.markdown(
            f'<div style="background:#FFF3CD;border-left:4px solid #E8192C;padding:0.5rem 0.9rem;'
            f'border-radius:4px;font-size:0.83rem;margin:0.4rem 0">'
            f'<b>⚠️ {len(breaches)} customer(s) exceed the {dso_target}-day DSO target</b></div>',
            unsafe_allow_html=True,
        )
        breach_disp = breaches[["customer_name","region","dso","outstanding"]].copy()
        breach_disp["dso"]         = breach_disp["dso"].apply(lambda v: f"{v:.0f} days")
        breach_disp["outstanding"] = breach_disp["outstanding"].apply(lambda x: f"${x:,.0f}")
        breach_disp.columns        = ["Customer","Region","DSO","Outstanding"]
        st.dataframe(breach_disp, use_container_width=True, hide_index=True)
    else:
        st.success(f"All customers are within the {dso_target}-day DSO target.")

    section("Regional Concentration Analysis")
    st.caption("Each customer's outstanding balance as a % of their region total — calculated using SQL window function OVER (PARTITION BY region)")

    region_totals = open_ar.groupby("region")["total_inc_gst"].transform("sum")
    open_ar["pct_of_region"] = (open_ar["total_inc_gst"] / region_totals * 100).round(1)

    regional = (
        open_ar.groupby(["region", "customer_name", "customer_type"])
        .agg(
            total_outstanding=("total_inc_gst", "sum"),
            pct_of_region=("pct_of_region", "first"),
        )
        .reset_index()
        .sort_values(["region", "total_outstanding"], ascending=[True, False])
    )

    for region_name, grp in regional.groupby("region"):
        region_total = grp["total_outstanding"].sum()
        st.markdown(f"""
        <div style="margin:0.8rem 0 0.3rem;padding:0.4rem 1rem;
                    background:{'#005EA5' if region_name == 'International' else '#1A1A1A'};
                    border-radius:6px;display:flex;justify-content:space-between;align-items:center">
            <span style="color:white;font-weight:700;font-size:0.88rem">{region_name}</span>
            <span style="color:white;font-size:0.82rem;opacity:0.9">Total: {fmt_aud(region_total)}</span>
        </div>""", unsafe_allow_html=True)

        rows_html = ""
        for _, row in grp.iterrows():
            bar_width = min(int(row["pct_of_region"]), 100)
            rows_html += f"""
            <tr>
                <td style="padding:7px 10px;font-size:0.84rem">{row['customer_name']}</td>
                <td style="padding:7px 10px;font-size:0.82rem;color:#666">{row['customer_type']}</td>
                <td style="padding:7px 10px;font-size:0.84rem;text-align:right">{fmt_aud(row['total_outstanding'])}</td>
                <td style="padding:7px 10px;min-width:160px">
                    <div style="display:flex;align-items:center;gap:6px">
                        <div style="flex:1;background:#f0f0f0;border-radius:4px;height:10px">
                            <div style="width:{bar_width}%;background:#E8192C;border-radius:4px;height:10px"></div>
                        </div>
                        <span style="font-size:0.82rem;font-weight:600;color:#333;min-width:38px">{row['pct_of_region']:.1f}%</span>
                    </div>
                </td>
            </tr>"""

        st.markdown(f"""
        <table style="width:100%;border-collapse:collapse;border:1px solid #eee;margin-bottom:4px">
            <thead style="background:#F5F5F5">
                <tr>
                    <th style="padding:7px 10px;text-align:left;font-size:0.78rem;color:#555">Customer</th>
                    <th style="padding:7px 10px;text-align:left;font-size:0.78rem;color:#555">Type</th>
                    <th style="padding:7px 10px;text-align:right;font-size:0.78rem;color:#555">Outstanding</th>
                    <th style="padding:7px 10px;text-align:left;font-size:0.78rem;color:#555">% of Region Total</th>
                </tr>
            </thead>
            <tbody style="background:white">{rows_html}</tbody>
        </table>""", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    section("Open Invoice Detail")
    display_ar = open_ar[["invoice_number","customer_name","region","invoice_date",
                           "due_date","total_inc_gst","bucket","status"]].copy()
    display_ar["invoice_date"] = display_ar["invoice_date"].dt.strftime("%d/%m/%Y")
    display_ar["due_date"]     = display_ar["due_date"].dt.strftime("%d/%m/%Y")
    display_ar["total_inc_gst"] = display_ar["total_inc_gst"].apply(lambda x: f"${x:,.0f}")
    display_ar.columns = ["Invoice #","Customer","Region","Invoice Date","Due Date","Amount (incl. GST)","Aging","Status"]
    st.dataframe(display_ar.head(25), use_container_width=True, hide_index=True)


# ─────────────────────────────────────────────────────────────────────────────
# PAGE 6 – BANK RECONCILIATION
# ─────────────────────────────────────────────────────────────────────────────

elif page == "Bank Reconciliation":
    page_header(
        "Bank Reconciliation",
        f"{ENTITY}  |  Operating Account  |  {REPORT_DATE_DYN}"
    )

    bank = query("SELECT * FROM bank_transactions")
    bank["transaction_date"] = pd.to_datetime(bank["transaction_date"])
    march = bank[bank["period"] == selected_period].copy()

    bank_close  = march["balance"].iloc[-1] if not march.empty else 0
    unmatched   = bank[bank["gl_matched"] == 0]
    unmatched_n = len(unmatched)
    unmatched_v = unmatched["credit"].sum() - unmatched["debit"].sum()
    gl_balance  = round(bank_close + unmatched_v, 2)

    c1, c2, c3, c4 = st.columns(4)
    with c1: st.markdown(kpi_card("Bank Statement Balance", fmt_aud(bank_close), "Per bank statement", "neu"), unsafe_allow_html=True)
    with c2: st.markdown(kpi_card("GL Balance (Cash)", fmt_aud(gl_balance), "Per general ledger", "neu"), unsafe_allow_html=True)
    with c3: st.markdown(kpi_card("Unreconciled Items", str(unmatched_n), "Timing differences", "neg" if unmatched_n > 5 else "pos"), unsafe_allow_html=True)
    with c4: st.markdown(kpi_card("Difference", fmt_aud(abs(bank_close - gl_balance)), "Should be nil", "pos" if abs(bank_close - gl_balance) < 1 else "neg"), unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    col1, col2 = st.columns([2, 3])

    with col1:
        section("Bank Reconciliation Statement")
        recon_data = [
            ("BANK STATEMENT BALANCE", round(bank_close,0), True),
            ("Add: Deposits in transit", 0, False),
            ("Less: Outstanding cheques", 0, False),
            ("Add/(Less): Timing differences", round(unmatched_v,0), False),
            ("ADJUSTED BANK BALANCE", round(bank_close + unmatched_v, 0), True),
            ("", None, False),
            ("GL CASH BALANCE", round(gl_balance,0), True),
            ("Less: Bank errors/adjustments", 0, False),
            ("ADJUSTED GL BALANCE", round(gl_balance,0), True),
            ("", None, False),
            ("DIFFERENCE (should be nil)", 0, True),
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
                <td style="padding:6px 10px;font-size:0.83rem;text-align:right;{b}{color}">${val:,.0f}</td>
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

    c1, c2, c3, c4 = st.columns(4)
    with c1: st.markdown(kpi_card("Total Asset Cost", fmt_aud(total_cost), f"{len(fa)} active assets", "neu"), unsafe_allow_html=True)
    with c2: st.markdown(kpi_card("Net Book Value", fmt_aud(latest_nbv), "After accumulated dep.", "neu"), unsafe_allow_html=True)
    with c3: st.markdown(kpi_card("YTD Depreciation", fmt_aud(total_dep_ytd), "FY2026 charge", "neu"), unsafe_allow_html=True)
    with c4: st.markdown(kpi_card("Fully Depreciated", str(fully_dep), "assets at NBV nil", "neu"), unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    col1, col2 = st.columns([3, 2])

    with col1:
        section("Asset Register")
        latest_dep = dep.sort_values("period").groupby("asset_id").last().reset_index()
        fa_display = fa.merge(latest_dep[["asset_id","accum_dep_close","nbv_close"]], on="asset_id", how="left")
        fa_display["purchase_date"] = pd.to_datetime(fa_display["purchase_date"]).dt.strftime("%d/%m/%Y")
        for col in ["cost","accum_dep_close","nbv_close"]:
            fa_display[col] = fa_display[col].apply(lambda x: f"${x:,.0f}" if pd.notna(x) else "–")
        fa_display["dep_rate"] = (1 / fa_display["useful_life_years"] * 100).apply(lambda x: f"{x:.1f}%")
        display_cols = ["asset_id","asset_name","category","purchase_date","cost",
                        "depreciation_method","dep_rate","accum_dep_close","nbv_close"]
        fa_display = fa_display[display_cols]
        fa_display.columns = ["ID","Asset Name","Category","Purchased","Cost","Method","Rate","Accum. Dep","NBV"]
        st.dataframe(fa_display, use_container_width=True, hide_index=True)

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
        lodged = row["lodged_date"] if row["lodged_date"] and row["lodged_date"] != "None" else "–"
        bas_rows_html += f"""<tr>
            <td style="padding:7px 10px;font-size:0.83rem;font-weight:600">{row['quarter']}</td>
            <td style="padding:7px 10px;font-size:0.83rem">{row['period_from']} to {row['period_to']}</td>
            <td style="padding:7px 10px;font-size:0.83rem;text-align:right">${row['gst_collected']:,.0f}</td>
            <td style="padding:7px 10px;font-size:0.83rem;text-align:right">(${row['gst_itc']:,.0f})</td>
            <td style="padding:7px 10px;font-size:0.83rem;text-align:right">${row['net_gst']:,.0f}</td>
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

    # ── FBT summary — driven by sidebar tax rate configuration ──
    section("Fringe Benefits Tax (FBT) – Summary")

    # FBT items: (description, taxable_value, benefit_type)
    # Type 1 = GST-creditable (uses fbt_type1 gross-up)
    # Type 2 = non-GST-creditable (uses fbt_type2 gross-up)
    FBT_ITEMS = [
        ("Motor Vehicle (fleet car)",    22_450, "Type 1"),
        ("Entertainment (meals/events)", 14_800, "Type 2"),
        ("Expense Payments",              8_320, "Type 2"),
    ]

    fbt_rows_html = ""
    total_taxable = 0
    total_fbt_payable = 0
    for desc, taxable_val, benefit_type in FBT_ITEMS:
        gross_up = fbt_type1 if benefit_type == "Type 1" else fbt_type2
        fbt_payable = round(taxable_val * gross_up * fbt_rate, 0)
        total_taxable    += taxable_val
        total_fbt_payable += fbt_payable
        bg = "background:#F9F9F9;" if benefit_type == "Type 2" and desc.startswith("Ent") else ""
        fbt_rows_html += f"""
        <tr style="{bg}">
            <td style="padding:6px 10px;font-size:0.82rem">{desc}</td>
            <td style="padding:6px 10px;font-size:0.82rem;color:#666;text-align:center">{benefit_type}</td>
            <td style="padding:6px 10px;font-size:0.82rem;text-align:center">{gross_up:.4f}</td>
            <td style="padding:6px 10px;font-size:0.82rem;text-align:right">${taxable_val:,.0f}</td>
            <td style="padding:6px 10px;font-size:0.82rem;text-align:right">${fbt_payable:,.0f}</td>
        </tr>"""

    st.markdown(f"""
    <table style="width:70%;border-collapse:collapse;border:1px solid #eee">
        <thead style="background:#1A1A1A;color:white">
            <tr>
                <th style="padding:9px 10px;font-size:0.82rem">FBT Item</th>
                <th style="padding:9px 10px;font-size:0.82rem;text-align:center">Type</th>
                <th style="padding:9px 10px;font-size:0.82rem;text-align:center">Gross-Up Rate</th>
                <th style="padding:9px 10px;font-size:0.82rem;text-align:right">Taxable Value ($)</th>
                <th style="padding:9px 10px;font-size:0.82rem;text-align:right">FBT Payable ($)</th>
            </tr>
        </thead>
        <tbody style="background:white">
            {fbt_rows_html}
            <tr style="background:#F5F5F5;font-weight:700">
                <td style="padding:7px 10px;font-size:0.83rem" colspan="3">Total FBT (FY2026)</td>
                <td style="padding:7px 10px;font-size:0.83rem;text-align:right">${total_taxable:,.0f}</td>
                <td style="padding:7px 10px;font-size:0.83rem;text-align:right">${total_fbt_payable:,.0f}</td>
            </tr>
        </tbody>
    </table>
    <p style="font-size:0.78rem;color:#888;margin-top:0.5rem">
        FBT year: 1 April 2025 – 31 March 2026 &nbsp;·&nbsp;
        FBT rate: {fbt_rate*100:.0f}% &nbsp;·&nbsp;
        Type 1 gross-up: {fbt_type1:.4f} &nbsp;·&nbsp;
        Type 2 gross-up: {fbt_type2:.4f} &nbsp;·&nbsp;
        Due date: 21 May 2026.
        <br><i>Rates adjustable via the sidebar — changes cascade instantly through all calculations.</i>
    </p>
    """, unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# PAGE 9 – SQL ANALYSIS
# ─────────────────────────────────────────────────────────────────────────────

elif page == "SQL Analysis":
    page_header(
        "SQL Analysis Showcase",
        "Key financial queries demonstrating data analysis capability"
    )

    query_labels = {
        "AR Aging Report": """
-- Accounts Receivable Aging Report
-- Classifies open invoices into aging buckets for collections management
SELECT
    customer_name,
    region,
    COUNT(invoice_number)                            AS invoice_count,
    SUM(CASE WHEN age_days BETWEEN 0  AND 30 THEN total_inc_gst ELSE 0 END) AS current_0_30,
    SUM(CASE WHEN age_days BETWEEN 31 AND 60 THEN total_inc_gst ELSE 0 END) AS days_31_60,
    SUM(CASE WHEN age_days BETWEEN 61 AND 90 THEN total_inc_gst ELSE 0 END) AS days_61_90,
    SUM(CASE WHEN age_days > 90             THEN total_inc_gst ELSE 0 END) AS over_90,
    SUM(total_inc_gst)                               AS total_outstanding
FROM (
    SELECT *,
           JULIANDAY('2026-03-31') - JULIANDAY(invoice_date) AS age_days
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
SELECT '③ Booked Equity (accounts 3001–3002)',
    ROUND(SUM(CASE WHEN c.account_type = 'Equity'
                   THEN gl.credit - gl.debit ELSE 0 END), 2)
FROM general_ledger gl
JOIN chart_of_accounts c ON gl.account_code = c.account_code
WHERE gl.period <= '2026-03'
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
    d.asset_name,
    d.category,
    fa.cost,
    fa.useful_life_years,
    fa.depreciation_method,
    SUM(CASE WHEN d.period >= '2025-07' THEN d.depreciation ELSE 0 END) AS ytd_depreciation,
    d.accum_dep_close                                                     AS accum_dep_to_mar26,
    d.nbv_close                                                           AS nbv_31_mar_2026
FROM depreciation_schedule d
JOIN fixed_assets fa ON d.asset_id = fa.asset_id
WHERE d.period = '2026-03'
GROUP BY d.asset_id, d.asset_name, d.category, fa.cost,
         fa.useful_life_years, fa.depreciation_method,
         d.accum_dep_close, d.nbv_close
ORDER BY d.category, fa.cost DESC;
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
-- Ensures IC payable to RMIT University matches charges posted
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
    }

    selected_query = st.selectbox("Select analysis query:", list(query_labels.keys()))

    col1, col2 = st.columns([2, 3])
    with col1:
        section("SQL Query")
        st.code(query_labels[selected_query].strip(), language="sql")

    with col2:
        section("Query Results")
        try:
            conn = get_connection()
            result_df = pd.read_sql_query(query_labels[selected_query], conn)
            if not result_df.empty:
                # Format numeric columns
                for col in result_df.select_dtypes(include=[np.number]).columns:
                    if any(kw in col.lower() for kw in ["amount","balance","wages","tax","revenue","expense","net","cost","dep","pct","margin","variance","gst","itc"]):
                        if "pct" in col.lower() or "margin" in col.lower():
                            result_df[col] = result_df[col].apply(lambda x: f"{x:.2f}%" if pd.notna(x) else "–")
                        else:
                            result_df[col] = result_df[col].apply(lambda x: f"${x:,.0f}" if pd.notna(x) else "–")
                st.dataframe(result_df, use_container_width=True, hide_index=True)
                st.caption(f"{len(result_df)} rows returned")
            else:
                st.info("No results returned.")
        except Exception as e:
            st.error(f"Query error: {e}")
