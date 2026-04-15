-- ============================================================
-- RMIT UP Financial Analysis – SQL Query Reference
-- Entity:  RMIT UP Pty Ltd  |  FY2026 (Jul 2025 – Jun 2026)
-- Author:  Financial Accountant Candidate
-- Updated: March 2026
-- ============================================================
-- NOTE: All queries target the SQLite database at data/rmit_finance.db
-- Run via: sqlite3 data/rmit_finance.db < sql/analysis_queries.sql
--          or paste into the SQL Analysis page of the Streamlit dashboard
-- ============================================================


-- ────────────────────────────────────────────────────────────
-- 1. CHART OF ACCOUNTS OVERVIEW
-- ────────────────────────────────────────────────────────────
SELECT
    account_code,
    account_name,
    account_type,
    report_section,
    normal_balance
FROM chart_of_accounts
ORDER BY account_code;


-- ────────────────────────────────────────────────────────────
-- 2. TRIAL BALANCE – as at 31 March 2026
-- ────────────────────────────────────────────────────────────
SELECT
    gl.account_code,
    c.account_name,
    c.account_type,
    c.report_section,
    ROUND(SUM(gl.debit),  2) AS total_debits,
    ROUND(SUM(gl.credit), 2) AS total_credits,
    ROUND(SUM(gl.debit) - SUM(gl.credit), 2) AS net_balance
FROM   general_ledger gl
JOIN   chart_of_accounts c ON gl.account_code = c.account_code
WHERE  gl.period <= '2026-03'
GROUP  BY gl.account_code, c.account_name, c.account_type, c.report_section
ORDER  BY gl.account_code;


-- ────────────────────────────────────────────────────────────
-- 3. INCOME STATEMENT – YTD FY2026
-- ────────────────────────────────────────────────────────────
SELECT
    c.report_section                                                        AS category,
    c.account_name                                                          AS line_item,
    ROUND(SUM(CASE WHEN c.account_type = 'Revenue' THEN gl.credit ELSE 0 END)
         -SUM(CASE WHEN c.account_type = 'Expense' THEN gl.debit  ELSE 0 END), 2) AS amount
FROM   general_ledger gl
JOIN   chart_of_accounts c ON gl.account_code = c.account_code
WHERE  c.account_type IN ('Revenue','Expense')
  AND  gl.period <= '2026-03'
GROUP  BY c.report_section, c.account_name
ORDER  BY c.account_type DESC, amount DESC;


-- ────────────────────────────────────────────────────────────
-- 4. MONTHLY P&L SUMMARY WITH MoM VARIANCE
-- ────────────────────────────────────────────────────────────
WITH monthly AS (
    SELECT
        gl.period,
        ROUND(SUM(CASE WHEN c.account_type = 'Revenue' THEN gl.credit ELSE 0 END), 2) AS revenue,
        ROUND(SUM(CASE WHEN c.account_type = 'Expense' THEN gl.debit  ELSE 0 END), 2) AS expenses
    FROM   general_ledger gl
    JOIN   chart_of_accounts c ON gl.account_code = c.account_code
    WHERE  gl.period <= '2026-03'
    GROUP  BY gl.period
)
SELECT
    period,
    revenue,
    expenses,
    ROUND(revenue - expenses, 2)                              AS net_result,
    ROUND((revenue - expenses) / revenue * 100, 2)           AS net_margin_pct,
    ROUND(revenue - LAG(revenue) OVER (ORDER BY period), 2)  AS rev_mom_var,
    ROUND((revenue - LAG(revenue) OVER (ORDER BY period))
          / LAG(revenue) OVER (ORDER BY period) * 100, 1)    AS rev_mom_pct
FROM monthly
ORDER BY period;


-- ────────────────────────────────────────────────────────────
-- 5. ACCOUNTS RECEIVABLE AGING REPORT WITH REGIONAL CONCENTRATION
--    Window function: OVER (PARTITION BY region) shows each
--    customer's share of their region's total outstanding balance.
-- ────────────────────────────────────────────────────────────
SELECT
    customer_name,
    customer_type,
    region,
    COUNT(invoice_number)                                                             AS invoice_count,
    ROUND(SUM(CASE WHEN age_days BETWEEN 0  AND 30 THEN total_inc_gst ELSE 0 END),2) AS current_0_30,
    ROUND(SUM(CASE WHEN age_days BETWEEN 31 AND 60 THEN total_inc_gst ELSE 0 END),2) AS days_31_60,
    ROUND(SUM(CASE WHEN age_days BETWEEN 61 AND 90 THEN total_inc_gst ELSE 0 END),2) AS days_61_90,
    ROUND(SUM(CASE WHEN age_days > 90              THEN total_inc_gst ELSE 0 END),2) AS over_90_days,
    ROUND(SUM(total_inc_gst), 2)                                                     AS total_outstanding,
    -- Window function: customer balance as % of their region's total
    ROUND(
        SUM(total_inc_gst) * 100.0
        / SUM(SUM(total_inc_gst)) OVER (PARTITION BY region),
    1) AS pct_of_region_total
FROM (
    SELECT *,
           CAST(JULIANDAY('2026-03-31') - JULIANDAY(invoice_date) AS INTEGER) AS age_days
    FROM   accounts_receivable
    WHERE  status != 'Paid'
) t
GROUP BY customer_name, customer_type, region
ORDER BY region, total_outstanding DESC;


-- ────────────────────────────────────────────────────────────
-- 6. DAYS SALES OUTSTANDING (DSO) CALCULATION
-- ────────────────────────────────────────────────────────────
WITH open_invoices AS (
    SELECT
        total_inc_gst,
        CAST(JULIANDAY('2026-03-31') - JULIANDAY(invoice_date) AS INTEGER) AS age_days
    FROM accounts_receivable
    WHERE status != 'Paid'
),
revenue_ytd AS (
    SELECT SUM(credit) AS ytd_revenue
    FROM   general_ledger
    WHERE  account_code BETWEEN '4001' AND '4999'
      AND  period <= '2026-03'
)
SELECT
    ROUND(SUM(o.total_inc_gst * o.age_days) / SUM(o.total_inc_gst), 1) AS weighted_avg_dso_days,
    COUNT(*)                                                              AS open_invoices,
    ROUND(SUM(o.total_inc_gst), 2)                                       AS total_ar_outstanding,
    ROUND(r.ytd_revenue, 2)                                               AS ytd_revenue,
    ROUND(SUM(o.total_inc_gst) / (r.ytd_revenue / 274) , 1)             AS dso_turnover_days
FROM   open_invoices o
CROSS  JOIN revenue_ytd r;


-- ────────────────────────────────────────────────────────────
-- 7. BANK RECONCILIATION – MARCH 2026
-- ────────────────────────────────────────────────────────────
-- Statement balance
SELECT
    'Bank Statement Closing Balance'   AS item,
    ROUND(MAX(balance), 2)             AS amount
FROM bank_transactions
WHERE period = '2026-03'
UNION ALL
-- Unmatched transactions (timing differences)
SELECT
    'Unmatched / Timing Differences',
    ROUND(SUM(credit) - SUM(debit), 2)
FROM bank_transactions
WHERE gl_matched = 0
UNION ALL
-- Count of unreconciled items
SELECT
    'Unreconciled Item Count',
    COUNT(*)
FROM bank_transactions
WHERE gl_matched = 0
  AND period = '2026-03';


-- ────────────────────────────────────────────────────────────
-- 8. FIXED ASSET SCHEDULE & DEPRECIATION YTD
-- ────────────────────────────────────────────────────────────
SELECT
    fa.asset_id,
    fa.asset_name,
    fa.category,
    fa.cost,
    fa.purchase_date,
    fa.useful_life_years,
    fa.depreciation_method,
    ROUND(fa.cost / fa.useful_life_years, 2)     AS annual_dep,
    ROUND(fa.cost / fa.useful_life_years / 12,2) AS monthly_dep,
    fa.accum_dep_opening                          AS accum_dep_1_jul_2025,
    ROUND(SUM(CASE WHEN d.period >= '2025-07' THEN d.depreciation ELSE 0 END), 2) AS ytd_dep_fy2026,
    ROUND(MAX(d.accum_dep_close), 2)              AS accum_dep_31_mar_2026,
    ROUND(MAX(d.nbv_close), 2)                    AS nbv_31_mar_2026,
    fa.status
FROM   fixed_assets fa
JOIN   depreciation_schedule d ON fa.asset_id = d.asset_id
WHERE  d.period <= '2026-03'
GROUP  BY fa.asset_id, fa.asset_name, fa.category, fa.cost,
          fa.purchase_date, fa.useful_life_years, fa.depreciation_method,
          fa.accum_dep_opening, fa.status
ORDER  BY fa.category, fa.cost DESC;


-- ────────────────────────────────────────────────────────────
-- 9. PAYROLL TAX RECONCILIATION – VIC
-- ────────────────────────────────────────────────────────────
SELECT
    period,
    ROUND(gross_wages, 2)        AS gross_wages,
    ROUND(threshold, 2)          AS monthly_threshold,
    ROUND(taxable_wages, 2)      AS taxable_wages,
    ROUND(tax_rate * 100, 2)     AS rate_pct,
    ROUND(tax_due, 2)            AS payroll_tax_due,
    due_date,
    lodgement_status,
    payment_status
FROM payroll_tax
WHERE period <= '2026-03'
ORDER BY period;


-- ────────────────────────────────────────────────────────────
-- 10. GST RECONCILIATION – OUTPUT TAX vs INPUT TAX CREDITS
-- ────────────────────────────────────────────────────────────
SELECT
    period,
    ROUND(SUM(CASE WHEN transaction_type = 'Output Tax'       THEN gst_amount ELSE 0 END), 2) AS gst_collected,
    ROUND(SUM(CASE WHEN transaction_type = 'Input Tax Credit' THEN gst_amount ELSE 0 END), 2) AS input_tax_credits,
    ROUND(SUM(CASE WHEN transaction_type = 'Output Tax'       THEN gst_amount ELSE 0 END)
         -SUM(CASE WHEN transaction_type = 'Input Tax Credit' THEN gst_amount ELSE 0 END), 2) AS net_gst_payable
FROM gst_transactions
WHERE period <= '2026-03'
GROUP BY period
ORDER BY period;


-- ────────────────────────────────────────────────────────────
-- 11. QUARTERLY BAS LODGEMENT SUMMARY
-- ────────────────────────────────────────────────────────────
SELECT
    quarter,
    period_from || ' to ' || period_to AS period,
    ROUND(gst_collected, 2)            AS gst_on_sales,
    ROUND(gst_itc, 2)                  AS less_input_tax_credits,
    ROUND(net_gst, 2)                  AS net_gst,
    ROUND(withheld_tax_payg, 2)        AS payg_withheld,
    ROUND(total_payable, 2)            AS total_payable,
    due_date,
    lodged_date,
    lodgement_status,
    payment_status
FROM bas_returns
ORDER BY quarter;


-- ────────────────────────────────────────────────────────────
-- 12. INTERCOMPANY RECONCILIATION – RMIT UNIVERSITY
-- ────────────────────────────────────────────────────────────
SELECT
    ic.period,
    ic.description,
    ROUND(ic.amount, 2)                  AS ic_charge,
    ic.status,
    ROUND(SUM(CASE WHEN gl.account_code = '5300' THEN gl.debit  ELSE 0 END), 2) AS dr_5300_ic_expense,
    ROUND(SUM(CASE WHEN gl.account_code = '2400' THEN gl.credit ELSE 0 END), 2) AS cr_2400_ic_payable,
    ROUND(SUM(CASE WHEN gl.account_code = '5300' THEN gl.debit  ELSE 0 END)
         -SUM(CASE WHEN gl.account_code = '2400' THEN gl.credit ELSE 0 END), 2) AS difference
FROM   intercompany ic
LEFT   JOIN general_ledger gl
         ON gl.period = ic.period
        AND gl.account_code IN ('2400','5300')
        AND gl.journal_type = 'Intercompany'
WHERE  ic.period <= '2026-03'
GROUP  BY ic.period, ic.description, ic.amount, ic.status
ORDER  BY ic.period;


-- ────────────────────────────────────────────────────────────
-- 13. MONTH-END CLOSE PERFORMANCE
-- ────────────────────────────────────────────────────────────
SELECT
    period,
    COUNT(*)                                              AS total_tasks,
    SUM(CASE WHEN status = 'Complete'    THEN 1 ELSE 0 END) AS completed,
    SUM(CASE WHEN status = 'In Progress' THEN 1 ELSE 0 END) AS in_progress,
    SUM(CASE WHEN status = 'Pending'     THEN 1 ELSE 0 END) AS pending,
    ROUND(SUM(CASE WHEN status = 'Complete' THEN 1 ELSE 0 END)
          * 100.0 / COUNT(*), 1)                          AS completion_pct,
    MAX(CASE WHEN status = 'Complete' THEN completed_date END) AS last_task_completed
FROM month_end_checklist
GROUP BY period, is_current
ORDER BY period;


-- ────────────────────────────────────────────────────────────
-- 14. JOURNAL ENTRY AUDIT – TOP JOURNALS BY VALUE
-- ────────────────────────────────────────────────────────────
SELECT
    gl.journal_id,
    gl.period,
    gl.journal_date,
    gl.journal_type,
    gl.account_code,
    c.account_name,
    gl.description,
    ROUND(gl.debit,  2) AS debit,
    ROUND(gl.credit, 2) AS credit,
    gl.posted_by
FROM   general_ledger gl
JOIN   chart_of_accounts c ON gl.account_code = c.account_code
WHERE  gl.period = '2026-03'
ORDER  BY (gl.debit + gl.credit) DESC
LIMIT  30;


-- ────────────────────────────────────────────────────────────
-- 15. COST CENTRE EXPENSE ANALYSIS
-- ────────────────────────────────────────────────────────────
SELECT
    gl.cost_centre,
    cc.cost_centre_name,
    c.report_section,
    ROUND(SUM(gl.debit), 2)  AS total_expenses_ytd
FROM   general_ledger gl
JOIN   chart_of_accounts c  ON gl.account_code  = c.account_code
JOIN   cost_centres     cc  ON gl.cost_centre   = cc.cost_centre_code
WHERE  c.account_type = 'Expense'
  AND  gl.period <= '2026-03'
GROUP  BY gl.cost_centre, cc.cost_centre_name, c.report_section
ORDER  BY total_expenses_ytd DESC;
