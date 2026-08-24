-- Window functions. Runs on Trino over Iceberg (gold layer).

-- 1) Top earner per department (the classic prompt). DENSE_RANK ties share a rank.
SELECT dept, emp_id, salary
FROM (
  SELECT emp_id, dept, salary,
         DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary DESC) AS rnk
  FROM lake.silver.hr_employees
) t
WHERE rnk = 1;

-- 2) Max salary per department attached to every employee row (no collapse, unlike GROUP BY).
SELECT emp_id, dept, salary,
       MAX(salary) OVER (PARTITION BY dept)                      AS dept_max_salary,
       salary - AVG(salary) OVER (PARTITION BY dept)             AS delta_from_dept_avg
FROM lake.silver.hr_employees;

-- 3) Running total of invoice amount per customer, ordered in time.
SELECT customer_id, invoice_id, amount,
       SUM(amount) OVER (PARTITION BY customer_id ORDER BY invoice_id
                         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total
FROM lake.silver.billing_invoices;

-- 4) Period-over-period change with LAG.
SELECT customer_id, invoice_id, amount,
       amount - LAG(amount) OVER (PARTITION BY customer_id ORDER BY invoice_id) AS mom_change
FROM lake.silver.billing_invoices;

-- ================================================================================================
-- Glossary
--   Window function     Computes across a set of rows WITHOUT collapsing them (unlike GROUP BY).
--   OVER (...)          Defines the window: PARTITION BY (groups) + ORDER BY (sequence) + frame.
--   PARTITION BY        Resets the calculation per group (e.g. per dept, per customer).
--   ORDER BY (in OVER)  Orders rows within each partition; required for ranking + running totals.
--   ROW_NUMBER()        Unique 1..N per partition; ties broken arbitrarily.
--   RANK()              Ranks with gaps after ties (1,1,3).
--   DENSE_RANK()        Ranks with no gaps after ties (1,1,2); used for "top earner per dept".
--   MAX/AVG/SUM OVER    Aggregate as a window: value attached to every row, no collapse.
--   Frame (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)  Rows included → running total.
--   LAG(col) / LEAD(col) Value from the previous / next row in the partition → period-over-period.
--   lake.silver.*       Iceberg tables in the silver layer, queried here via Trino.
-- ================================================================================================
