-- A gold mart built by transforming silver in-warehouse: ELT, plus window functions.
-- Ranks segments by revenue and shows each segment's share, via window functions over an aggregate.
WITH rev AS (
  SELECT c.segment, sum(CAST(i.amount AS double)) AS revenue   -- bronze lands as strings; cast at read
  FROM {{ ref('dim_customer_scd2') }} c
  JOIN {{ source('bronze', 'billing_invoices') }} i
    ON i.customer_id = c.id
  WHERE c.dbt_valid_to IS NULL          -- current SCD2 version only
  GROUP BY c.segment
)
SELECT
  segment,
  revenue,
  RANK()  OVER (ORDER BY revenue DESC)                       AS revenue_rank,
  revenue * 1.0 / SUM(revenue) OVER ()                        AS revenue_share
FROM rev

{% raw %}
-- ================================================================================================
-- Glossary  (wrapped in {% raw %} so dbt doesn't evaluate the {{ }} examples below as real calls)
--   Model                A SELECT dbt materializes as a view/table (this one → a gold table).
--   {{ ref('...') }}     Depends on another dbt object (the SCD2 snapshot); builds the DAG order.
--   {{ source('...') }}  Reads a declared raw source table (bronze billing_invoices).
--   WITH ... AS (CTE)    Common Table Expression; names a subquery (rev) for readability.
--   dbt_valid_to IS NULL Filters the SCD2 snapshot to the current version of each customer.
--   RANK() OVER (ORDER BY ...)   Ranks segments by revenue; a window function.
--   SUM(revenue) OVER () Grand total across all rows → used to compute each segment's share.
--   Gold layer           Aggregated, business-ready marts serving BI + ML from one copy.
--   ELT                  This transform runs in-warehouse (Trino) after raw load.
-- ================================================================================================
{% endraw %}
