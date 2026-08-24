-- SCD Type 2 the declarative way. dbt maintains dbt_valid_from / dbt_valid_to and a
-- surrogate key, tracking history on the columns in `check_cols`. Same semantics as the manual
-- MERGE in spark/batch.py. Shown both ways on purpose.
{% snapshot dim_customer_scd2 %}
{{
  config(
    target_schema='silver',
    unique_key='id',
    strategy='check',
    check_cols=['segment', 'region']
  )
}}
SELECT id, name, segment, region
FROM {{ source('bronze', 'crm_customers') }}
{% endsnapshot %}

{% raw %}
-- ================================================================================================
-- Glossary  (wrapped in {% raw %} so dbt doesn't evaluate the {{ }} / {% %} examples below)
--   {% snapshot %}       dbt block that materializes and maintains SCD Type 2 history over time.
--   config()             Sets snapshot behavior (target schema, key, change-detection strategy).
--   target_schema        Where the snapshot table is written (silver here).
--   unique_key           Business key identifying a logical record across versions (id).
--   strategy='check'     Detect changes by comparing check_cols (vs. 'timestamp' using an updated_at).
--   check_cols           Columns whose change triggers a new version (segment, region).
--   dbt_valid_from/to    Effective-dating columns dbt maintains; valid_to IS NULL = current version.
--   {{ source(...) }}    Resolves a declared raw source table (see models/sources.yml).
--   Jinja {{ }} / {% %}  dbt's templating: {{ }} inserts values, {% %} controls logic/blocks.
--   SCD Type 2           Versioned dimension rows preserving history; same result as the Spark MERGE.
-- ================================================================================================
{% endraw %}
