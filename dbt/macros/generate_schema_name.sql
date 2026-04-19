-- Controls how dbt names schemas in BigQuery.
-- Without this macro, dbt Cloud generates names like `smard_smard_staging`
-- (target_schema + custom_schema concatenated with underscore).
-- With this macro, it generates `smard_staging` and `smard_marts` as intended.

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ target.schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
