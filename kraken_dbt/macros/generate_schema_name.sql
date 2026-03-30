-- By default dbt prefixes schema names with the target schema (e.g. main_landing).
-- This macro overrides that so schemas are clean: landing, bronze, silver, gold.
{% macro generate_schema_name(custom_schema_name, node) %}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{% endmacro %}