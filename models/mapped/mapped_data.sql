{% set col_map    = var('column_map') %}
{% set transforms = var('transformations', {}) %}
{% set source_cols = col_map.values() | select('ne', None) | list %}

SELECT
{%- if source_cols %}
{%- for source_col in source_cols %}
  {%- if source_col in transforms %}
    {%- set t = transforms[source_col] %}
    {%- if t['type'] == 'value_map' %}
  CASE "{{ source_col }}"
    {%- for from_val, to_val in t['values'].items() %}
    WHEN '{{ from_val }}' THEN '{{ to_val }}'
    {%- endfor %}
    ELSE "{{ source_col }}"
  END AS "{{ source_col }}"
    {%- endif %}
  {%- else %}
  "{{ source_col }}"
  {%- endif %}
  {%- if not loop.last %},{% endif %}
{%- endfor %}
{%- else %}
  *
{%- endif %}
FROM {{ ref('validated_data') }}
WHERE 1=1
{%- if var('active_record_filter', {}) %}
  AND "{{ var('active_record_filter')['field'] }}" {{ var('active_record_filter')['condition'] }}
{%- endif %}
{#
  Filter clause — supports two formats:
    Legacy dict   {"STATE": "VIC"}                 → equality filters
    New list      [{"field":"STATE","operator":"=","value":"VIC"}, ...]
                  → full operator support: =  !=  IN  NOT IN  IS NULL  IS NOT NULL  >  <  >=  <=
#}
{%- set filter_spec = var('filters', []) -%}
{%- if filter_spec is mapping -%}
  {# ── Legacy dict format ── #}
  {%- for field, value in filter_spec.items() %}
  AND "{{ field }}" = '{{ value }}'
  {%- endfor %}
{%- else -%}
  {# ── New list format ── #}
  {%- for f in filter_spec -%}
    {%- set op = f.operator | upper -%}
    {%- if op == '=' %}
  AND "{{ f.field }}" = '{{ f.value }}'
    {%- elif op == '!=' %}
  AND "{{ f.field }}" != '{{ f.value }}'
    {%- elif op == 'IN' %}
  AND "{{ f.field }}" IN ('{{ f.values | join("', '") }}')
    {%- elif op == 'NOT IN' %}
  AND "{{ f.field }}" NOT IN ('{{ f.values | join("', '") }}')
    {%- elif op == 'IS NULL' %}
  AND "{{ f.field }}" IS NULL
    {%- elif op == 'IS NOT NULL' %}
  AND "{{ f.field }}" IS NOT NULL
    {%- elif op in ('>', '<', '>=', '<=') %}
  AND "{{ f.field }}" {{ op }} '{{ f.value }}'
    {%- endif %}
  {%- endfor %}
{%- endif %}
