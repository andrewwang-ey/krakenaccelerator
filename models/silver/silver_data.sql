{% set col_map    = var('column_map') %}
{% set transforms = var('transformations', {}) %}
{% set source_cols = col_map.values() | list %}

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
FROM {{ ref('bronze_data') }}
WHERE 1=1
{%- if var('active_record_filter', {}) %}
  AND "{{ var('active_record_filter')['field'] }}" {{ var('active_record_filter')['condition'] }}
{%- endif %}
{%- for field, value in var('filters', {}).items() %}
  AND "{{ field }}" = '{{ value }}'
{%- endfor %}
