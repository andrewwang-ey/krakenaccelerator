{% set col_map = var('column_map') %}

SELECT
{%- for kraken_field, source_col in col_map.items() %}
  "{{ source_col }}" AS {{ kraken_field }}
  {%- if not loop.last %},{% endif %}
{%- endfor %}
FROM {{ ref('silver_data') }}
LIMIT {{ var('row_limit', 25) }}
