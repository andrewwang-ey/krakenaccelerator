SELECT *
FROM {{ ref('landing_data') }}
WHERE 1=1
{%- for field in var('required_fields') %}
  AND "{{ field }}" IS NOT NULL
{%- endfor %}