SELECT
    *,
    CASE
{%- for field in var('required_fields') %}
        WHEN "{{ field }}" IS NULL THEN 'Missing {{ field }}'
{%- endfor %}
        ELSE 'Unknown'
    END AS rejection_reason
FROM {{ ref('landing_data') }}
WHERE 1=1
{%- for field in var('required_fields') %}
    OR "{{ field }}" IS NULL
{%- endfor %}