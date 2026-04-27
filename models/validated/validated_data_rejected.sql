SELECT
    *,
    CASE
{%- if var('required_fields') %}
{%- for field in var('required_fields') %}
        WHEN "{{ field }}" IS NULL THEN 'Missing {{ field }}'
{%- endfor %}
{%- else %}
        WHEN 1=0 THEN 'No required fields configured'
{%- endif %}
        ELSE 'Unknown'
    END AS rejection_reason
FROM {{ ref('landing_data') }}
WHERE
{%- if var('required_fields') %}
{%- for field in var('required_fields') %}
    {%- if not loop.first %} OR {% endif %}"{{ field }}" IS NULL
{%- endfor %}
{%- else %}
    1=0
{%- endif %}

