SELECT
    ADDRESS_ID,
    PARTY_ID,
    ADDRESS_TYPE,
    ADDRESS_LINE_1,
    ADDRESS_LINE_2,
    ADDRESS_LINE_3,
    CITY,
    CASE STATE
        WHEN 'Victoria'                     THEN 'VIC'
        WHEN 'New South Wales'              THEN 'NSW'
        WHEN 'Queensland'                   THEN 'QLD'
        WHEN 'South Australia'              THEN 'SA'
        WHEN 'Western Australia'            THEN 'WA'
        WHEN 'Tasmania'                     THEN 'TAS'
        WHEN 'Northern Territory'           THEN 'NT'
        WHEN 'Australian Capital Territory' THEN 'ACT'
        ELSE STATE
    END AS STATE,
    CAST(POSTCODE AS VARCHAR) AS POSTCODE,
    COUNTRY_CODE,
    START_DATE,
    END_DATE,
    IS_PRIMARY
FROM {{ ref('bronze_address') }}
WHERE ADDRESS_TYPE = '{{ var("address_type") }}'
  AND IS_PRIMARY   = '{{ var("is_primary") }}'
  AND END_DATE IS NULL
  AND STATE IN ('{{ var("state") }}')
