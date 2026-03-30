SELECT
    PARTY_ID,
    ADDRESS_LINE_1 AS address1,
    ADDRESS_LINE_2 AS address2,
    CITY           AS city,
    STATE          AS state,
    POSTCODE       AS zipCode
FROM {{ ref('silver_address') }}
LIMIT 25
