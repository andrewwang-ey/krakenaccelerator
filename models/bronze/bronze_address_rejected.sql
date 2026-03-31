SELECT
    *,
    CASE
        WHEN ADDRESS_ID   IS NULL                               THEN 'Missing ADDRESS_ID'
        WHEN PARTY_ID     IS NULL                               THEN 'Missing PARTY_ID'
        WHEN ADDRESS_TYPE IS NULL                               THEN 'Missing ADDRESS_TYPE'
        WHEN LENGTH(CAST(POSTCODE AS VARCHAR)) != 4             THEN 'Invalid POSTCODE'
        WHEN END_DATE IS NOT NULL AND END_DATE <= CURRENT_DATE  THEN 'Address expired'
    END AS rejection_reason
FROM {{ ref('landing_address') }}
WHERE ADDRESS_ID   IS NULL
   OR PARTY_ID     IS NULL
   OR ADDRESS_TYPE IS NULL
   OR LENGTH(CAST(POSTCODE AS VARCHAR)) != 4
   OR (END_DATE IS NOT NULL AND END_DATE <= CURRENT_DATE)
