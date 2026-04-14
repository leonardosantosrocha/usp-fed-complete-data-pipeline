SELECT 1
FROM {{ ref('stg_tax_data') }}
 WHERE NOT agi_stub BETWEEN 1 AND 6