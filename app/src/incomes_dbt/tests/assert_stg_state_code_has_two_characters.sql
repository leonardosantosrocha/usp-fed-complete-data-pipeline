SELECT 1
FROM {{ ref('stg_tax_data') }}
WHERE LENGTH(state_code) != 2
