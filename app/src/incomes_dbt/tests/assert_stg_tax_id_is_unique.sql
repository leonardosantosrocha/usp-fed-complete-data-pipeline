SELECT 1
FROM {{ ref('stg_tax_data') }}
GROUP BY tax_id
HAVING COUNT(*) > 1
