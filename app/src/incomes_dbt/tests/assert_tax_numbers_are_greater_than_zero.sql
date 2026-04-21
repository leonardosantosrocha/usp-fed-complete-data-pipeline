SELECT 1
FROM {{ ref('stg_tax_data') }}
WHERE num_returns < 0 OR total_income < 0 OR total_tax < 0