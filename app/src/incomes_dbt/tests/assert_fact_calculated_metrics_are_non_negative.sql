SELECT 1
FROM {{ ref('fact_income_by_state_bracket') }}
WHERE avg_income_per_return < 0
   OR avg_tax_per_return < 0
   OR effective_tax_rate < 0
