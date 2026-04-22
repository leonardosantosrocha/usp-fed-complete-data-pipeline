SELECT 1
FROM {{ ref('mart_market_state_summary') }}
WHERE total_returns <= 0
   OR total_income <= 0
   OR total_tax < 0
