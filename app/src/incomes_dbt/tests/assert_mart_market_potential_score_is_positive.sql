SELECT 1
FROM {{ ref('mart_market_state_summary') }}
WHERE market_potential_score IS NOT NULL
  AND market_potential_score <= 0
