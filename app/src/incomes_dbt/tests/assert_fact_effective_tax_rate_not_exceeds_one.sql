{{ config(severity = 'warn') }}

SELECT 1
FROM {{ ref('fact_income_by_state_bracket') }}
WHERE effective_tax_rate > 1
