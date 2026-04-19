{{ config(severity = 'warn') }}

WITH brackets AS (
    SELECT *
      FROM {{ ref('dim_income_brackets') }}
),

facts AS (
    SELECT *
    FROM {{ ref('fact_income_by_state_bracket') }}
)

SELECT COUNT(*) AS total_rows
  FROM facts
 INNER JOIN brackets ON facts.income_bracket_key = brackets.income_bracket_key
  WHERE facts.total_income < brackets.income_bracket_min AND facts.total_income >= brackets.income_bracket_max