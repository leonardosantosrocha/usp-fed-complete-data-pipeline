SELECT 1
FROM (
    SELECT COUNT(*) AS total_rows
    FROM {{ ref('dim_income_brackets') }}
) counts
WHERE total_rows != 6
