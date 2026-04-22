SELECT 1
FROM {{ ref('dim_states') }}
WHERE region_name NOT IN ('South', 'West', 'Northeast', 'Midwest')
