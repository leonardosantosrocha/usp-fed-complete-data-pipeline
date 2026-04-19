{{ config(materialized='table') }}

with staging as (
    select * from {{ ref('stg_tax_data') }}
),

dim_states as (
    select * from {{ ref('dim_states') }}
),

dim_income as (
    select * from {{ ref('dim_income_brackets') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['stg.tax_id']) }} as fact_key,
        s.state_key,
        i.income_bracket_key,
        stg.num_returns,
        stg.total_income,
        stg.total_tax,
        stg.tax_year,

        CAST(stg.total_income / nullif(stg.num_returns, 0) AS NUMERIC(16,2)) as avg_income_per_return,
        CAST(stg.total_tax / nullif(stg.num_returns, 0) AS NUMERIC(16,2)) as avg_tax_per_return,
        CAST(stg.total_tax / nullif(stg.total_income, 0) AS NUMERIC(16,2)) as effective_tax_rate

    from staging stg
    inner join dim_states s on stg.state_code = s.state_code
    inner join dim_income i on stg.agi_stub = i.agi_stub
 where stg.total_income > 0
)

select * from final