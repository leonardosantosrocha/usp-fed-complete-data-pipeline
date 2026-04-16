{{ config(materialized='table') }}

with fact_tax as (
    select * from {{ ref('fact_income_by_state_bracket') }}
),

dim_states as (
    select * from {{ ref('dim_states') }}
),

state_year_aggregates as (
    select
        f.state_key,
        f.tax_year,
        sum(f.num_returns) as total_returns,
        sum(f.total_income) as total_income,
        sum(f.total_tax) as total_tax
    from fact_tax f
    group by 1, 2
),

final as (
    select
        s.state_code,
        s.region_name,
        sa.tax_year,
        sa.total_returns,
        sa.total_income,
        sa.total_tax,

        CAST(sa.total_income / NULLIF(sa.total_returns, 0) AS NUMERIC(16,2)) as avg_income_per_return,
        CAST(sa.total_tax / NULLIF(sa.total_returns, 0) AS NUMERIC(16,2)) as avg_tax_per_return,

        CAST(sa.total_tax / NULLIF(sa.total_income, 0) AS NUMERIC(16,2)) as effective_tax_rate,

        CAST((sa.total_income / NULLIF(sa.total_returns, 0)) /
            NULLIF(log(sa.total_returns), 0) AS NUMERIC(16,2)) as market_potential_score

    from state_year_aggregates sa
    inner join dim_states s on sa.state_key = s.state_key
)

select * from final