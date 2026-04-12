{{ config(materialized='table') }}

with raw_seed as (
    select * from {{ ref('brackets') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['agi_stub']) }} as income_bracket_key,
    agi_stub,
    income_bracket_label,
    income_bracket_min,
    income_bracket_max,
    income_bracket_order
from raw_seed