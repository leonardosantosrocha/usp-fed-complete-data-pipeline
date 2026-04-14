{{ config(materialized='table')}}

with raw_seed as (
    select * from {{ ref('states') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['state_code']) }} as state_key,
    state_code,
    state_name,
    region_name
from raw_seed