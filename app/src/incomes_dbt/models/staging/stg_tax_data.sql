WITH source AS (
    SELECT * FROM {{ source('income_tax_raw', 'tb_staging_individual_income_tax_2014')}}
),

renamed AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'zipcode',
            'state',
            'agi_stub'
        ]) }} AS tax_id,

        CAST(agi_stub AS INTEGER) AS agi_stub,
        UPPER(TRIM("state")) AS state_code,
        CAST("year" AS INTEGER) AS tax_year,
        CAST("n1" AS INTEGER) AS num_returns,
        CAST("a00100" AS NUMERIC(18,2)) AS total_income,
        CAST("a06500" AS NUMERIC(18,2)) AS total_tax
    FROM source
)

SELECT * FROM renamed