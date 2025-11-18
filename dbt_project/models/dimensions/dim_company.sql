-- Dimension table for production companies
-- SCD Type 1

{{ config(
    materialized='table',
    unique_key='company_key'
) }}

WITH companies AS (
    SELECT * FROM {{ source('raw', 'raw_companies') }}
),

final AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY id) AS company_key,
        id AS company_id,
        name AS company_name,
        origin_country,
        CURRENT_TIMESTAMP() AS created_at,
        CURRENT_TIMESTAMP() AS updated_at
    FROM companies
    WHERE id IS NOT NULL
)

SELECT * FROM final