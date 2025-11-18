-- Dimension table for people (actors, directors, crew)
-- SCD Type 1

{{ config(
    materialized='table',
    unique_key='person_key'
) }}

WITH people AS (
    SELECT DISTINCT
        person_id,
        person_name,
        gender
    FROM {{ ref('stg_credits') }}
    WHERE person_id IS NOT NULL
),

final AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY person_id) AS person_key,
        person_id,
        person_name,
        CASE gender
            WHEN 0 THEN 'Not specified'
            WHEN 1 THEN 'Female'
            WHEN 2 THEN 'Male'
            WHEN 3 THEN 'Non-binary'
            ELSE 'Unknown'
        END AS gender_description,
        CURRENT_TIMESTAMP() AS created_at,
        CURRENT_TIMESTAMP() AS updated_at
    FROM people
)

SELECT * FROM final