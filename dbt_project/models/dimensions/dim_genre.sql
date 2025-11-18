-- Dimension table for genres
-- SCD Type 1 (overwrite)

{{ config(
    materialized='table',
    unique_key='genre_key'
) }}

WITH genres AS (
    SELECT * FROM {{ ref('stg_genres') }}
),

final AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY genre_id) AS genre_key,
        genre_id,
        genre_name,
        CURRENT_TIMESTAMP() AS created_at,
        CURRENT_TIMESTAMP() AS updated_at
    FROM genres
)

SELECT * FROM final