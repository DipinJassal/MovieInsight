-- Staging model for genres

WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_genres') }}
)

SELECT
    id AS genre_id,
    TRIM(name) AS genre_name,
    loaded_at
FROM source
WHERE id IS NOT NULL