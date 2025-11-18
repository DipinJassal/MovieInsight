-- Staging model for genres

WITH source AS (
    SELECT * FROM TMDB_DW.RAW.raw_genres
)

SELECT
    id AS genre_id,
    TRIM(name) AS genre_name,
    loaded_at
FROM source
WHERE id IS NOT NULL