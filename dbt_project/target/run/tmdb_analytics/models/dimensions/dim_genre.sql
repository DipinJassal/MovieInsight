
  
    

        create or replace transient table TMDB_DW.ANALYTICS_analytics.dim_genre
         as
        (-- Dimension table for genres
-- SCD Type 1 (overwrite)



WITH genres AS (
    SELECT * FROM TMDB_DW.ANALYTICS_staging.stg_genres
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
        );
      
  