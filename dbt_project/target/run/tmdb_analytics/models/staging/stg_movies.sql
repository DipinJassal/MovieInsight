
  create or replace   view TMDB_DW.ANALYTICS_staging.stg_movies
  
   as (
    -- Staging model for movies
-- Cleans and standardizes raw movie data

WITH source AS (
    SELECT * FROM TMDB_DW.RAW.raw_movies
),

cleaned AS (
    SELECT
        id AS movie_id,
        TRIM(title) AS title,
        TRIM(original_title) AS original_title,
        overview,
        TRY_CAST(release_date AS DATE) AS release_date,
        popularity,
        vote_average,
        vote_count,
        COALESCE(budget, 0) AS budget,
        COALESCE(revenue, 0) AS revenue,
        runtime,
        status,
        tagline,
        
        -- Calculate derived metrics
        CASE 
            WHEN budget > 0 THEN ROUND((revenue - budget) / budget * 100, 2)
            ELSE NULL 
        END AS roi_percentage,
        
        CASE 
            WHEN revenue > 0 THEN revenue - budget
            ELSE NULL 
        END AS profit,
        
        -- Extract year and decade
        YEAR(release_date) AS release_year,
        FLOOR(YEAR(release_date) / 10) * 10 AS release_decade,
        
        -- Store JSON fields
        genres,
        production_companies,
        loaded_at
        
    FROM source
    WHERE id IS NOT NULL
      AND title IS NOT NULL
      AND release_date IS NOT NULL
)

SELECT * FROM cleaned
  );

