-- Staging model for credits
-- Separates cast and crew

WITH source AS (
    SELECT * FROM TMDB_DW.RAW.raw_credits
),

cleaned AS (
    SELECT
        credit_id,
        movie_id,
        person_id,
        TRIM(name) AS person_name,
        TRIM(character) AS character_name,
        TRIM(job) AS job_title,
        TRIM(department) AS department,
        credit_type,
        gender,
        cast_order,
        
        -- Flag key roles
        CASE WHEN job_title = 'Director' THEN TRUE ELSE FALSE END AS is_director,
        CASE WHEN credit_type = 'cast' AND cast_order <= 5 THEN TRUE ELSE FALSE END AS is_lead_actor,
        
        loaded_at
        
    FROM source
    WHERE credit_id IS NOT NULL
      AND movie_id IS NOT NULL
)

SELECT * FROM cleaned