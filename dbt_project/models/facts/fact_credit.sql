-- Fact table for movie credits
-- Grain: One row per person per movie per role

{{ config(
    materialized='table',
    unique_key='credit_key'
) }}

WITH credits AS (
    SELECT * FROM {{ ref('stg_credits') }}
),

final AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY c.credit_id) AS credit_key,
        c.credit_id,
        
        -- Foreign keys
        fm.movie_key,
        dp.person_key,
        
        -- Credit details
        c.credit_type,
        c.character_name,
        c.job_title,
        c.department,
        c.cast_order,
        
        -- Flags
        c.is_director,
        c.is_lead_actor,
        
        -- Metadata
        CURRENT_TIMESTAMP() AS created_at,
        CURRENT_TIMESTAMP() AS updated_at
        
    FROM credits c
    LEFT JOIN {{ ref('fact_movie') }} fm ON c.movie_id = fm.movie_id
    LEFT JOIN {{ ref('dim_person') }} dp ON c.person_id = dp.person_id
    WHERE fm.movie_key IS NOT NULL
      AND dp.person_key IS NOT NULL
)

SELECT * FROM final