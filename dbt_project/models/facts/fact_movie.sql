{{
    config(
        materialized='table'
    )
}}

WITH movies AS (
    SELECT * FROM {{ ref('stg_movies') }}
),

genres AS (
    SELECT * FROM {{ ref('dim_genre') }}
),

companies AS (
    SELECT * FROM {{ ref('dim_company') }}
),

time_dim AS (
    SELECT * FROM {{ ref('dim_time') }}
),

-- Extract primary genre from JSON string
movie_genres AS (
    SELECT
        m.movie_id,
        TRY_PARSE_JSON(m.genres) AS genres_json
    FROM movies m
),

primary_genre AS (
    SELECT
        movie_id,
        genres_json[0]:id::INTEGER AS genre_id
    FROM movie_genres
    WHERE genres_json IS NOT NULL
    AND ARRAY_SIZE(genres_json) > 0
),

-- Extract primary company from JSON string
movie_companies AS (
    SELECT
        m.movie_id,
        TRY_PARSE_JSON(m.production_companies) AS companies_json
    FROM movies m
),

primary_company AS (
    SELECT
        movie_id,
        companies_json[0]:id::INTEGER AS company_id
    FROM movie_companies
    WHERE companies_json IS NOT NULL
    AND ARRAY_SIZE(companies_json) > 0
),

final AS (
    SELECT
        -- Surrogate key
        ROW_NUMBER() OVER (ORDER BY m.movie_id) AS movie_key,
        
        -- Natural key
        m.movie_id,
        
        -- Foreign keys
        COALESCE(g.genre_key, -1) AS genre_key,
        COALESCE(c.company_key, -1) AS company_key,
        COALESCE(t.date_key, -1) AS release_date_key,
        
        -- Descriptive attributes
        m.title,
        m.original_title,
        m.overview,
        m.release_year,
        m.release_decade,
        
        -- Measures
        m.budget,
        m.revenue,
        m.profit,
        m.roi_percentage,
        m.runtime,
        m.popularity,
        m.vote_average,
        m.vote_count,
        
        -- Categorizations
        CASE
            WHEN m.budget >= 100000000 THEN 'Blockbuster'
            WHEN m.budget >= 50000000 THEN 'Major'
            WHEN m.budget >= 10000000 THEN 'Medium'
            WHEN m.budget > 0 THEN 'Low Budget'
            ELSE 'Unknown'
        END AS budget_category,
        
        CASE
            WHEN m.vote_average >= 8.0 THEN 'Excellent'
            WHEN m.vote_average >= 7.0 THEN 'Good'
            WHEN m.vote_average >= 6.0 THEN 'Average'
            WHEN m.vote_average >= 5.0 THEN 'Below Average'
            ELSE 'Poor'
        END AS rating_category,
        
        -- Status flags
        CASE WHEN m.profit > 0 THEN TRUE ELSE FALSE END AS is_profitable,
        
        -- Timestamps
        CURRENT_TIMESTAMP() AS created_at,
        CURRENT_TIMESTAMP() AS updated_at
        
    FROM movies m
    LEFT JOIN primary_genre pg ON m.movie_id = pg.movie_id
    LEFT JOIN genres g ON pg.genre_id = g.genre_id
    LEFT JOIN primary_company pc ON m.movie_id = pc.movie_id
    LEFT JOIN companies c ON pc.company_id = c.company_id
    LEFT JOIN time_dim t ON m.release_date = t.full_date
)

SELECT * FROM final
