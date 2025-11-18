
  
    

        create or replace transient table TMDB_DW.ANALYTICS_analytics.dim_company
         as
        (-- Dimension table for production companies
-- SCD Type 1



WITH companies AS (
    SELECT * FROM TMDB_DW.RAW.raw_companies
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
        );
      
  