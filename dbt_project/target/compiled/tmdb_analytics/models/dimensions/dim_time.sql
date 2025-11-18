-- Dimension table for time
-- Pre-populated date dimension



WITH date_spine AS (
    -- Generate dates from 1900 to 2030
    SELECT
        DATEADD(DAY, SEQ4(), '1900-01-01'::DATE) AS date_day
    FROM TABLE(GENERATOR(ROWCOUNT => 47482))
),

final AS (
    SELECT
        TO_NUMBER(TO_CHAR(date_day, 'YYYYMMDD')) AS date_key,
        date_day AS full_date,
        YEAR(date_day) AS year,
        QUARTER(date_day) AS quarter,
        MONTH(date_day) AS month,
        MONTHNAME(date_day) AS month_name,
        DAY(date_day) AS day,
        DAYOFWEEK(date_day) AS day_of_week,
        DAYNAME(date_day) AS day_name,
        WEEKOFYEAR(date_day) AS week_of_year,
        FLOOR(YEAR(date_day) / 10) * 10 AS decade,
        
        CASE 
            WHEN DAYOFWEEK(date_day) IN (0, 6) THEN TRUE 
            ELSE FALSE 
        END AS is_weekend,
        
        CASE 
            WHEN MONTH(date_day) IN (6, 7, 8) THEN 'Summer'
            WHEN MONTH(date_day) IN (9, 10, 11) THEN 'Fall'
            WHEN MONTH(date_day) IN (12, 1, 2) THEN 'Winter'
            ELSE 'Spring'
        END AS season
        
    FROM date_spine
)

SELECT * FROM final