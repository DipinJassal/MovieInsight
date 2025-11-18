-- ============================================================================
-- TABLEAU VIEWS FOR ANALYTICS
-- ============================================================================
-- Run these in Snowflake after dbt models complete
-- ============================================================================

USE DATABASE TMDB_DW;
USE SCHEMA ANALYTICS;

-- View 1: Genre Performance Summary
CREATE OR REPLACE VIEW vw_genre_performance AS
SELECT
    g.genre_name,
    t.decade,
    COUNT(DISTINCT fm.movie_key) AS movie_count,
    AVG(fm.budget) AS avg_budget,
    AVG(fm.revenue) AS avg_revenue,
    AVG(fm.profit) AS avg_profit,
    AVG(fm.roi_percentage) AS avg_roi,
    AVG(fm.vote_average) AS avg_rating,
    SUM(fm.revenue) AS total_revenue,
    SUM(fm.budget) AS total_budget
FROM fact_movie fm
JOIN dim_genre g ON fm.genre_key = g.genre_key
JOIN dim_time t ON fm.release_date_key = t.date_key
WHERE fm.budget > 0 AND fm.revenue > 0
GROUP BY g.genre_name, t.decade;

-- View 2: Director Performance
CREATE OR REPLACE VIEW vw_director_performance AS
SELECT
    dp.person_name AS director_name,
    COUNT(DISTINCT fm.movie_key) AS movies_directed,
    AVG(fm.revenue) AS avg_revenue,
    AVG(fm.roi_percentage) AS avg_roi,
    AVG(fm.vote_average) AS avg_rating,
    SUM(fm.revenue) AS total_revenue,
    MAX(fm.revenue) AS highest_grossing_movie_revenue,
    MIN(t.year) AS first_movie_year,
    MAX(t.year) AS latest_movie_year
FROM fact_credit fc
JOIN fact_movie fm ON fc.movie_key = fm.movie_key
JOIN dim_person dp ON fc.person_key = dp.person_key
JOIN dim_time t ON fm.release_date_key = t.date_key
WHERE fc.is_director = TRUE
  AND fm.revenue > 0
GROUP BY dp.person_name
HAVING COUNT(DISTINCT fm.movie_key) >= 3;

-- View 3: Monthly Release Patterns
CREATE OR REPLACE VIEW vw_release_patterns AS
SELECT
    t.month,
    t.month_name,
    g.genre_name,
    COUNT(DISTINCT fm.movie_key) AS movie_count,
    AVG(fm.revenue) AS avg_revenue,
    AVG(fm.vote_average) AS avg_rating
FROM fact_movie fm
JOIN dim_time t ON fm.release_date_key = t.date_key
JOIN dim_genre g ON fm.genre_key = g.genre_key
WHERE fm.revenue > 0
GROUP BY t.month, t.month_name, g.genre_name;

-- View 4: Budget vs Revenue Analysis
CREATE OR REPLACE VIEW vw_budget_revenue_analysis AS
SELECT
    fm.budget_category,
    g.genre_name,
    t.decade,
    COUNT(DISTINCT fm.movie_key) AS movie_count,
    AVG(fm.budget) AS avg_budget,
    AVG(fm.revenue) AS avg_revenue,
    AVG(fm.roi_percentage) AS avg_roi
FROM fact_movie fm
JOIN dim_genre g ON fm.genre_key = g.genre_key
JOIN dim_time t ON fm.release_date_key = t.date_key
WHERE fm.budget > 0 AND fm.revenue > 0
GROUP BY fm.budget_category, g.genre_name, t.decade;

-- View 5: Top Performing Movies
CREATE OR REPLACE VIEW vw_top_movies AS
SELECT
    fm.title,
    g.genre_name,
    c.company_name,
    t.year AS release_year,
    fm.budget,
    fm.revenue,
    fm.profit,
    fm.roi_percentage,
    fm.vote_average,
    fm.vote_count,
    fm.popularity
FROM fact_movie fm
JOIN dim_genre g ON fm.genre_key = g.genre_key
JOIN dim_company c ON fm.company_key = c.company_key
JOIN dim_time t ON fm.release_date_key = t.date_key
WHERE fm.revenue > 0
ORDER BY fm.revenue DESC
LIMIT 1000;

-- View 6: Actor Performance Metrics
CREATE OR REPLACE VIEW vw_actor_performance AS
SELECT
    dp.person_name AS actor_name,
    dp.gender_description,
    COUNT(DISTINCT fm.movie_key) AS movies_count,
    AVG(fm.revenue) AS avg_movie_revenue,
    AVG(fm.vote_average) AS avg_movie_rating,
    SUM(fm.revenue) AS total_box_office,
    COUNT(DISTINCT CASE WHEN fc.is_lead_actor THEN fm.movie_key END) AS lead_roles_count
FROM fact_credit fc
JOIN fact_movie fm ON fc.movie_key = fm.movie_key
JOIN dim_person dp ON fc.person_key = dp.person_key
WHERE fc.credit_type = 'cast'
  AND fm.revenue > 0
GROUP BY dp.person_name, dp.gender_description
HAVING COUNT(DISTINCT fm.movie_key) >= 5;

-- View 7: Production Company Rankings
CREATE OR REPLACE VIEW vw_company_rankings AS
SELECT
    c.company_name,
    c.origin_country,
    COUNT(DISTINCT fm.movie_key) AS movies_produced,
    AVG(fm.budget) AS avg_budget,
    AVG(fm.revenue) AS avg_revenue,
    AVG(fm.roi_percentage) AS avg_roi,
    SUM(fm.revenue) AS total_revenue,
    AVG(fm.vote_average) AS avg_rating
FROM fact_movie fm
JOIN dim_company c ON fm.company_key = c.company_key
WHERE fm.revenue > 0
GROUP BY c.company_name, c.origin_country
HAVING COUNT(DISTINCT fm.movie_key) >= 5
ORDER BY total_revenue DESC;

-- Verify views created
SHOW VIEWS IN SCHEMA ANALYTICS;

SELECT 'Tableau views created successfully!' AS status;