-- ============================================================================
-- SNOWFLAKE DATABASE SETUP
-- ============================================================================
-- Run this in Snowflake web UI or SnowSQL
-- ============================================================================

-- Create database
CREATE DATABASE IF NOT EXISTS TMDB_DW;
USE DATABASE TMDB_DW;

-- Create schemas
CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS STAGING;
CREATE SCHEMA IF NOT EXISTS ANALYTICS;

-- Create warehouse
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE;

USE WAREHOUSE COMPUTE_WH;

-- ============================================================================
-- RAW SCHEMA TABLES
-- ============================================================================

USE SCHEMA RAW;

-- Raw movies table
CREATE OR REPLACE TABLE raw_movies (
    id INTEGER PRIMARY KEY,
    title VARCHAR(500),
    original_title VARCHAR(500),
    overview TEXT,
    release_date DATE,
    popularity FLOAT,
    vote_average FLOAT,
    vote_count INTEGER,
    budget BIGINT,
    revenue BIGINT,
    runtime INTEGER,
    status VARCHAR(50),
    tagline TEXT,
    poster_path VARCHAR(200),
    backdrop_path VARCHAR(200),
    genres VARIANT,
    production_companies VARIANT,
    production_countries VARIANT,
    spoken_languages VARIANT,
    keywords VARIANT,
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Raw credits table
CREATE OR REPLACE TABLE raw_credits (
    credit_id VARCHAR(50) PRIMARY KEY,
    movie_id INTEGER,
    person_id INTEGER,
    name VARCHAR(200),
    character VARCHAR(500),
    job VARCHAR(100),
    department VARCHAR(100),
    credit_type VARCHAR(20),
    gender INTEGER,
    profile_path VARCHAR(200),
    cast_order INTEGER,
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Raw genres table
CREATE OR REPLACE TABLE raw_genres (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100),
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Raw companies table
CREATE OR REPLACE TABLE raw_companies (
    id INTEGER PRIMARY KEY,
    name VARCHAR(200),
    origin_country VARCHAR(10),
    logo_path VARCHAR(200),
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Grant permissions
GRANT USAGE ON DATABASE TMDB_DW TO ROLE ACCOUNTADMIN;
GRANT USAGE ON ALL SCHEMAS IN DATABASE TMDB_DW TO ROLE ACCOUNTADMIN;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA RAW TO ROLE ACCOUNTADMIN;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA STAGING TO ROLE ACCOUNTADMIN;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA ANALYTICS TO ROLE ACCOUNTADMIN;

-- ============================================================================
-- VERIFICATION
-- ============================================================================

-- Check tables created
SHOW TABLES IN SCHEMA RAW;

-- Check schemas
SHOW SCHEMAS IN DATABASE TMDB_DW;

SELECT 'Snowflake setup completed!' AS status;