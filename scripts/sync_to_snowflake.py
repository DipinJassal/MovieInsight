"""
Simple MongoDB to Snowflake sync
Stores JSON as strings, lets dbt handle parsing
"""
import sys
sys.path.insert(0, 'airflow/utils')

import snowflake.connector
from pymongo import MongoClient
import os
from dotenv import load_dotenv
import json

load_dotenv()

print("="*70)
print(" SIMPLE MONGODB TO SNOWFLAKE SYNC ".center(70))
print("="*70)

# Connect
print("\nConnecting...")
mongo_client = MongoClient(os.getenv('MONGO_URI'))
mongo_db = mongo_client[os.getenv('MONGO_DB_NAME')]

sf_conn = snowflake.connector.connect(
    user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'),
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
    database=os.getenv('SNOWFLAKE_DATABASE'),
    schema='RAW'
)
sf_cursor = sf_conn.cursor()
print("✓ Connected\n")

print("Recreating tables...")

sf_cursor.execute("DROP TABLE IF EXISTS raw_movies")
sf_cursor.execute("""
CREATE TABLE raw_movies (
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
    genres VARCHAR(5000),
    production_companies VARCHAR(5000),
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
""")

sf_cursor.execute("DROP TABLE IF EXISTS raw_credits")
sf_cursor.execute("""
CREATE TABLE raw_credits (
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
)
""")

print("✓ Tables recreated\n")

# ============================================================================
# 1. GENRES
# ============================================================================
print("="*70)
print("1. SYNCING GENRES")
print("="*70)

genres = list(mongo_db.raw_genres.find({}, {'_id': 0}))
print(f"Found {len(genres)} genres")

sf_cursor.execute("TRUNCATE TABLE raw_genres")
for genre in genres:
    sf_cursor.execute(
        "INSERT INTO raw_genres (id, name) VALUES (%s, %s)",
        (genre['id'], genre['name'])
    )
sf_conn.commit()
print(f"✓ {len(genres)} genres\n")

# ============================================================================
# 2. COMPANIES
# ============================================================================
print("="*70)
print("2. SYNCING COMPANIES")
print("="*70)

companies = list(mongo_db.raw_companies.find({}, {'_id': 0}))
print(f"Found {len(companies)} companies")

sf_cursor.execute("TRUNCATE TABLE raw_companies")
success = 0
for company in companies:
    try:
        sf_cursor.execute(
            "INSERT INTO raw_companies (id, name, origin_country, logo_path) VALUES (%s, %s, %s, %s)",
            (company.get('id'), company.get('name'), company.get('origin_country'), company.get('logo_path'))
        )
        success += 1
    except:
        continue

sf_conn.commit()
print(f"✓ {success} companies\n")

# ============================================================================
# 3. MOVIES (Store JSON as strings)
# ============================================================================
print("="*70)
print("3. SYNCING MOVIES")
print("="*70)

movies = list(mongo_db.raw_movies.find({}, {'_id': 0}))
print(f"Found {len(movies)} movies")
print("This will take 5-10 minutes...\n")

success = 0
for i, movie in enumerate(movies, 1):
    if i % 100 == 0:
        print(f"  Progress: {i}/{len(movies)} ({i/len(movies)*100:.1f}%)")
    
    try:
        # Convert to JSON strings (not parsed JSON)
        genres_str = json.dumps(movie.get('genres', []))
        companies_str = json.dumps(movie.get('production_companies', []))
        
        sf_cursor.execute("""
            INSERT INTO raw_movies (
                id, title, original_title, overview, release_date,
                popularity, vote_average, vote_count, budget, revenue,
                runtime, status, tagline, poster_path, backdrop_path,
                genres, production_companies
            ) VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s
            )
        """, (
            movie.get('id'),
            movie.get('title'),
            movie.get('original_title'),
            movie.get('overview'),
            movie.get('release_date'),
            movie.get('popularity'),
            movie.get('vote_average'),
            movie.get('vote_count'),
            movie.get('budget', 0),
            movie.get('revenue', 0),
            movie.get('runtime'),
            movie.get('status'),
            movie.get('tagline'),
            movie.get('poster_path'),
            movie.get('backdrop_path'),
            genres_str,
            companies_str
        ))
        
        success += 1
        
        if i % 50 == 0:
            sf_conn.commit()
            
    except Exception as e:
        if i < 5:
            print(f"  Error: {str(e)[:100]}")
        continue

sf_conn.commit()
print(f"\n✓ {success} movies\n")

# ============================================================================
# 4. CREDITS
# ============================================================================
print("="*70)
print("4. SYNCING CREDITS")
print("="*70)

credits = list(mongo_db.raw_credits.find({}, {'_id': 0}))
print(f"Found {len(credits)} credits")
print("This will take 10-20 minutes...\n")

success = 0
for i in range(0, len(credits), 1000):
    batch = credits[i:i+1000]
    
    for credit in batch:
        try:
            sf_cursor.execute("""
                INSERT INTO raw_credits (
                    credit_id, movie_id, person_id, name, character,
                    job, department, credit_type, gender, profile_path, cast_order
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                credit.get('credit_id'),
                credit.get('movie_id'),
                credit.get('id'),
                credit.get('name'),
                credit.get('character'),
                credit.get('job'),
                credit.get('department'),
                credit.get('credit_type'),
                credit.get('gender'),
                credit.get('profile_path'),
                credit.get('order')
            ))
            success += 1
        except:
            continue
    
    sf_conn.commit()
    progress = min(i + 1000, len(credits))
    print(f"  Progress: {progress:,}/{len(credits):,} ({progress/len(credits)*100:.1f}%)")

print(f"\n✓ {success:,} credits\n")

# Close
sf_cursor.close()
sf_conn.close()
mongo_client.close()

print("="*70)
print(" ✅ SYNC COMPLETE ".center(70, "="))
print("="*70)

# Verify
sf_conn = snowflake.connector.connect(
    user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'),
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
    database=os.getenv('SNOWFLAKE_DATABASE'),
    schema='RAW'
)
sf_cursor = sf_conn.cursor()

sf_cursor.execute("SELECT COUNT(*) FROM raw_movies")
m = sf_cursor.fetchone()[0]
sf_cursor.execute("SELECT COUNT(*) FROM raw_credits")
c = sf_cursor.fetchone()[0]
sf_cursor.execute("SELECT COUNT(*) FROM raw_genres")
g = sf_cursor.fetchone()[0]
sf_cursor.execute("SELECT COUNT(*) FROM raw_companies")
co = sf_cursor.fetchone()[0]

print(f"\nSnowflake Data:")
print(f"  Movies:    {m:,}")
print(f"  Credits:   {c:,}")
print(f"  Genres:    {g}")
print(f"  Companies: {co:,}")

sf_cursor.close()
sf_conn.close()

print("\n✅ Ready for dbt!")
print("  cd dbt_project")
print("  dbt run --profiles-dir .")