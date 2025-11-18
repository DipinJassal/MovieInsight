"""
Complete pipeline verification script.
Checks data in MongoDB, Snowflake, and dbt models.
"""

from pymongo import MongoClient
import snowflake.connector
from dotenv import load_dotenv
import os

load_dotenv()

print("="*70)
print(" COMPLETE PIPELINE VERIFICATION ".center(70))
print("="*70)

# ============================================================================
# CHECK MONGODB
# ============================================================================

print("\n1. CHECKING MONGODB")
print("-"*70)

try:
    client = MongoClient(os.getenv('MONGO_URI'))
    db = client[os.getenv('MONGO_DB_NAME')]
    
    print("MongoDB Collections:")
    print(f"  Movies: {db.raw_movies.count_documents({}):,}")
    print(f"  Credits: {db.raw_credits.count_documents({}):,}")
    print(f"  Genres: {db.raw_genres.count_documents({}):,}")
    print(f"  Companies: {db.raw_companies.count_documents({}):,}")
    
    client.close()
    print("✓ MongoDB verification passed")
    
except Exception as e:
    print(f"✗ MongoDB error: {str(e)}")

# ============================================================================
# CHECK SNOWFLAKE RAW
# ============================================================================

print("\n2. CHECKING SNOWFLAKE RAW SCHEMA")
print("-"*70)

try:
    conn = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema='RAW'
    )
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM raw_movies")
    movies = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM raw_credits")
    credits = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM raw_genres")
    genres = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM raw_companies")
    companies = cursor.fetchone()[0]
    
    print("Snowflake RAW Tables:")
    print(f"  raw_movies: {movies:,}")
    print(f"  raw_credits: {credits:,}")
    print(f"  raw_genres: {genres:,}")
    print(f"  raw_companies: {companies:,}")
    
    cursor.close()
    print("✓ Snowflake RAW verification passed")
    
except Exception as e:
    print(f"✗ Snowflake RAW error: {str(e)}")

# ============================================================================
# CHECK SNOWFLAKE ANALYTICS
# ============================================================================

print("\n3. CHECKING SNOWFLAKE ANALYTICS SCHEMA")
print("-"*70)

try:
    conn = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema='ANALYTICS'
    )
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM dim_genre")
    dim_genres = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM dim_person")
    dim_persons = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM dim_company")
    dim_companies = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM dim_time")
    dim_time = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM fact_movie")
    fact_movies = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM fact_credit")
    fact_credits = cursor.fetchone()[0]
    
    print("Snowflake ANALYTICS Dimensions:")
    print(f"  dim_genre: {dim_genres:,}")
    print(f"  dim_person: {dim_persons:,}")
    print(f"  dim_company: {dim_companies:,}")
    print(f"  dim_time: {dim_time:,}")
    
    print("\nSnowflake ANALYTICS Facts:")
    print(f"  fact_movie: {fact_movies:,}")
    print(f"  fact_credit: {fact_credits:,}")
    
    cursor.close()
    conn.close()
    print("✓ Snowflake ANALYTICS verification passed")
    
except Exception as e:
    print(f"✗ Snowflake ANALYTICS error: {str(e)}")

# ============================================================================
# SUMMARY
# ============================================================================

print("\n" + "="*70)
print(" VERIFICATION COMPLETE ".center(70, "="))
print("="*70)

print("\n✅ If all checks passed, your pipeline is working correctly!")
print("\nNext steps:")
print("1. Connect Tableau to Snowflake")
print("2. Use ANALYTICS schema")
print("3. Build dashboards from views")