"""
============================================================================
MONGODB TO SNOWFLAKE SYNC
============================================================================
Syncs data from MongoDB Atlas to Snowflake RAW schema.
============================================================================
"""

import snowflake.connector
from pymongo import MongoClient
import pandas as pd
import os
from dotenv import load_dotenv
import json

load_dotenv()


class MongoToSnowflake:
    """
    Sync data from MongoDB to Snowflake.
    """
    
    def __init__(self):
        # MongoDB connection
        self.mongo_client = MongoClient(os.getenv('MONGO_URI'))
        self.mongo_db = self.mongo_client[os.getenv('MONGO_DB_NAME')]
        
        # Snowflake connection
        self.sf_conn = snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema='RAW'
        )
        self.sf_cursor = self.sf_conn.cursor()
    
    def sync_movies(self):
        """Sync movies from MongoDB to Snowflake"""
        print("\n📥 Syncing movies...")
        
        movies = list(self.mongo_db.raw_movies.find({}, {'_id': 0}))
        
        if not movies:
            print("  No movies found")
            return
        
        df = pd.DataFrame(movies)
        
        # Handle JSON fields
        json_cols = ['genres', 'production_companies', 'production_countries', 
                     'spoken_languages', 'keywords']
        
        for col in json_cols:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: json.dumps(x) if pd.notna(x) else None)
        
        # Clear and load
        self.sf_cursor.execute("TRUNCATE TABLE raw_movies")
        
        for _, row in df.iterrows():
            insert_query = """
            INSERT INTO raw_movies (
                id, title, original_title, overview, release_date, popularity,
                vote_average, vote_count, budget, revenue, runtime, status,
                tagline, poster_path, backdrop_path, genres, production_companies,
                production_countries, spoken_languages, keywords
            ) VALUES (
                %(id)s, %(title)s, %(original_title)s, %(overview)s, %(release_date)s,
                %(popularity)s, %(vote_average)s, %(vote_count)s, %(budget)s, %(revenue)s,
                %(runtime)s, %(status)s, %(tagline)s, %(poster_path)s, %(backdrop_path)s,
                PARSE_JSON(%(genres)s), PARSE_JSON(%(production_companies)s),
                PARSE_JSON(%(production_countries)s), PARSE_JSON(%(spoken_languages)s),
                PARSE_JSON(%(keywords)s)
            )
            """
            
            try:
                self.sf_cursor.execute(insert_query, row.to_dict())
            except Exception as e:
                print(f"  Error: {str(e)[:100]}")
                continue
        
        self.sf_conn.commit()
        print(f"  ✓ Synced {len(df)} movies")
    
    def sync_credits(self):
        """Sync credits from MongoDB to Snowflake"""
        print("\n📥 Syncing credits...")
        
        credits = list(self.mongo_db.raw_credits.find({}, {'_id': 0}))
        
        if not credits:
            print("  No credits found")
            return
        
        df = pd.DataFrame(credits)
        
        self.sf_cursor.execute("TRUNCATE TABLE raw_credits")
        
        for _, row in df.iterrows():
            insert_query = """
            INSERT INTO raw_credits (
                credit_id, movie_id, person_id, name, character, job, department,
                credit_type, gender, profile_path, cast_order
            ) VALUES (
                %(credit_id)s, %(movie_id)s, %(id)s, %(name)s, %(character)s,
                %(job)s, %(department)s, %(credit_type)s, %(gender)s,
                %(profile_path)s, %(order)s
            )
            """
            
            params = {
                'credit_id': row.get('credit_id'),
                'movie_id': row.get('movie_id'),
                'id': row.get('id'),
                'name': row.get('name'),
                'character': row.get('character'),
                'job': row.get('job'),
                'department': row.get('department'),
                'credit_type': row.get('credit_type'),
                'gender': row.get('gender'),
                'profile_path': row.get('profile_path'),
                'order': row.get('order')
            }
            
            try:
                self.sf_cursor.execute(insert_query, params)
            except Exception as e:
                continue
        
        self.sf_conn.commit()
        print(f"  ✓ Synced {len(df)} credits")
    
    def sync_genres(self):
        """Sync genres from MongoDB to Snowflake"""
        print("\n📥 Syncing genres...")
        
        genres = list(self.mongo_db.raw_genres.find({}, {'_id': 0}))
        
        if not genres:
            return
        
        df = pd.DataFrame(genres)
        
        self.sf_cursor.execute("TRUNCATE TABLE raw_genres")
        
        for _, row in df.iterrows():
            self.sf_cursor.execute(
                "INSERT INTO raw_genres (id, name) VALUES (%s, %s)",
                (row['id'], row['name'])
            )
        
        self.sf_conn.commit()
        print(f"  ✓ Synced {len(df)} genres")
    
    def sync_companies(self):
        """Sync companies from MongoDB to Snowflake"""
        print("\n📥 Syncing companies...")
        
        companies = list(self.mongo_db.raw_companies.find({}, {'_id': 0}))
        
        if not companies:
            return
        
        df = pd.DataFrame(companies)
        
        self.sf_cursor.execute("TRUNCATE TABLE raw_companies")
        
        for _, row in df.iterrows():
            self.sf_cursor.execute(
                "INSERT INTO raw_companies (id, name, origin_country, logo_path) VALUES (%s, %s, %s, %s)",
                (row.get('id'), row.get('name'), row.get('origin_country'), row.get('logo_path'))
            )
        
        self.sf_conn.commit()
        print(f"  ✓ Synced {len(df)} companies")
    
    def close(self):
        """Close connections"""
        self.sf_cursor.close()
        self.sf_conn.close()
        self.mongo_client.close()