#!/usr/bin/env python3
"""
Standalone script for genre extraction - called by Airflow
"""
import sys
import os

# Add utils to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'airflow', 'utils'))

from tmdb_api import TMDBClient
from mongo_handler import MongoHandler

print("="*50)
print("GENRE EXTRACTION SCRIPT")
print("="*50)

try:
    print("Initializing clients...")
    tmdb = TMDBClient()
    mongo = MongoHandler()
    
    print("Fetching genres...")
    genres = tmdb.get_genres()
    print(f"Got {len(genres)} genres")
    
    print("Inserting to MongoDB...")
    stats = mongo.insert_genres(genres)
    print(f"Inserted: {stats['inserted']}")
    
    tmdb.close()
    mongo.close()
    
    print("="*50)
    print("SUCCESS")
    print("="*50)
    exit(0)
    
except Exception as e:
    print(f"ERROR: {str(e)}")
    import traceback
    traceback.print_exc()
    exit(1)