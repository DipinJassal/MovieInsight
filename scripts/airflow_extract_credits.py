#!/usr/bin/env python3
import sys
import os
import json

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'airflow', 'utils'))

from tmdb_api import TMDBClient
from mongo_handler import MongoHandler

print("EXTRACTING CREDITS")
try:
    # Load movie IDs from previous task
    with open('/tmp/airflow_detailed_ids.json', 'r') as f:
        movie_ids = json.load(f)
    
    tmdb = TMDBClient()
    mongo = MongoHandler()
    
    total_credits = 0
    for i, movie_id in enumerate(movie_ids, 1):
        print(f"[{i}/{len(movie_ids)}] Movie {movie_id}")
        credits = tmdb.get_movie_credits(movie_id)
        if credits:
            stats = mongo.insert_credits(movie_id, credits)
            total_credits += stats['cast_count'] + stats['crew_count']
    
    print(f"SUCCESS: {total_credits} credits")
    
    tmdb.close()
    mongo.close()
    exit(0)
except Exception as e:
    print(f"ERROR: {str(e)}")
    import traceback
    traceback.print_exc()
    exit(1)