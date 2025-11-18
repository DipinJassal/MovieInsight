#!/usr/bin/env python3
import sys
import os
import json

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'airflow', 'utils'))

from tmdb_api import TMDBClient
from mongo_handler import MongoHandler

MAX_DETAILS = -1

print("EXTRACTING MOVIE DETAILS")
try:
    # Load movie IDs from previous task
    with open('/tmp/airflow_movie_ids.json', 'r') as f:
        movie_ids = json.load(f)
    
    if MAX_DETAILS > 0:
        movie_ids = movie_ids[:MAX_DETAILS]
    
    tmdb = TMDBClient()
    mongo = MongoHandler()
    
    detailed_ids = []
    for i, movie_id in enumerate(movie_ids, 1):
        print(f"[{i}/{len(movie_ids)}] Movie {movie_id}")
        details = tmdb.get_movie_details(movie_id)
        if details:
            mongo.insert_movies([details])
            detailed_ids.append(movie_id)
            companies = details.get('production_companies', [])
            if companies:
                mongo.insert_companies(companies)
    
    # Save for next task
    with open('/tmp/airflow_detailed_ids.json', 'w') as f:
        json.dump(detailed_ids, f)
    
    print(f"SUCCESS: {len(detailed_ids)} details")
    
    tmdb.close()
    mongo.close()
    exit(0)
except Exception as e:
    print(f"ERROR: {str(e)}")
    import traceback
    traceback.print_exc()
    exit(1)