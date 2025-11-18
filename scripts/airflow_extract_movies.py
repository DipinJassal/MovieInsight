#!/usr/bin/env python3
import sys
import os
import json

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'airflow', 'utils'))

from tmdb_api import TMDBClient
from mongo_handler import MongoHandler

PAGES_TO_FETCH = 50

print("EXTRACTING MOVIES")
try:
    tmdb = TMDBClient()
    mongo = MongoHandler()
    
    movies = tmdb.get_popular_movies(pages=PAGES_TO_FETCH)
    stats = mongo.insert_movies(movies)
    
    # Save movie IDs for next task
    movie_ids = [m['id'] for m in movies]
    with open('/tmp/airflow_movie_ids.json', 'w') as f:
        json.dump(movie_ids, f)
    
    print(f"SUCCESS: {len(movies)} movies")
    
    tmdb.close()
    mongo.close()
    exit(0)
except Exception as e:
    print(f"ERROR: {str(e)}")
    import traceback
    traceback.print_exc()
    exit(1)