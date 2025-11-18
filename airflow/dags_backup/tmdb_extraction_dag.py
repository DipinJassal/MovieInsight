from datetime import datetime, timedelta
import sys
import os

# CRITICAL: Add utils to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'utils'))

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from tmdb_api import TMDBClient
from mongo_handler import MongoHandler

default_args = {
    'owner': 'team7',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

PAGES_TO_FETCH = 50
MAX_DETAILS = -1

def extract_genres(**context):
    print("Extracting genres...")
    tmdb = TMDBClient()
    mongo = MongoHandler()
    
    try:
        genres = tmdb.get_genres()
        stats = mongo.insert_genres(genres)
        print(f"Inserted {stats['inserted']} genres")
        return len(genres)
    finally:
        tmdb.close()
        mongo.close()

def extract_popular_movies(**context):
    print("Extracting popular movies...")
    tmdb = TMDBClient()
    mongo = MongoHandler()
    
    try:
        movies = tmdb.get_popular_movies(pages=PAGES_TO_FETCH)
        stats = mongo.insert_movies(movies)
        
        movie_ids = [movie['id'] for movie in movies]
        ti = context['ti']
        ti.xcom_push(key='movie_ids', value=movie_ids)
        
        print(f"Inserted {stats['inserted']} movies")
        return len(movies)
    finally:
        tmdb.close()
        mongo.close()

def extract_movie_details(**context):
    print("Extracting movie details...")
    tmdb = TMDBClient()
    mongo = MongoHandler()
    
    try:
        ti = context['ti']
        movie_ids = ti.xcom_pull(key='movie_ids', task_ids='extract_popular_movies')
        
        if MAX_DETAILS > 0:
            movie_ids = movie_ids[:MAX_DETAILS]
        
        detailed_movies = []
        for i, movie_id in enumerate(movie_ids, 1):
            print(f"[{i}/{len(movie_ids)}] Fetching movie {movie_id}")
            details = tmdb.get_movie_details(movie_id)
            if details:
                detailed_movies.append(details)
                companies = details.get('production_companies', [])
                if companies:
                    mongo.insert_companies(companies)
        
        if detailed_movies:
            stats = mongo.insert_movies(detailed_movies)
            detailed_ids = [m['id'] for m in detailed_movies]
            ti.xcom_push(key='detailed_movie_ids', value=detailed_ids)
        
        return len(detailed_movies)
    finally:
        tmdb.close()
        mongo.close()

def extract_movie_credits(**context):
    print("Extracting movie credits...")
    tmdb = TMDBClient()
    mongo = MongoHandler()
    
    try:
        ti = context['ti']
        movie_ids = ti.xcom_pull(key='detailed_movie_ids', task_ids='extract_movie_details')
        
        total_credits = 0
        for i, movie_id in enumerate(movie_ids, 1):
            print(f"[{i}/{len(movie_ids)}] Fetching credits for {movie_id}")
            credits = tmdb.get_movie_credits(movie_id)
            if credits:
                stats = mongo.insert_credits(movie_id, credits)
                total_credits += stats['cast_count'] + stats['crew_count']
        
        return total_credits
    finally:
        tmdb.close()
        mongo.close()

with DAG(
    dag_id='tmdb_data_extraction',
    default_args=default_args,
    description='Extract movie data from TMDB API',
    schedule_interval='@daily',
    catchup=False,
    tags=['tmdb', 'extraction'],
) as dag:
    
    task_genres = PythonOperator(
        task_id='extract_genres',
        python_callable=extract_genres,
        provide_context=True,
    )
    
    task_movies = PythonOperator(
        task_id='extract_popular_movies',
        python_callable=extract_popular_movies,
        provide_context=True,
    )
    
    task_details = PythonOperator(
        task_id='extract_movie_details',
        python_callable=extract_movie_details,
        provide_context=True,
    )
    
    task_credits = PythonOperator(
        task_id='extract_movie_credits',
        python_callable=extract_movie_credits,
        provide_context=True,
    )
    
    task_genres >> task_movies >> task_details >> task_credits