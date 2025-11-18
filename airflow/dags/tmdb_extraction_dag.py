from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'team7',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=60),
}

# Get the project path
import os
PROJECT_PATH = '/Users/dipinjassal/ml/tmdb_data_warehouse'
VENV_PYTHON = f'{PROJECT_PATH}/venv/bin/python'

with DAG(
    dag_id='tmdb_data_extraction',
    default_args=default_args,
    description='Extract movie data from TMDB API',
    schedule_interval=None,
    catchup=False,
    tags=['tmdb', 'extraction'],
) as dag:
    
    task_genres = BashOperator(
        task_id='extract_genres',
        bash_command=f'cd {PROJECT_PATH} && {VENV_PYTHON} scripts/airflow_extract_genres.py',
    )
    
    task_movies = BashOperator(
        task_id='extract_popular_movies',
        bash_command=f'cd {PROJECT_PATH} && {VENV_PYTHON} scripts/airflow_extract_movies.py',
    )
    
    task_details = BashOperator(
        task_id='extract_movie_details',
        bash_command=f'cd {PROJECT_PATH} && {VENV_PYTHON} scripts/airflow_extract_details.py',
    )
    
    task_credits = BashOperator(
        task_id='extract_movie_credits',
        bash_command=f'cd {PROJECT_PATH} && {VENV_PYTHON} scripts/airflow_extract_credits.py',
    )
    
    task_genres >> task_movies >> task_details >> task_credits