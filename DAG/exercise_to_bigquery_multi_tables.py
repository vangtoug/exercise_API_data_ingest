import os #add access to operating system modules

from airflow import DAG #import DAG functionality from airflow
from airflow.operators.python import PythonOperator
from google.cloud import bigquery
import requests
import pandas as pd
import json
from datetime import datetime

# Constants
PROJECT_ID = "sublime-formula-452220-v7"  # your GCP project ID in BigQuery
DATASET_ID = "exercise_to_bigquery"  # your dataset name in BigQuery
API_KEY = "OUINDaTzeZ2053AcHz94wQ==VEYmzNTIOuRpQdiO" 
BASE_API_URL = "https://api.api-ninjas.com/v1/exercises"

# Table mappings
TABLES = {
    "strength": f"{PROJECT_ID}.{DATASET_ID}.exercise_strength",  # creates your table name in BigQuery
    "olympic_weightlifting": f"{PROJECT_ID}.{DATASET_ID}.exercise_olympic_weightlifting",  # creates your table name in BigQuery
    "expert": f"{PROJECT_ID}.{DATASET_ID}.exercise_expert"  # creates your table name in BigQuery
}

# Function to fetch and save exercise data based on type/difficulty
def fetch_exercise_data(filter_key, filter_value, filename):
    headers = {'X-Api-Key': API_KEY}
    api_url = f"{BASE_API_URL}?{filter_key}={filter_value}"
    
    response = requests.get(api_url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        with open(f"/tmp/{filename}.json", 'w') as f:
            json.dump(data, f)
    else:
        raise Exception(f"API request failed: {response.status_code} - {response.text}")

# Function to load data into BigQuery
def load_to_bigquery(filename, table_id):
    client = bigquery.Client()
    
    # Load saved JSON data
    with open(f"/tmp/{filename}.json", 'r') as f:
        data = json.load(f)
    
    # Convert JSON to DataFrame
    df = pd.DataFrame(data)

    # Load DataFrame into BigQuery
    job = client.load_table_from_dataframe(df, table_id)
    job.result()  # Wait for the job to complete

    print(f"Data successfully loaded into {table_id}")

# Define the Airflow DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 2, 26),
    'retries': 1
}

dag = DAG(
    'exercise_to_bigquery_multi_tables',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

# Task 1: Fetch and Load "strength" exercises
fetch_strength_task = PythonOperator(
    task_id='fetch_strength_exercises',
    python_callable=fetch_exercise_data,
    op_args=['type', 'strength', 'strength_data'],
    dag=dag
)

load_strength_task = PythonOperator(
    task_id='load_strength_to_bigquery',
    python_callable=load_to_bigquery,
    op_args=['strength_data', TABLES['strength']],
    dag=dag
)

# Task 2: Fetch and Load "olympic_weightlifting" exercises
fetch_olympic_task = PythonOperator(
    task_id='fetch_olympic_exercises',
    python_callable=fetch_exercise_data,
    op_args=['type', 'olympic_weightlifting', 'olympic_data'],
    dag=dag
)

load_olympic_task = PythonOperator(
    task_id='load_olympic_to_bigquery',
    python_callable=load_to_bigquery,
    op_args=['olympic_data', TABLES['olympic_weightlifting']],
    dag=dag
)

# Task 3: Fetch and Load "expert" difficulty exercises
fetch_expert_task = PythonOperator(
    task_id='fetch_expert_exercises',
    python_callable=fetch_exercise_data,
    op_args=['difficulty', 'expert', 'expert_data'],
    dag=dag
)

load_expert_task = PythonOperator(
    task_id='load_expert_to_bigquery',
    python_callable=load_to_bigquery,
    op_args=['expert_data', TABLES['expert']],
    dag=dag
)

# Define task dependencies
#fetch_strength_task >> load_strength_task
#fetch_olympic_task >> load_olympic_task
#fetch_expert_task >> load_expert_task

fetch_strength_task >> load_strength_task >> fetch_olympic_task >> load_olympic_task >> fetch_expert_task >> load_expert_task
