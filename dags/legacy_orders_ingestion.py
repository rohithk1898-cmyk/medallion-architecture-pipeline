from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import json
import os

def extract_api_to_bronze():
    # 1. Reach out to the internet and grab the data
    api_url = "https://dummyjson.com/carts"
    print(f"Extracting data from {api_url}...")
    
    response = requests.get(api_url)
    data = response.json()
    
    # 2. Make sure our local 'bronze' folder exists
    bronze_dir = '/opt/airflow/data/bronze'
    os.makedirs(bronze_dir, exist_ok=True)
    
    # 3. Save the data to our computer
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    file_path = f"{bronze_dir}/legacy_carts_{timestamp}.json"
    
    with open(file_path, 'w') as f:
        json.dump(data, f)
        
    print(f"Success! Carts saved to Bronze Layer at {file_path}")

# 4. Set up the Airflow schedule
with DAG(
    dag_id='legacy_orders_ingestion',
    start_date=datetime(2026, 11, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    ingest_task = PythonOperator(
        task_id='fetch_carts_api',
        python_callable=extract_api_to_bronze
    )