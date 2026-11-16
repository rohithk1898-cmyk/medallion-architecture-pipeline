from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import json
import os
import glob

def transform_bronze_to_silver():
    # 1. Define paths mapped to the Docker container's internal storage
    bronze_dir = '/opt/airflow/data/bronze'
    silver_dir = '/opt/airflow/data/silver'
    
    # 2. Automatically find the most recently downloaded JSON file
    list_of_files = glob.glob(f'{bronze_dir}/*.json')
    if not list_of_files:
        raise FileNotFoundError("No Bronze data found to process!")
    latest_file = max(list_of_files, key=os.path.getctime)
    
    # 3. Load the raw JSON data
    with open(latest_file, 'r') as file:
        raw_data = json.load(file)
        
    # 4. Transform the nested data using Pandas
    carts = raw_data.get('carts', [])
    
    # Flatten the nested 'products' array while keeping parent cart metadata attached
    df = pd.json_normalize(
        carts, 
        record_path=['products'], 
        meta=['id', 'userId', 'totalProducts'],
        meta_prefix='cart_'
    )
    
    # 5. Save the cleaned, tabular data to the Silver layer as a CSV
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    output_file = f"{silver_dir}/silver_carts_{timestamp}.csv"
    
    df.to_csv(output_file, index=False)
    print(f"Success! Tabular data exported to {output_file}")

# DAG Configuration
default_args = {
    'owner': 'kamal',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='silver_carts_transformation',
    default_args=default_args,
    start_date=datetime(2026, 11, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    
    transform_task = PythonOperator(
        task_id='flatten_and_clean_data',
        python_callable=transform_bronze_to_silver
    )