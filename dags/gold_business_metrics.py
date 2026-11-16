from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import glob
import os

def aggregate_silver_to_gold():
    # 1. Define storage paths
    silver_dir = '/opt/airflow/data/silver'
    gold_dir = '/opt/airflow/data/gold'
    
    # 2. Find the most recently cleaned Silver CSV
    list_of_files = glob.glob(f'{silver_dir}/*.csv')
    if not list_of_files:
        raise FileNotFoundError("No Silver data found to process!")
    latest_file = max(list_of_files, key=os.path.getctime)
    
    # 3. Load the clean data
    df = pd.read_csv(latest_file)
    
    # 4. Transform to Gold: Aggregate total revenue and items per user
    gold_df = df.groupby('cart_userId').agg({
        'total': 'sum',
        'quantity': 'sum'
    }).rename(columns={
        'total': 'total_revenue_spent', 
        'quantity': 'total_items_purchased'
    }).reset_index()
    
    # 5. Save the final business report to the Gold layer
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    output_file = f"{gold_dir}/gold_user_metrics_{timestamp}.csv"
    
    gold_df.to_csv(output_file, index=False)
    print(f"Success! Business metrics exported to {output_file}")

# DAG Configuration
default_args = {
    'owner': 'kamal',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='gold_business_metrics',
    default_args=default_args,
    start_date=datetime(2026, 11, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    
    aggregate_task = PythonOperator(
        task_id='calculate_business_metrics',
        python_callable=aggregate_silver_to_gold
    )