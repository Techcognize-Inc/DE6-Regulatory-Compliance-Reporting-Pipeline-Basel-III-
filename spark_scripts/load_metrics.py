import psycopg2
import pandas as pd
import glob
import os
from datetime import datetime
import uuid

DB_PARAMS = {
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow",
    "host": "postgres", # Use 'postgres' if running inside Airflow container
    "port": "5432"
}

DATA_DIR = "/opt/airflow/data" # Path mapped to /opt/airflow/data

def get_latest_metric(metric_folder):
    """Helper to find the CSV part file inside Spark's output folder"""
    path = os.path.join(DATA_DIR, metric_folder, "*.csv")
    files = glob.glob(path)
    if not files:
        return 0.0
    df = pd.read_csv(files[0])
    return float(df['metric_value'].iloc[0])

def load_actual_spark_results():
    print("Connecting to PostgreSQL to load ACTUAL metrics...")
    
    # 1. Pull the values calculated by Spark
    car_val = get_latest_metric("car_result")
    lcr_val = get_latest_metric("lcr_result")
    npl_val = get_latest_metric("npl_result")

    run_id = str(uuid.uuid4())
    comp_date = datetime.now()
    
    bank_metrics = [
        (run_id, 'CAR', car_val, comp_date),
        (run_id, 'LCR', lcr_val, comp_date),
        (run_id, 'NPL', npl_val, comp_date)
    ]
    
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()
    
    # 2. Insert into PostgreSQL
    insert_query = "INSERT INTO regulatory_metrics (run_id, metric_name, metric_value, computation_date) VALUES (%s, %s, %s, %s)"
    cur.executemany(insert_query, bank_metrics)
    
    # 3. Log to Audit
    cur.execute("INSERT INTO audit_log (run_id, pipeline_step, status, message) VALUES (%s, %s, %s, %s)", 
                (run_id, 'Metrics Load', 'SUCCESS', f'Loaded CAR:{car_val}, LCR:{lcr_val}, NPL:{npl_val}'))
    
    conn.commit()
    print(f"✅ Successfully loaded real Spark data under Run ID: {run_id}")
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    load_actual_spark_results()