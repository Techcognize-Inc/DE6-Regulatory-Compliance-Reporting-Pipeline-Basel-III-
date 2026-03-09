import psycopg2
from datetime import datetime
import uuid

# Connection parameters matching our docker-compose.yml
DB_PARAMS = {
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow",
    "host": "localhost",
    "port": "5432"
}

def load_simulated_spark_results():
    print("Connecting to PostgreSQL to load metrics...")
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()
    
    run_id = str(uuid.uuid4())
    computation_date = datetime.now()
    
    # These are the simulated outputs from our Spark jobs
    # In a real pipeline, Airflow would read these from a temporary staging table or S3 bucket
    bank_metrics = [
        (run_id, 'CAR', 12.50, computation_date),  # Capital Adequacy Ratio (Min 8%)
        (run_id, 'LCR', 115.20, computation_date), # Liquidity Coverage Ratio (Min 100%)
        (run_id, 'NPL', 2.85, computation_date)    # Non-Performing Loan Ratio
    ]
    
    insert_query = """
        INSERT INTO regulatory_metrics (run_id, metric_name, metric_value, computation_date)
        VALUES (%s, %s, %s, %s)
    """
    
    cur.executemany(insert_query, bank_metrics)
    
    # Also log this run in our audit table
    audit_query = """
        INSERT INTO audit_log (run_id, pipeline_step, status, message)
        VALUES (%s, %s, %s, %s)
    """
    cur.execute(audit_query, (run_id, 'Metrics Load', 'SUCCESS', 'Basel III metrics successfully loaded to reporting layer.'))
    
    conn.commit()
    print(f"Metrics loaded successfully under Run ID: {run_id}")
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    load_simulated_spark_results()