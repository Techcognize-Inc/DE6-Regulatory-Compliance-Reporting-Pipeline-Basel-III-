import psycopg2
from psycopg2 import sql

# Connection parameters match our docker-compose.yml
DB_PARAMS = {
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow",
    "host": "localhost",
    "port": "5432"
}

def init_database():
    print("Connecting to PostgreSQL...")
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()

    # Senior DE Tip: Always use IF NOT EXISTS so the script is idempotent (safe to rerun)
    create_tables_query = """
    -- Table for our internal bank metrics
    CREATE TABLE IF NOT EXISTS regulatory_metrics (
        run_id VARCHAR(50),
        metric_name VARCHAR(50),
        metric_value DECIMAL(10, 4),
        computation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (run_id, metric_name)
    );

    -- Table for FDIC industry benchmarks
    CREATE TABLE IF NOT EXISTS fdic_benchmarks (
        report_period VARCHAR(20),
        metric_name VARCHAR(50),
        industry_average DECIMAL(10, 4),
        fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (report_period, metric_name)
    );

    -- Audit log for regulatory compliance
    CREATE TABLE IF NOT EXISTS audit_log (
        log_id SERIAL PRIMARY KEY,
        run_id VARCHAR(50),
        pipeline_step VARCHAR(100),
        status VARCHAR(20),
        message TEXT,
        log_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    cur.execute(create_tables_query)
    conn.commit()
    
    print("Database schema successfully initialized!")
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    init_database()