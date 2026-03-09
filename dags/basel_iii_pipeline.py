from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

# ---------------------------------------------------------
# Senior DE Config: SLAs and Retries
# ---------------------------------------------------------
default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'sla': timedelta(hours=2) 
}

with DAG(
    'basel_iii_regulatory_reporting',
    default_args=default_args,
    description='Monthly Basel III Pipeline: CAR, LCR, NPL Computation',
    schedule_interval='@monthly',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['finance', 'regulatory', 'spark'],
) as dag:

    # ---------------------------------------------------------
    # 1. Fetch External Benchmarks
    # Note: Using absolute container path /opt/airflow/
    # ---------------------------------------------------------
    fetch_fdic_benchmarks = BashOperator(
        task_id='fetch_fdic_benchmarks',
        bash_command='python /opt/airflow/spark_scripts/fetch_fdic_benchmarks.py'
    )

    # ---------------------------------------------------------
    # 2. Data Ingestion / Generation
    # ---------------------------------------------------------
    ingest_balance_sheets = BashOperator(
        task_id='ingest_balance_sheets',
        bash_command='python /opt/airflow/spark_scripts/generate_mock_data.py'
    )

    # ---------------------------------------------------------
    # 3. Spark Computations (Parallelized)
    # ---------------------------------------------------------
    spark_compute_car = SparkSubmitOperator(
        task_id='spark_compute_car',
        application='/opt/airflow/spark_scripts/spark_compute_car.py',
        conn_id='spark_default',
        name='airflow_basel_car',
        verbose=False
    )

    spark_compute_lcr = SparkSubmitOperator(
        task_id='spark_compute_lcr',
        application='/opt/airflow/spark_scripts/spark_compute_lcr.py',
        conn_id='spark_default',
        name='airflow_basel_lcr',
        verbose=False
    )

    spark_compute_npl = SparkSubmitOperator(
        task_id='spark_compute_npl',
        application='/opt/airflow/spark_scripts/spark_compute_npl.py',
        conn_id='spark_default',
        name='airflow_basel_npl',
        verbose=False
    )

    # ---------------------------------------------------------
    # 4. Downstream Steps (Metrics Load & Reporting)
    # ---------------------------------------------------------
    load_regulatory_metrics = EmptyOperator(
        task_id='load_regulatory_metrics'
    )

    generate_report = EmptyOperator(
        task_id='generate_report'
    )

    update_dashboard = EmptyOperator(
        task_id='update_dashboard'
    )

    # ---------------------------------------------------------
    # DAG Dependencies (The Execution Flow)
    # ---------------------------------------------------------
    
    # Ingestion steps run first
    [fetch_fdic_benchmarks, ingest_balance_sheets] >> spark_compute_car
    
    # Spark jobs run in parallel
    fetch_fdic_benchmarks >> spark_compute_lcr
    ingest_balance_sheets >> spark_compute_npl
    
    # Final aggregation and reporting
    [spark_compute_car, spark_compute_lcr, spark_compute_npl] >> load_regulatory_metrics
    load_regulatory_metrics >> generate_report >> update_dashboard