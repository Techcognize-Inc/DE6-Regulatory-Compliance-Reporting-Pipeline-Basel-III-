import os
import sys
import pytest
from pyspark.sql import SparkSession

# Set environment variables for Windows Spark
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'
os.environ['PYSPARK_PYTHON'] = sys.executable  
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


@pytest.fixture(scope="session", autouse=True)
def setup_pyspark_env():
    python_path = sys.executable
    os.environ['PYSPARK_PYTHON'] = python_path
    os.environ['PYSPARK_DRIVER_PYTHON'] = python_path


@pytest.fixture(scope="session")
def spark_session():
    import tempfile
    temp_dir = tempfile.gettempdir()
    
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("pytest-pyspark-testing")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.python.worker.reuse", "false")
        .config("spark.default.parallelism", "1")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .config("spark.sql.codegen.wholeStage", "false")
        .config("spark.network.timeout", "120s")
        .config("spark.rpc.numRetries", "3")
        .config("spark.driver.extraJavaOptions", f"-Djna.tmpdir={temp_dir}")
        .getOrCreate()
    )
    
    yield spark
    spark.stop()