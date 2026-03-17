from datetime import datetime, timedelta
from pyspark.sql.functions import col


def test_lcr_rolling_window(spark_session):
    today = datetime.now()
    # One inside 30 days, one outside
    data = [
        ("TXN1", 1000, today + timedelta(days=15)),
        ("TXN2", 5000, today + timedelta(days=45))
    ]
    df = spark_session.createDataFrame(data, ["id", "amount", "due_date"])
    
    # Logic: Filter for next 30 days
    window_end = today + timedelta(days=30)
    df_filtered = df.filter(col("due_date") <= window_end)
    
    # Validation: Collect to Python to avoid Spark count() issues on Windows
    filtered_rows = df_filtered.collect()
    
    assert len(filtered_rows) == 1
    assert filtered_rows[0]["id"] == "TXN1"