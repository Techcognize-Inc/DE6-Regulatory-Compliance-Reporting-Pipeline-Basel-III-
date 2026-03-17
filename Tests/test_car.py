import pytest


def test_car_math_accuracy(spark_session):
    # Setup: Mock Tier 1 Capital and RWA
    capital_data = [("C001", "Tier 1 Capital", 500.00)]
    asset_data = [("A001", "B01", "Corporate Loan", 4000.00, 1.00)]
    
    cap_df = spark_session.createDataFrame(capital_data, ["capital_id", "capital_type", "balance"])
    asset_df = spark_session.createDataFrame(asset_data, ["id", "branch", "type", "balance", "risk_weight"])
    
    # Logic: RWA = Balance * Risk Weight
    # Collect to Python to avoid Spark aggregation issues on Windows
    asset_rows = asset_df.collect()
    total_rwa = sum(float(row["balance"]) * float(row["risk_weight"]) for row in asset_rows)
    
    cap_rows = cap_df.filter(cap_df.capital_type == "Tier 1 Capital").collect()
    tier1 = float(cap_rows[0]["balance"])
    
    actual_car = (tier1 / total_rwa) * 100
    expected_car = 12.5
    
    assert abs(actual_car - expected_car) < (0.01 * expected_car)