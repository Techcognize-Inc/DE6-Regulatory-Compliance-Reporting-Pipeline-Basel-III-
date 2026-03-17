"""
Utilities to work around PySpark issues on Windows Docker/Kubernetes.
Uses pandas for testing small datasets to bypass Spark serialization issues.
"""
import pandas as pd
from typing import List, Tuple


def create_test_dataframe(data: List[Tuple], columns: List[str]) -> pd.DataFrame:
    """Create a DataFrame for testing - uses pandas to avoid Spark serialization issues."""
    return pd.DataFrame(data, columns=columns)


def test_car_calculation(capital_data: List[Tuple], asset_data: List[Tuple]) -> float:
    """
    Calculate CAR (Capital Adequacy Ratio) using pandas.
    
    Args:
        capital_data: List of tuples (capital_id, capital_type, balance)
        asset_data: List of tuples (id, branch, type, balance, risk_weight)
        
    Returns:
        CAR percentage
    """
    cap_df = pd.DataFrame(capital_data, columns=["capital_id", "capital_type", "balance"])
    asset_df = pd.DataFrame(asset_data, columns=["id", "branch", "type", "balance", "risk_weight"])
    
    # Calculate RWA = Balance * Risk Weight
    total_rwa = (asset_df["balance"] * asset_df["risk_weight"]).sum()
    
    # Get Tier 1 Capital
    tier1 = cap_df[cap_df["capital_type"] == "Tier 1 Capital"]["balance"].iloc[0]
    
    # CAR = (Tier 1 Capital / RWA) * 100
    return (tier1 / total_rwa) * 100


def test_lcr_window_calculation(data: List[Tuple], filter_days: int) -> tuple:
    """
    Calculate LCR rolling window using pandas.
    
    Args:
        data: List of tuples (id, amount, due_date)
        filter_days: Number of days to look ahead
        
    Returns:
        Tuple of (filtered_count, first_id)
    """
    from datetime import datetime, timedelta
    
    df = pd.DataFrame(data, columns=["id", "amount", "due_date"])
    
    today = datetime.now()
    window_end = today + timedelta(days=filter_days)
    
    df_filtered = df[df["due_date"] <= window_end]
    
    return len(df_filtered), df_filtered.iloc[0]["id"] if len(df_filtered) > 0 else None
