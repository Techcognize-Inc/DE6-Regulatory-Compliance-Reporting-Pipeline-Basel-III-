import requests
import csv
import os
from datetime import datetime

DATA_DIR = './data'
os.makedirs(DATA_DIR, exist_ok=True)

def fetch_industry_benchmarks():
    print("Fetching data from FDIC BankFind API...")
    
    # We hit the FDIC financials endpoint. 
    # To keep it lightweight, we'll query a subset of large banks to form our "industry average" proxy.
    url = "https://banks.data.fdic.gov/api/financials"
    
    # Query parameters: Pulling Tier 1 Risk-Based Capital Ratio (EQVWCOR) and NPL proxies
    # for a recent period (e.g., 2023-12-31)
    params = {
        "filters": "REPDTE:20231231 AND ASSET:[100000000 TO *]", # Banks with > $100M assets
        "fields": "REPDTE,CERT,NAME,EQVWCOR", 
        "limit": 100,
        "format": "json"
    }

    response = requests.get(url, params=params)
    
    if response.status_code != 200:
        raise Exception(f"API Request failed with status code {response.status_code}: {response.text}")
    
    data = response.json().get('data', [])
    
    print(f"Retrieved {len(data)} bank records. Calculating averages...")
    
    # Calculate industry averages from the payload
    # Note: EQVWCOR is the Tier 1 Risk-Based Capital Ratio
    car_values = []
    for item in data:
        metrics = item.get('data', {})
        car = metrics.get('EQVWCOR')
        if car is not None:
            car_values.append(float(car))
            
    avg_car = sum(car_values) / len(car_values) if car_values else 0.0
    
    # For demonstration, we'll hardcode standard regulatory baselines for LCR and NPL, 
    # as they require complex derived aggregations from Call Reports.
    benchmarks = [
        {"metric_name": "CAR", "industry_average": round(avg_car, 2)},
        {"metric_name": "LCR", "industry_average": 100.00}, # Basel III minimum is 100%
        {"metric_name": "NPL", "industry_average": 1.50}    # General healthy industry average
    ]
    
    # Write to CSV in our shared data volume
    output_file = os.path.join(DATA_DIR, 'fdic_benchmarks.csv')
    with open(output_file, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['report_period', 'metric_name', 'industry_average', 'fetched_at'])
        
        report_period = "20231231"
        fetched_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        for b in benchmarks:
            writer.writerow([report_period, b['metric_name'], b['industry_average'], fetched_at])
            
    print(f"Benchmarks saved to {output_file}")
    print(f"Computed Industry Avg CAR: {avg_car:.2f}%")

if __name__ == "__main__":
    fetch_industry_benchmarks()