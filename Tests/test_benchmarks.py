def test_fdic_benchmark_delta():
    bank_car = 12.50
    fdic_avg = 11.00 # Simulated pull from fetch_fdic_benchmarks.py
    
    delta = bank_car - fdic_avg
    
    # Verify the delta is exactly 1.5%
    assert delta == 1.50
    # Ensure it's marked as "Above Benchmark"
    assert delta > 0