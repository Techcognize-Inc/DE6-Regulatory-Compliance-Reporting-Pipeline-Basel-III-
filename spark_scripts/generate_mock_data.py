import csv
import random
import uuid
from faker import Faker
import os

# Initialize Faker and establish our data directory
fake = Faker()
DATA_DIR = './data'
os.makedirs(DATA_DIR, exist_ok=True)

# ---------------------------------------------------------
# Configuration & Basel III Rules Engine
# ---------------------------------------------------------
NUM_BRANCHES = 50
NUM_ASSETS = 500000      # Set to 500k for testing; scale to millions later
NUM_LIABILITIES = 500000

# Asset Profiles: (Type, Risk Weight %, HQLA Level)
ASSET_PROFILES = [
    ('Cash', 0.0, 'Level 1'),
    ('Government Bonds', 0.0, 'Level 1'),
    ('Corporate Bonds', 0.50, 'Level 2A'),
    ('Residential Mortgage', 0.35, 'Non-HQLA'),
    ('Commercial Real Estate', 1.00, 'Non-HQLA'),
    ('Corporate Loan', 1.00, 'Non-HQLA'),
    ('Consumer Loan', 0.75, 'Non-HQLA')
]

# Liability Profiles: (Type, 30-Day Outflow Rate %)
LIABILITY_PROFILES = [
    ('Retail Checking', 0.05),
    ('Retail Savings', 0.05),
    ('Corporate Deposit', 0.40),
    ('Wholesale Funding', 1.00)
]

# ---------------------------------------------------------
# Data Generation Functions
# ---------------------------------------------------------
def generate_branches(num_branches):
    print("Generating Branches...")
    branches = []
    with open(f'{DATA_DIR}/branches.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['branch_id', 'branch_name', 'region'])
        for _ in range(num_branches):
            branch_id = str(uuid.uuid4())
            branches.append(branch_id)
            writer.writerow([branch_id, fake.company() + " Branch", fake.state()])
    return branches

def generate_assets(num_assets, branches, chunk_size=10000):
    print(f"Generating {num_assets} Assets (Loans & Securities)...")
    with open(f'{DATA_DIR}/assets.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['asset_id', 'branch_id', 'asset_type', 'balance', 'risk_weight', 'hqla_level', 'loan_status'])
        
        for i in range(0, num_assets, chunk_size):
            chunk = []
            for _ in range(min(chunk_size, num_assets - i)):
                asset_id = str(uuid.uuid4())
                branch_id = random.choice(branches)
                asset_profile = random.choice(ASSET_PROFILES)
                
                # Assign balance: $10k to $5M
                balance = round(random.uniform(10000, 5000000), 2) 
                
                # Determine if it's a loan to assign performing/non-performing status
                is_loan = 'Loan' in asset_profile[0] or 'Mortgage' in asset_profile[0]
                # 3% chance of default for realism
                loan_status = random.choices(['Performing', 'Non-Performing'], weights=[97, 3], k=1)[0] if is_loan else 'N/A'
                
                chunk.append([
                    asset_id, branch_id, asset_profile[0], balance, 
                    asset_profile[1], asset_profile[2], loan_status
                ])
            writer.writerows(chunk)

def generate_liabilities(num_liabilities, branches, chunk_size=10000):
    print(f"Generating {num_liabilities} Liabilities (Deposits & Funding)...")
    with open(f'{DATA_DIR}/liabilities.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['liability_id', 'branch_id', 'liability_type', 'balance', 'outflow_rate'])
        
        for i in range(0, num_liabilities, chunk_size):
            chunk = []
            for _ in range(min(chunk_size, num_liabilities - i)):
                liability_id = str(uuid.uuid4())
                branch_id = random.choice(branches)
                liab_profile = random.choice(LIABILITY_PROFILES)
                
                # Assign balance: $5k to $10M
                balance = round(random.uniform(5000, 10000000), 2)
                
                chunk.append([liability_id, branch_id, liab_profile[0], balance, liab_profile[1]])
            writer.writerows(chunk)

def generate_capital():
    print("Generating Bank Capital Data...")
    # Just a simple aggregate tier 1 capital table for CAR calculation
    with open(f'{DATA_DIR}/capital.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['capital_id', 'capital_type', 'balance'])
        writer.writerow([str(uuid.uuid4()), 'Tier 1 Capital', round(random.uniform(500000000, 1000000000), 2)])

# ---------------------------------------------------------
# Main Execution
# ---------------------------------------------------------
if __name__ == "__main__":
    branch_ids = generate_branches(NUM_BRANCHES)
    generate_assets(NUM_ASSETS, branch_ids)
    generate_liabilities(NUM_LIABILITIES, branch_ids)
    generate_capital()
    print(f"Mock data generation complete. Files saved to {DATA_DIR}/")