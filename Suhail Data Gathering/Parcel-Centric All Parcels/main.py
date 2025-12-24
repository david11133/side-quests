import csv
import sys
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---------------------------------------
# CONFIGURATION
# ---------------------------------------
INPUT_INVENTORY_FILE = "master_parcel_inventory.csv"
OUTPUT_METRICS_FILE = "final_parcel_metrics.csv"
OUTPUT_TRANSACTIONS_FILE = "final_parcel_transactions.csv"

# API Endpoints
CONSOLIDATED_TX_URL = "https://api2.suhail.ai/consolidatedTransactions"
PARCEL_METRICS_URL = "https://api2.suhail.ai/api/parcel/metrics/priceOfMeter"

# Performance
MAX_WORKERS = 8
BATCH_SIZE = 1  # Single requests are safer for stability, increase if slow

# ---------------------------------------
# SETUP
# ---------------------------------------
def create_session():
    session = requests.Session()
    adapter = HTTPAdapter(
        pool_connections=20,
        pool_maxsize=20,
        max_retries=Retry(total=3, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
    )
    session.mount('https://', adapter)
    return session

session = create_session()

# ---------------------------------------
# WORKER FUNCTIONS
# ---------------------------------------
def fetch_details(parcel_data):
    """
    Fetches both Metrics and Transactions for a single parcel from the inventory.
    """
    p_id = parcel_data.get('parcelObjectId')
    # Default Region ID (10 = Riyadh Region). You might need to change this if scraping other regions.
    region_id = 10 
    
    results = {
        'metrics': [],
        'transactions': []
    }
    
    # 1. Fetch Metrics (Price History) - Available for most parcels
    try:
        m_resp = session.get(
            PARCEL_METRICS_URL, 
            params={"parcelObjsIds": p_id, "groupingType": "Monthly"}, 
            timeout=10
        )
        if m_resp.status_code == 200:
            data = m_resp.json().get("data", [])
            # Flatten the nested structure
            for item in data:
                for m in item.get('neighborhoodMetrics', []):
                    m['parcelObjectId'] = p_id # Link back to parcel
                    results['metrics'].append(m)
    except Exception:
        pass

    # 2. Fetch Transactions (Sales History) - Only for sold parcels
    try:
        t_resp = session.get(
            CONSOLIDATED_TX_URL, 
            params={
                "ParcelObjectId": p_id, 
                "RegionId": region_id, 
                "LookbackValue": 50, # 50 years to get everything
                "LookbackType": "years",
                "Type": "الكل"
            }, 
            timeout=10
        )
        if t_resp.status_code == 200:
            tx_data = t_resp.json().get("data", {}).get("transactions", [])
            for tx in tx_data:
                tx['parcelObjectId'] = p_id
                results['transactions'].append(tx)
    except Exception:
        pass
        
    return results

# ---------------------------------------
# MAIN LOOP
# ---------------------------------------
if __name__ == "__main__":
    # 1. Load Inventory
    print(f"📂 Loading inventory from {INPUT_INVENTORY_FILE}...")
    parcels_to_process = []
    try:
        with open(INPUT_INVENTORY_FILE, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get('parcelObjectId'):
                    parcels_to_process.append(row)
    except FileNotFoundError:
        print("❌ Error: Run tile_scraper_full.py first to generate the inventory!")
        sys.exit()

    print(f"🚀 Starting detailed scrape for {len(parcels_to_process)} parcels...")

    # Prepare Output Files
    # We write headers later based on first result to be dynamic
    metrics_file_initialized = False
    transactions_file_initialized = False
    
    processed_count = 0
    
    # 2. Process in Threads
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_parcel = {executor.submit(fetch_details, p): p for p in parcels_to_process}
        
        for future in as_completed(future_to_parcel):
            processed_count += 1
            data = future.result()
            
            # Save Metrics
            if data['metrics']:
                with open(OUTPUT_METRICS_FILE, 'a', newline='', encoding='utf-8-sig') as f:
                    writer = csv.DictWriter(f, fieldnames=data['metrics'][0].keys())
                    if not metrics_file_initialized:
                        writer.writeheader()
                        metrics_file_initialized = True
                    writer.writerows(data['metrics'])

            # Save Transactions
            if data['transactions']:
                with open(OUTPUT_TRANSACTIONS_FILE, 'a', newline='', encoding='utf-8-sig') as f:
                    # Collect all possible keys from transaction data (it varies)
                    keys = set().union(*(d.keys() for d in data['transactions']))
                    writer = csv.DictWriter(f, fieldnames=list(keys))
                    if not transactions_file_initialized:
                        writer.writeheader()
                        transactions_file_initialized = True
                    writer.writerows(data['transactions'])

            # Progress Bar
            if processed_count % 50 == 0:
                sys.stdout.write(f"\r  Processed: {processed_count}/{len(parcels_to_process)} parcels")
                sys.stdout.flush()

    print(f"\n✅ Done! Data saved to {OUTPUT_METRICS_FILE} and {OUTPUT_TRANSACTIONS_FILE}")