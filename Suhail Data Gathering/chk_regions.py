import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import csv
import sys
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from typing import Dict, List, Optional, Tuple, Set
import time
import signal

METRICS_URL = "https://api2.suhail.ai/api/mapMetrics/landMetrics/list"
TRANSACTIONS_URL = "https://api2.suhail.ai/transactions/neighbourhood"
TX_DETAILS_URL = "https://api2.suhail.ai/api/transactions/search"
PARCEL_URL = "https://api2.suhail.ai/api/parcel/search"

REGION_IDS = range(1, 31)
METRICS_LIMIT = 600
PAGE_SIZE = 1000
OUTPUT_FILE = "neighborhood_transactions.csv"

TEST = False  # set False for full run

# Performance settings
MAX_WORKERS = 8
BATCH_SIZE = 30
REQUEST_TIMEOUT = 15
BATCH_TIMEOUT = 60

# ---------------------------------------
# Check which regions are already scraped
# ---------------------------------------

def check_region_completion(region_id: int) -> bool:
    """
    Check if a region is fully scraped by verifying it has data.
    Returns True if region should be skipped (already complete).
    Returns False if region needs to be scraped.
    """
    if not os.path.exists(OUTPUT_FILE):
        return False
    
    try:
        # Count rows for this region
        row_count = 0
        with open(OUTPUT_FILE, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if 'regionId' in row and row['regionId']:
                    try:
                        if int(row['regionId']) == region_id:
                            row_count += 1
                            # If we find at least some data, assume region is done
                            # You can adjust this threshold if needed
                            if row_count >= 10:
                                return True
                    except (ValueError, TypeError):
                        continue
        
        # If we found some rows but less than threshold, region might be incomplete
        # Return False to re-scrape it
        return False
        
    except Exception as e:
        print(f"⚠️  Error checking region {region_id}: {e}")
        return False


def get_all_regions_status() -> Dict[int, bool]:
    """Get completion status for all regions"""
    status = {}
    for region_id in REGION_IDS:
        status[region_id] = check_region_completion(region_id)
    return status

# ---------------------------------------
# Session with connection pooling & retries
# ---------------------------------------

def create_session():
    session = requests.Session()
    
    adapter = HTTPAdapter(
        pool_connections=15,
        pool_maxsize=15,
        max_retries=Retry(
            total=2,
            backoff_factor=0.3,
            status_forcelist=[500, 502, 503, 504],
            raise_on_status=False
        )
    )
    
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    
    return session

session = create_session()

# Caches
transaction_details_cache = {}
parcel_geometry_cache = {}
seen_transactions = set()
rows = []

# ---------------------------------------
# Helpers with better error handling
# ---------------------------------------

def is_valid_subdivision(subdivision_no):
    """Parcel API only accepts numeric subdivision numbers"""
    return subdivision_no and subdivision_no.strip().isdigit()


def get_transaction_details_batch(region_id: int, tx_numbers: List[str]) -> Dict[str, dict]:
    """Fetch multiple transaction details in parallel with timeout protection"""
    results = {}
    uncached = [tx for tx in tx_numbers if tx not in transaction_details_cache]
    
    if not uncached:
        return {tx: transaction_details_cache[tx] for tx in tx_numbers}
    
    def fetch_one(tx_number):
        try:
            resp = session.get(
                TX_DETAILS_URL,
                params={"transactionNo": tx_number, "regionId": region_id},
                timeout=REQUEST_TIMEOUT
            )
            resp.raise_for_status()
            data = resp.json().get("data", [])
            return tx_number, data[0] if data else {}, None
        except requests.exceptions.Timeout:
            return tx_number, {}, "timeout"
        except Exception as e:
            return tx_number, {}, str(e)
    
    # Process in smaller batches to avoid overwhelming
    for i in range(0, len(uncached), BATCH_SIZE):
        batch = uncached[i:i + BATCH_SIZE]
        sys.stdout.write(f"[D:{len(batch)}]")
        sys.stdout.flush()
        
        try:
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = {executor.submit(fetch_one, tx): tx for tx in batch}
                
                for future in as_completed(futures, timeout=BATCH_TIMEOUT):
                    try:
                        tx_number, details, error = future.result(timeout=5)
                        
                        if error:
                            print(f"\n⚠️  TX {tx_number}: {error}")
                            transaction_details_cache[tx_number] = {}
                        else:
                            transaction_details_cache[tx_number] = details
                        
                        results[tx_number] = transaction_details_cache[tx_number]
                        
                    except TimeoutError:
                        tx_number = futures[future]
                        print(f"\n⚠️  TX {tx_number}: future timeout")
                        transaction_details_cache[tx_number] = {}
                        results[tx_number] = {}
                    except Exception as e:
                        tx_number = futures[future]
                        print(f"\n⚠️  TX {tx_number}: {e}")
                        transaction_details_cache[tx_number] = {}
                        results[tx_number] = {}
                        
        except TimeoutError:
            print(f"\n⚠️  Batch timeout, skipping remaining in batch")
            for tx in batch:
                if tx not in results:
                    transaction_details_cache[tx] = {}
                    results[tx] = {}
        except Exception as e:
            print(f"\n⚠️  Batch error: {e}")
            for tx in batch:
                if tx not in results:
                    transaction_details_cache[tx] = {}
                    results[tx] = {}
    
    # Add cached results
    for tx in tx_numbers:
        if tx not in results:
            results[tx] = transaction_details_cache.get(tx, {})
    
    return results


def get_parcel_geometry_batch(requests_list: List[Tuple]) -> Dict[Tuple, Optional[dict]]:
    """Fetch multiple parcel geometries in parallel with timeout protection"""
    results = {}
    uncached = [r for r in requests_list if r not in parcel_geometry_cache]
    
    if not uncached:
        return {r: parcel_geometry_cache[r] for r in requests_list}
    
    def fetch_one(params):
        region_id, province_id, subdivision_no, parcel_no = params
        
        if not is_valid_subdivision(subdivision_no):
            return params, None, None
        
        try:
            resp = session.get(
                PARCEL_URL,
                params={
                    "regionId": region_id,
                    "provinceId": province_id,
                    "subdivisionNo": subdivision_no,
                    "parcelNo": parcel_no,
                    "offset": 0,
                    "limit": 10
                },
                timeout=REQUEST_TIMEOUT
            )
            
            if resp.status_code in (404, 410):
                return params, None, None
            
            resp.raise_for_status()
            details = resp.json().get("data", {}).get("parcelDetails", [])
            geometry = details[0].get("geometry") if details else None
            return params, geometry, None
            
        except requests.exceptions.Timeout:
            return params, None, "timeout"
        except Exception as e:
            return params, None, str(e)
    
    # Process in smaller batches
    for i in range(0, len(uncached), BATCH_SIZE):
        batch = uncached[i:i + BATCH_SIZE]
        sys.stdout.write(f"[G:{len(batch)}]")
        sys.stdout.flush()
        
        try:
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = {executor.submit(fetch_one, r): r for r in batch}
                
                for future in as_completed(futures, timeout=BATCH_TIMEOUT):
                    try:
                        params, geometry, error = future.result(timeout=5)
                        
                        if error and error != "timeout":
                            pass  # Silent for geometry errors
                        
                        parcel_geometry_cache[params] = geometry
                        results[params] = geometry
                        
                    except TimeoutError:
                        params = futures[future]
                        parcel_geometry_cache[params] = None
                        results[params] = None
                    except Exception:
                        params = futures[future]
                        parcel_geometry_cache[params] = None
                        results[params] = None
                        
        except TimeoutError:
            for r in batch:
                if r not in results:
                    parcel_geometry_cache[r] = None
                    results[r] = None
        except Exception:
            for r in batch:
                if r not in results:
                    parcel_geometry_cache[r] = None
                    results[r] = None
    
    # Add cached results
    for r in requests_list:
        if r not in results:
            results[r] = parcel_geometry_cache.get(r)
    
    return results


# ---------------------------------------
# CSV Writing Helper
# ---------------------------------------

def append_to_csv(new_rows):
    """Append rows to CSV incrementally"""
    if not new_rows:
        return
    
    file_exists = False
    try:
        with open(OUTPUT_FILE, 'r'):
            file_exists = True
    except FileNotFoundError:
        pass
    
    with open(OUTPUT_FILE, "a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=new_rows[0].keys())
        if not file_exists:
            writer.writeheader()
        writer.writerows(new_rows)

# ---------------------------------------
# Main loop
# ---------------------------------------

start_time = time.time()

# Check which regions have already been scraped
print("🔍 Checking existing data...")
regions_status = get_all_regions_status()
completed_regions = [r for r, done in regions_status.items() if done]
regions_to_process = [r for r, done in regions_status.items() if not done]

if completed_regions:
    print(f"✅ Found complete data for regions: {completed_regions}")
    print(f"⏭️  Skipping {len(completed_regions)} completed regions")
else:
    print(f"📝 No completed regions found - starting fresh")

print(f"🎯 Will process {len(regions_to_process)} regions: {regions_to_process}\n")

for region_id in regions_to_process:
    region_start = time.time()
    print(f"\n▶ Region {region_id}")
    region_rows = []
    offset = 0
    neighborhoods_processed = 0

    while True:
        try:
            metrics_resp = session.get(
                METRICS_URL,
                params={"regionId": region_id, "offset": offset, "limit": METRICS_LIMIT},
                timeout=30
            )
            metrics_resp.raise_for_status()
        except Exception as e:
            print(f"\n⚠️  Failed to fetch metrics: {e}")
            break

        items = metrics_resp.json().get("data", {}).get("items", [])
        if not items:
            break

        if TEST:
            items = items[:5]

        for item in items:
            neighborhoods_processed += 1
            neighborhood_id = item["neighborhoodId"]
            neighborhood_name = item["neighborhoodName"]
            province_name = item["provinceName"]

            # Collect all transactions first
            all_transactions = []
            page = 0
            
            sys.stdout.write(f"\n  Neighborhood {neighborhoods_processed}: {neighborhood_name[:30]}")
            sys.stdout.flush()
            
            while True:
                try:
                    tx_resp = session.get(
                        TRANSACTIONS_URL,
                        params={
                            "regionId": region_id,
                            "neighbourhoodId": neighborhood_id,
                            "page": page,
                            "pageSize": PAGE_SIZE
                        },
                        timeout=30
                    )
                    tx_resp.raise_for_status()
                except Exception as e:
                    print(f"\n⚠️  Failed to fetch transactions: {e}")
                    break

                transactions = tx_resp.json().get("data", [])
                if not transactions:
                    break

                all_transactions.extend(transactions)
                page += 1
                
                # Show progress
                sys.stdout.write(f" [{len(all_transactions)} txs]")
                sys.stdout.flush()
                
                if TEST:
                    break
            
            # Filter out already-seen transactions
            new_transactions = []
            for tx in all_transactions:
                tx_number = tx.get("transactionNumber")
                unique_key = (region_id, neighborhood_id, tx_number)
                if unique_key not in seen_transactions:
                    seen_transactions.add(unique_key)
                    new_transactions.append(tx)
            
            if not new_transactions:
                sys.stdout.write(" (all seen)")
                sys.stdout.flush()
                continue
            
            sys.stdout.write(f" → {len(new_transactions)} new")
            sys.stdout.flush()
            
            # Batch fetch transaction details
            tx_numbers = [tx.get("transactionNumber") for tx in new_transactions]
            details_map = get_transaction_details_batch(region_id, tx_numbers)
            
            # Prepare parcel geometry requests
            geometry_requests = []
            for tx in new_transactions:
                tx_number = tx.get("transactionNumber")
                details = details_map.get(tx_number, {})
                
                parcel_no = tx.get("parcelNo")
                subdivision_no = tx.get("subdivisionNo")
                province_id = details.get("provinceId")
                
                if not details.get("geometry") and parcel_no and subdivision_no and province_id:
                    geometry_requests.append((region_id, province_id, subdivision_no, parcel_no))
            
            # Batch fetch geometries
            geometry_map = {}
            if geometry_requests:
                geometry_map = get_parcel_geometry_batch(geometry_requests)
            
            # Build rows
            for tx in new_transactions:
                tx_number = tx.get("transactionNumber")
                details = details_map.get(tx_number, {})
                
                parcel_no = tx.get("parcelNo")
                subdivision_no = tx.get("subdivisionNo")
                province_id = details.get("provinceId")
                
                # Extract coordinates
                centroid_x = details.get("centroidX")
                centroid_y = details.get("centroidY")
                
                # Try to get centroid from nested structure if not at level
                if not centroid_x and details.get("centroid"):
                    centroid_x = details["centroid"].get("x")
                    centroid_y = details["centroid"].get("y")
                
                # Try to get from parcels array
                if not centroid_x and details.get("parcels") and len(details["parcels"]) > 0:
                    first_parcel = details["parcels"][0]
                    centroid_x = first_parcel.get("centroidX")
                    centroid_y = first_parcel.get("centroidY")
                
                # Get polygon data if available
                polygon_data = details.get("polygonData")
                
                # Fallback to geometry lookup if no coordinates
                geometry = details.get("geometry")
                if not geometry and not centroid_x and parcel_no and subdivision_no and province_id:
                    key = (region_id, province_id, subdivision_no, parcel_no)
                    geometry = geometry_map.get(key)
                
                row = {
                    "regionId": region_id,
                    "provinceName": province_name,
                    "neighborhoodId": neighborhood_id,
                    "neighborhoodName": neighborhood_name,
                    "رقم الصفقة": tx_number,
                    "رقم المخطط": subdivision_no,
                    "رقم البلوك": tx.get("blockNo") or "---",
                    "رقم القطعة": parcel_no,
                    "قيمة الصفقة (﷼)": tx.get("transactionPrice"),
                    "سعر المتر (﷼)": tx.get("priceOfMeter"),
                    "تاريخ الصفقة": tx.get("transactionDate"),
                    "نوع الأرض": details.get("type"),
                    "نوع الاستخدام": details.get("metricsType"),
                    "المساحة الإجمالية": details.get("totalArea"),
                    "المصدر": details.get("transactionSource"),
                    "sellingType": details.get("sellingType"),
                    "landUseGroup": details.get("landUseGroup"),
                    "propertyType": details.get("propertyType"),
                    "centroidX": centroid_x,
                    "centroidY": centroid_y,
                    "polygonData": polygon_data,
                    "geometry": json.dumps(geometry, ensure_ascii=False) if geometry else None
                }
                
                rows.append(row)
                region_rows.append(row)

        offset += METRICS_LIMIT
        if TEST:
            break
    
    # Write region data to CSV immediately
    if region_rows:
        append_to_csv(region_rows)
        region_elapsed = time.time() - region_start
        print(f"\n  ✓ Region {region_id} done: {len(region_rows)} rows in {region_elapsed:.1f}s")
    else:
        print(f"\n  ✓ Region {region_id} done: no new data")

elapsed = time.time() - start_time
print(f"\n{'='*60}")
print(f"✅ All regions finished in {elapsed:.1f}s")
print(f"📊 Total unique transactions: {len(rows)}")
print(f"📄 Data saved to {OUTPUT_FILE}")

if completed_regions:
    print(f"⏭️  Skipped {len(completed_regions)} previously completed regions")