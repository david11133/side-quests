import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import csv
import sys
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Set, Tuple
import time
from collections import deque

# API Endpoints
METRICS_URL = "https://api2.suhail.ai/api/mapMetrics/landMetrics/list"
TRANSACTIONS_URL = "https://api2.suhail.ai/transactions/neighbourhood"
TX_DETAILS_URL = "https://api2.suhail.ai/api/transactions/search"
PARCEL_URL = "https://api2.suhail.ai/api/parcel/search"

# Configuration
REGION_IDS = range(1, 16)
METRICS_LIMIT = 600
PAGE_SIZE = 1000
OUTPUT_FILE = "neighborhood_transactions.csv"
TEST = False  # Set to True for testing with limited data

# Performance settings
MAX_WORKERS = 6
BATCH_SIZE = 25
REQUEST_TIMEOUT = 15
CACHE_SIZE_LIMIT = 10000  # Limit cache size to prevent memory issues

# ---------------------------------------
# Session with connection pooling
# ---------------------------------------

def create_session():
    """Create a requests session with retry logic and connection pooling"""
    session = requests.Session()
    
    adapter = HTTPAdapter(
        pool_connections=10,
        pool_maxsize=10,
        max_retries=Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[500, 502, 503, 504, 429],
            raise_on_status=False
        )
    )
    
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    
    return session

session = create_session()

# Limited-size caches with LRU-like behavior
class LimitedCache:
    """Simple LRU-like cache with size limit"""
    def __init__(self, max_size=1000):
        self.cache = {}
        self.access_order = deque()
        self.max_size = max_size
    
    def get(self, key, default=None):
        return self.cache.get(key, default)
    
    def set(self, key, value):
        if key not in self.cache and len(self.cache) >= self.max_size:
            # Remove oldest entry
            if self.access_order:
                old_key = self.access_order.popleft()
                self.cache.pop(old_key, None)
        
        self.cache[key] = value
        if key in self.access_order:
            self.access_order.remove(key)
        self.access_order.append(key)
    
    def __contains__(self, key):
        return key in self.cache

# Caches
transaction_details_cache = LimitedCache(max_size=CACHE_SIZE_LIMIT)
parcel_geometry_cache = LimitedCache(max_size=CACHE_SIZE_LIMIT)
seen_transactions: Set[Tuple] = set()

# ---------------------------------------
# Helper Functions
# ---------------------------------------

def is_valid_subdivision(subdivision_no):
    """Check if subdivision number is valid for API call"""
    return subdivision_no and str(subdivision_no).strip().isdigit()

def needs_details_fetch(tx):
    """Determine if we need to fetch transaction details"""
    # Check if transaction is missing critical detail fields
    # If the transaction endpoint already provides these, skip the details fetch
    
    # Fields that typically come from details endpoint:
    # type, metricsType, totalArea, transactionSource, sellingType, landUseGroup, propertyType
    
    # If transaction already has most of these fields, we don't need details
    has_type = tx.get("type") is not None
    has_metrics_type = tx.get("metricsType") is not None
    has_total_area = tx.get("totalArea") is not None
    
    # If we have these key fields, skip details fetch
    if has_type and has_metrics_type:
        return False
    
    # Otherwise, fetch details
    return True

def extract_coordinates(tx, details):
    """Extract coordinates from transaction or details"""
    # Try transaction first
    centroid_x = tx.get("centroidX")
    centroid_y = tx.get("centroidY")
    
    if centroid_x:
        return centroid_x, centroid_y
    
    # Try details at top level
    centroid_x = details.get("centroidX")
    centroid_y = details.get("centroidY")
    
    if centroid_x:
        return centroid_x, centroid_y
    
    # Try nested centroid
    if details.get("centroid"):
        centroid_x = details["centroid"].get("x")
        centroid_y = details["centroid"].get("y")
        if centroid_x:
            return centroid_x, centroid_y
    
    # Try parcels array
    if details.get("parcels") and len(details["parcels"]) > 0:
        first_parcel = details["parcels"][0]
        centroid_x = first_parcel.get("centroidX")
        centroid_y = first_parcel.get("centroidY")
        if centroid_x:
            return centroid_x, centroid_y
    
    return None, None

def needs_geometry_fetch(tx, details, centroid_x):
    """Determine if we need to fetch parcel geometry"""
    # If we already have coordinates or geometry, skip
    if centroid_x:
        return False
    
    if details.get("geometry") or details.get("polygonData"):
        return False
    
    # If we don't have the required fields for geometry lookup, skip
    if not (tx.get("parcelNo") and tx.get("subdivisionNo")):
        return False
    
    return True

def fetch_transaction_details(region_id: int, tx_number: str) -> dict:
    """Fetch details for a single transaction"""
    try:
        resp = session.get(
            TX_DETAILS_URL,
            params={"transactionNo": tx_number, "regionId": region_id},
            timeout=REQUEST_TIMEOUT
        )
        resp.raise_for_status()
        data = resp.json().get("data", [])
        return data[0] if data else {}
    except Exception as e:
        sys.stdout.write(f"!")
        sys.stdout.flush()
        return {}

def fetch_parcel_geometry(region_id: int, province_id: int, subdivision_no: str, parcel_no: str) -> Optional[dict]:
    """Fetch geometry for a single parcel"""
    if not is_valid_subdivision(subdivision_no):
        return None
    
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
            return None
        
        resp.raise_for_status()
        details = resp.json().get("data", {}).get("parcelDetails", [])
        return details[0].get("geometry") if details else None
        
    except Exception:
        return None

def batch_fetch_details(region_id: int, tx_list: List[dict]) -> Dict[str, dict]:
    """Fetch details for transactions in batch"""
    results = {}
    to_fetch = []
    
    # Check cache first
    for tx in tx_list:
        tx_number = tx.get("transactionNumber")
        if tx_number in transaction_details_cache:
            results[tx_number] = transaction_details_cache.get(tx_number)
        else:
            to_fetch.append(tx_number)
    
    if not to_fetch:
        return results
    
    sys.stdout.write(f"[D:{len(to_fetch)}]")
    sys.stdout.flush()
    
    # Process in smaller batches to avoid timeout issues
    for i in range(0, len(to_fetch), BATCH_SIZE):
        batch = to_fetch[i:i + BATCH_SIZE]
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_tx = {
                executor.submit(fetch_transaction_details, region_id, tx_num): tx_num 
                for tx_num in batch
            }
            
            try:
                for future in as_completed(future_to_tx, timeout=45):
                    tx_number = future_to_tx[future]
                    try:
                        details = future.result(timeout=5)
                        transaction_details_cache.set(tx_number, details)
                        results[tx_number] = details
                    except Exception:
                        transaction_details_cache.set(tx_number, {})
                        results[tx_number] = {}
            except Exception as e:
                # If batch times out, mark remaining as empty
                for tx_num in batch:
                    if tx_num not in results:
                        transaction_details_cache.set(tx_num, {})
                        results[tx_num] = {}
    
    return results

def batch_fetch_geometries(requests_list: List[Tuple]) -> Dict[Tuple, Optional[dict]]:
    """Fetch geometries only for parcels that need them"""
    results = {}
    to_fetch = []
    
    # Filter cached
    for req in requests_list:
        if req in parcel_geometry_cache:
            results[req] = parcel_geometry_cache.get(req)
        else:
            to_fetch.append(req)
    
    if not to_fetch:
        return results
    
    sys.stdout.write(f"[G:{len(to_fetch)}]")
    sys.stdout.flush()
    
    # Process in smaller batches to avoid timeout issues
    for i in range(0, len(to_fetch), BATCH_SIZE):
        batch = to_fetch[i:i + BATCH_SIZE]
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_req = {
                executor.submit(fetch_parcel_geometry, *req): req 
                for req in batch
            }
            
            try:
                for future in as_completed(future_to_req, timeout=45):
                    req = future_to_req[future]
                    try:
                        geometry = future.result(timeout=5)
                        parcel_geometry_cache.set(req, geometry)
                        results[req] = geometry
                    except Exception:
                        parcel_geometry_cache.set(req, None)
                        results[req] = None
            except Exception:
                # If batch times out, mark remaining as None
                for req in batch:
                    if req not in results:
                        parcel_geometry_cache.set(req, None)
                        results[req] = None
    
    return results

def append_to_csv(new_rows):
    """Append rows to CSV file"""
    if not new_rows:
        return
    
    try:
        with open(OUTPUT_FILE, 'r'):
            file_exists = True
    except FileNotFoundError:
        file_exists = False
    
    with open(OUTPUT_FILE, "a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=new_rows[0].keys())
        if not file_exists:
            writer.writeheader()
        writer.writerows(new_rows)

def build_row(region_id, province_name, neighborhood_id, neighborhood_name, tx, details, geometry=None):
    """Build a CSV row from transaction data - ENSURES ALL FIELDS ARE PRESENT"""
    tx_number = tx.get("transactionNumber")
    subdivision_no = tx.get("subdivisionNo")
    parcel_no = tx.get("parcelNo")
    
    # Extract coordinates
    centroid_x, centroid_y = extract_coordinates(tx, details)
    
    # Get polygon and geometry data
    polygon_data = details.get("polygonData") if details else None
    geom = (details.get("geometry") if details else None) or geometry
    
    # CRITICAL: Always provide all fields with proper defaults
    # This ensures CSV columns are never misaligned
    return {
        "regionId": region_id,
        "provinceName": province_name,
        "neighborhoodId": neighborhood_id,
        "neighborhoodName": neighborhood_name,
        "رقم الصفقة": tx_number or "",
        "رقم المخطط": subdivision_no or "",
        "رقم البلوك": tx.get("blockNo") or "---",
        "رقم القطعة": parcel_no or "",
        "قيمة الصفقة (ï·¼)": tx.get("transactionPrice") or "",
        "سعر المتر (ï·¼)": tx.get("priceOfMeter") or "",
        "تاريخ الصفقة": tx.get("transactionDate") or "",
        "نوع الأرض": details.get("type") if details else "",
        "نوع الاستخدام": details.get("metricsType") if details else "",
        "المساحة الإجمالية": details.get("totalArea") if details else "",
        "المصدر": details.get("transactionSource") if details else "",
        "sellingType": details.get("sellingType") if details else "",
        "landUseGroup": details.get("landUseGroup") if details else "",
        "propertyType": details.get("propertyType") if details else "",
        "centroidX": centroid_x or "",
        "centroidY": centroid_y or "",
        "polygonData": polygon_data or "",
        "geometry": json.dumps(geom, ensure_ascii=False) if geom else ""
    }

# ---------------------------------------
# Main Processing Loop
# ---------------------------------------

def process_region(region_id):
    """Process all neighborhoods in a region"""
    region_start = time.time()
    print(f"\n▶ Region {region_id}")
    region_rows = []
    offset = 0
    neighborhoods_processed = 0

    while True:
        # Fetch metrics (neighborhoods)
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
            items = items[:3]

        for item in items:
            neighborhoods_processed += 1
            neighborhood_id = item["neighborhoodId"]
            neighborhood_name = item["neighborhoodName"]
            province_name = item["provinceName"]
            province_id = item.get("provinceId")

            sys.stdout.write(f"\n  {neighborhoods_processed}. {neighborhood_name[:40]}")
            sys.stdout.flush()

            # Fetch all transaction pages
            all_transactions = []
            page = 0
            
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
                
                if TEST and page >= 1:
                    break
            
            if not all_transactions:
                sys.stdout.write(" (no txs)")
                sys.stdout.flush()
                continue
            
            # Filter out duplicates BEFORE fetching details
            new_transactions = []
            for tx in all_transactions:
                tx_number = tx.get("transactionNumber")
                unique_key = (region_id, neighborhood_id, tx_number)
                if unique_key not in seen_transactions:
                    seen_transactions.add(unique_key)
                    new_transactions.append(tx)
            
            if not new_transactions:
                sys.stdout.write(f" ({len(all_transactions)} txs, all seen)")
                sys.stdout.flush()
                continue
            
            sys.stdout.write(f" ({len(new_transactions)}/{len(all_transactions)} new)")
            sys.stdout.flush()
            
            # Filter transactions that actually need details fetch
            tx_needing_details = [tx for tx in new_transactions if needs_details_fetch(tx)]
            
            # Fetch details only for transactions that need them
            details_map = {}
            if tx_needing_details:
                details_map = batch_fetch_details(region_id, tx_needing_details)
            
            # For transactions that didn't need fetch, use the transaction data itself
            for tx in new_transactions:
                tx_number = tx.get("transactionNumber")
                if tx_number not in details_map:
                    # Use transaction data as "details"
                    details_map[tx_number] = {
                        "type": tx.get("type"),
                        "metricsType": tx.get("metricsType"),
                        "totalArea": tx.get("totalArea"),
                        "transactionSource": tx.get("transactionSource"),
                        "sellingType": tx.get("sellingType"),
                        "landUseGroup": tx.get("landUseGroup"),
                        "propertyType": tx.get("propertyType"),
                        "centroidX": tx.get("centroidX"),
                        "centroidY": tx.get("centroidY"),
                        "geometry": tx.get("geometry"),
                        "polygonData": tx.get("polygonData"),
                        "provinceId": tx.get("provinceId")
                    }
            
            # Determine which transactions need geometry lookups
            geometry_requests = []
            for tx in new_transactions:
                tx_number = tx.get("transactionNumber")
                details = details_map.get(tx_number, {})
                
                # Extract coordinates to check if we need geometry
                centroid_x, _ = extract_coordinates(tx, details)
                
                # Only fetch geometry if needed
                if needs_geometry_fetch(tx, details, centroid_x):
                    parcel_no = tx.get("parcelNo")
                    subdivision_no = tx.get("subdivisionNo")
                    detail_province_id = details.get("provinceId") if details else None
                    final_province_id = detail_province_id or province_id
                    
                    if parcel_no and subdivision_no and final_province_id:
                        req = (region_id, final_province_id, subdivision_no, parcel_no)
                        geometry_requests.append((req, tx_number))
            
            # Fetch geometries if needed
            geometry_map = {}
            if geometry_requests:
                unique_requests = list(set([req for req, _ in geometry_requests]))
                geometry_results = batch_fetch_geometries(unique_requests)
                
                # Map back to transaction numbers
                for req, tx_number in geometry_requests:
                    geometry_map[tx_number] = geometry_results.get(req)
            
            # Build rows
            for tx in new_transactions:
                tx_number = tx.get("transactionNumber")
                details = details_map.get(tx_number, {})
                geometry = geometry_map.get(tx_number)
                
                row = build_row(
                    region_id, province_name, neighborhood_id, 
                    neighborhood_name, tx, details, geometry
                )
                
                region_rows.append(row)

        offset += METRICS_LIMIT
        if TEST:
            break
    
    # Write region data
    if region_rows:
        append_to_csv(region_rows)
        region_elapsed = time.time() - region_start
        print(f"\n  ✓ Region {region_id}: {len(region_rows)} rows in {region_elapsed:.1f}s")
    else:
        print(f"\n  ✓ Region {region_id}: no new data")
    
    return len(region_rows)

# ---------------------------------------
# Main Execution
# ---------------------------------------

if __name__ == "__main__":
    start_time = time.time()
    total_rows = 0
    
    print(f"Starting scraper (TEST mode: {TEST})")
    print(f"Output file: {OUTPUT_FILE}")
    print("="*60)
    
    for region_id in REGION_IDS:
        try:
            rows_added = process_region(region_id)
            total_rows += rows_added
        except KeyboardInterrupt:
            print("\n\n⚠️  Interrupted by user")
            break
        except Exception as e:
            print(f"\n⚠️  Error processing region {region_id}: {e}")
            continue
    
    elapsed = time.time() - start_time
    print(f"\n{'='*60}")
    print(f"✅ Completed in {elapsed:.1f}s ({elapsed/60:.1f} minutes)")
    print(f"📊 Total transactions: {total_rows}")
    print(f"📄 Data saved to {OUTPUT_FILE}")