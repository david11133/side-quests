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
from dataclasses import dataclass, asdict

# API Endpoints
REGIONS_URL = "https://api2.suhail.ai/regions"
METRICS_URL = "https://api2.suhail.ai/api/mapMetrics/landMetrics/list"
PARCEL_SEARCH_URL = "https://api2.suhail.ai/api/parcel/search"
CONSOLIDATED_TX_URL = "https://api2.suhail.ai/consolidatedTransactions"
PARCEL_METRICS_URL = "https://api2.suhail.ai/api/parcel/metrics/priceOfMeter"

# Configuration
REGION_IDS = range(1, 17)
METRICS_LIMIT = 600
PAGE_SIZE = 1000
TEST = False

# Output files
PARCELS_OUTPUT = "parcels.csv"
TRANSACTIONS_OUTPUT = "transactions.csv"
METRICS_OUTPUT = "parcel_metrics.csv"

# Performance settings
MAX_WORKERS = 6
BATCH_SIZE = 25
REQUEST_TIMEOUT = 15
CACHE_SIZE_LIMIT = 10000

# ---------------------------------------
# Data Models
# ---------------------------------------

@dataclass
class ParcelRecord:
    parcelObjectId: int
    parcelId: str
    parcelNo: str
    blockNo: Optional[str]
    subdivisionNo: Optional[str]
    area: Optional[float]
    propertyType: Optional[str]
    metricsType: Optional[str]
    landUseGroup: Optional[str]
    centroidX: Optional[float]
    centroidY: Optional[float]
    polygonData: Optional[str]
    geometry: Optional[str]
    regionId: int
    provinceId: Optional[int]
    provinceName: Optional[str]
    neighborhoodId: int
    neighborhoodName: str
    parcelImageURL: Optional[str]
    hasTransactions: bool = False

@dataclass
class TransactionRecord:
    transactionNumber: int
    parcelObjectId: int
    parcelId: str
    transactionDate: str
    transactionPrice: Optional[float]
    priceOfMeter: Optional[float]
    transactionSource: Optional[str]
    sellingType: Optional[str]
    type: Optional[str]
    totalArea: Optional[float]
    noOfProperties: Optional[int]
    propertyType: Optional[str]
    metricsType: Optional[str]
    landUseGroup: Optional[str]
    landUsageGroup: Optional[str]
    zoningId: Optional[int]
    isProjectParcel: bool
    projectId: Optional[int]
    projectName: Optional[str]
    buyerName: Optional[str]
    transactionYear: int
    isLowValueTransaction: bool
    regionId: int
    provinceId: Optional[int]
    neighborhoodId: int
    neighborhoodName: str
    subdivisionNo: Optional[str]
    parcelNo: str
    blockNo: Optional[str]

@dataclass
class MetricRecord:
    parcelObjectId: int
    neighborhoodId: int
    month: int
    year: int
    metricsType: str
    averagePriceOfMeter: float

# ---------------------------------------
# Session with connection pooling
# ---------------------------------------

def create_session():
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

# ---------------------------------------
# Cache Implementation
# ---------------------------------------

class LimitedCache:
    def __init__(self, max_size=1000):
        self.cache = {}
        self.access_order = deque()
        self.max_size = max_size
    
    def get(self, key, default=None):
        return self.cache.get(key, default)
    
    def set(self, key, value):
        if key not in self.cache and len(self.cache) >= self.max_size:
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
parcel_cache = LimitedCache(max_size=CACHE_SIZE_LIMIT)
transactions_cache = LimitedCache(max_size=CACHE_SIZE_LIMIT)
metrics_cache = LimitedCache(max_size=CACHE_SIZE_LIMIT)
seen_parcels: Set[int] = set()

# ---------------------------------------
# Data Fetching Functions
# ---------------------------------------

def fetch_regions():
    response = session.get(REGIONS_URL, timeout=30)
    response.raise_for_status()
    regions = response.json()["data"]
    
    region_dict = {}
    province_dict = {}
    
    for region in regions:
        region_id = region.get("id")
        region_dict[region_id] = region
        for province in region.get("provinces", []):
            province_id = province.get("id")
            province_dict[province_id] = province
    
    return region_dict, province_dict

def fetch_parcels_for_neighborhood(region_id: int, province_id: int, neighborhood_id: int, subdivision_no: str = None, parcel_no: str = None) -> List[dict]:
    """Fetch parcels for a neighborhood using parcel search endpoint"""
    params = {
        "regionId": region_id,
        "provinceId": province_id,
        "offset": 0,
        "limit": 1000
    }
    
    if subdivision_no:
        params["subdivisionNo"] = subdivision_no
    if parcel_no:
        params["parcelNo"] = parcel_no
    
    try:
        resp = session.get(PARCEL_SEARCH_URL, params=params, timeout=REQUEST_TIMEOUT)
        if resp.status_code in (404, 410):
            return []
        resp.raise_for_status()
        
        data = resp.json().get("data", {})
        return data.get("parcelDetails", [])
    except Exception as e:
        sys.stdout.write("!")
        sys.stdout.flush()
        return []

def fetch_consolidated_transactions(parcel_object_id: int, region_id: int, lookback_value: int = 12, lookback_type: str = "months") -> dict:
    """Fetch consolidated transactions for a parcel"""
    cache_key = (parcel_object_id, region_id)
    
    if cache_key in transactions_cache:
        return transactions_cache.get(cache_key)
    
    params = {
        "ParcelObjectId": parcel_object_id,
        "RegionId": region_id,
        "LookbackValue": lookback_value,
        "LookbackType": lookback_type,
        "FromPrice": 0,
        "ToPrice": 100000000,
        "Type": "الكل"
    }
    
    try:
        resp = session.get(CONSOLIDATED_TX_URL, params=params, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        result = resp.json().get("data", {})
        transactions_cache.set(cache_key, result)
        return result
    except Exception:
        transactions_cache.set(cache_key, {"transactions": []})
        return {"transactions": []}

def fetch_parcel_metrics(parcel_object_id: int, grouping_type: str = "Monthly") -> List[dict]:
    """Fetch price metrics for a parcel"""
    cache_key = (parcel_object_id, grouping_type)
    
    if cache_key in metrics_cache:
        return metrics_cache.get(cache_key)
    
    params = {
        "parcelObjsIds": parcel_object_id,
        "groupingType": grouping_type
    }
    
    try:
        resp = session.get(PARCEL_METRICS_URL, params=params, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        result = resp.json().get("data", [])
        metrics_cache.set(cache_key, result)
        return result
    except Exception:
        metrics_cache.set(cache_key, [])
        return []

# ---------------------------------------
# Batch Processing Functions
# ---------------------------------------

def batch_fetch_transactions(parcel_ids: List[Tuple[int, int]]) -> Dict[Tuple[int, int], dict]:
    """Batch fetch consolidated transactions for multiple parcels"""
    results = {}
    to_fetch = []
    
    for parcel_obj_id, region_id in parcel_ids:
        cache_key = (parcel_obj_id, region_id)
        if cache_key in transactions_cache:
            results[cache_key] = transactions_cache.get(cache_key)
        else:
            to_fetch.append((parcel_obj_id, region_id))
    
    if not to_fetch:
        return results
    
    sys.stdout.write(f"[T:{len(to_fetch)}]")
    sys.stdout.flush()
    
    for i in range(0, len(to_fetch), BATCH_SIZE):
        batch = to_fetch[i:i + BATCH_SIZE]
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_parcel = {
                executor.submit(fetch_consolidated_transactions, p_id, r_id): (p_id, r_id)
                for p_id, r_id in batch
            }
            
            try:
                for future in as_completed(future_to_parcel, timeout=45):
                    cache_key = future_to_parcel[future]
                    try:
                        data = future.result(timeout=5)
                        results[cache_key] = data
                    except Exception:
                        results[cache_key] = {"transactions": []}
            except Exception:
                for cache_key in batch:
                    if cache_key not in results:
                        results[cache_key] = {"transactions": []}
    
    return results

def batch_fetch_metrics(parcel_ids: List[int]) -> Dict[int, List[dict]]:
    """Batch fetch metrics for multiple parcels"""
    results = {}
    to_fetch = []
    
    for parcel_obj_id in parcel_ids:
        cache_key = (parcel_obj_id, "Monthly")
        if cache_key in metrics_cache:
            results[parcel_obj_id] = metrics_cache.get(cache_key)
        else:
            to_fetch.append(parcel_obj_id)
    
    if not to_fetch:
        return results
    
    sys.stdout.write(f"[M:{len(to_fetch)}]")
    sys.stdout.flush()
    
    for i in range(0, len(to_fetch), BATCH_SIZE):
        batch = to_fetch[i:i + BATCH_SIZE]
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_parcel = {
                executor.submit(fetch_parcel_metrics, p_id): p_id
                for p_id in batch
            }
            
            try:
                for future in as_completed(future_to_parcel, timeout=45):
                    parcel_obj_id = future_to_parcel[future]
                    try:
                        data = future.result(timeout=5)
                        results[parcel_obj_id] = data
                    except Exception:
                        results[parcel_obj_id] = []
            except Exception:
                for parcel_obj_id in batch:
                    if parcel_obj_id not in results:
                        results[parcel_obj_id] = []
    
    return results

# ---------------------------------------
# Data Parsing Functions
# ---------------------------------------

def parse_parcel(parcel_data: dict, tx_data: dict, region_id: int, province_id: int, province_name: str, neighborhood_id: int, neighborhood_name: str, has_transactions: bool = False) -> ParcelRecord:
    """Parse raw parcel data into ParcelRecord"""
    # Get polygonData from transaction data (it's there, not in parcel_data)
    polygon_data = tx_data.get("polygonData") or parcel_data.get("polygonData")
    if isinstance(polygon_data, dict):
        polygon_data = json.dumps(polygon_data, ensure_ascii=False)
    elif isinstance(polygon_data, str) and polygon_data:
        # Already a string, keep it
        pass
    else:
        polygon_data = ""
    
    geometry = parcel_data.get("geometry") or tx_data.get("geometry")
    if isinstance(geometry, dict):
        geometry = json.dumps(geometry, ensure_ascii=False)
    elif not geometry:
        geometry = ""
    
    # Get subdivisionNo and blockNo from transaction data
    subdivision_no = tx_data.get("subdivisionNo") or parcel_data.get("subdivisionNo")
    block_no = tx_data.get("blockNo") or parcel_data.get("blockNo")
    
    # Handle "---" as empty blockNo (from old script)
    if block_no == "---":
        block_no = None
    
    return ParcelRecord(
        parcelObjectId=parcel_data.get("parcelObjectId"),
        parcelId=str(parcel_data.get("parcelId", "")),
        parcelNo=parcel_data.get("parcelNo", ""),
        blockNo=block_no,
        subdivisionNo=subdivision_no,
        area=parcel_data.get("area"),
        propertyType=parcel_data.get("propertyType"),
        metricsType=parcel_data.get("metricsType"),
        landUseGroup=parcel_data.get("landUseGroup"),
        centroidX=parcel_data.get("centroidX"),
        centroidY=parcel_data.get("centroidY"),
        polygonData=polygon_data,
        geometry=geometry,
        regionId=region_id,
        provinceId=province_id,
        provinceName=province_name,
        neighborhoodId=neighborhood_id,
        neighborhoodName=neighborhood_name,
        parcelImageURL=parcel_data.get("parcelImageURL"),
        hasTransactions=has_transactions
    )

def parse_transaction(tx_data: dict, parcel_object_id: int, region_id: int, province_id: int, neighborhood_id: int, neighborhood_name: str) -> TransactionRecord:
    """Parse raw transaction data into TransactionRecord"""
    return TransactionRecord(
        transactionNumber=tx_data.get("transactionNumber"),
        parcelObjectId=parcel_object_id,
        parcelId=str(tx_data.get("parcelId", "")),
        transactionDate=tx_data.get("transactionDate", ""),
        transactionPrice=tx_data.get("transactionPrice"),
        priceOfMeter=tx_data.get("priceOfMeter") or tx_data.get("_priceOfMeter"),
        transactionSource=tx_data.get("transactionSource"),
        sellingType=tx_data.get("sellingType"),
        type=tx_data.get("type"),
        totalArea=tx_data.get("totalArea") or tx_data.get("area"),
        noOfProperties=tx_data.get("noOfProperties"),
        propertyType=tx_data.get("propertyType"),
        metricsType=tx_data.get("metricsType"),
        landUseGroup=tx_data.get("landUseGroup"),
        landUsageGroup=tx_data.get("landUsageGroup"),
        zoningId=tx_data.get("zoningId"),
        isProjectParcel=tx_data.get("isProjectParcel", False),
        projectId=tx_data.get("projectId"),
        projectName=tx_data.get("projectName", ""),
        buyerName=tx_data.get("buyerName"),
        transactionYear=tx_data.get("transactionYear", 0),
        isLowValueTransaction=tx_data.get("isLowValueTransaction", False),
        regionId=region_id,
        provinceId=province_id,
        neighborhoodId=neighborhood_id,
        neighborhoodName=neighborhood_name,
        subdivisionNo=tx_data.get("subdivisionNo"),
        parcelNo=tx_data.get("parcelNo", ""),
        blockNo=tx_data.get("blockNo")
    )

def parse_metrics(metrics_data: List[dict], parcel_object_id: int) -> List[MetricRecord]:
    """Parse raw metrics data into MetricRecord list"""
    records = []
    
    for item in metrics_data:
        neighborhood_metrics = item.get("neighborhoodMetrics", [])
        for metric in neighborhood_metrics:
            records.append(MetricRecord(
                parcelObjectId=parcel_object_id,
                neighborhoodId=metric.get("neighborhoodId"),
                month=metric.get("month"),
                year=metric.get("year"),
                metricsType=metric.get("metricsType", ""),
                averagePriceOfMeter=metric.get("avaragePriceOfMeter", 0.0)
            ))
    
    return records

# ---------------------------------------
# CSV Writing Functions
# ---------------------------------------

def append_to_csv(filename: str, rows: List[dict]):
    """Append rows to CSV file"""
    if not rows:
        return
    
    try:
        with open(filename, 'r'):
            file_exists = True
    except FileNotFoundError:
        file_exists = False
    
    with open(filename, "a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)

# ---------------------------------------
# Main Processing Loop
# ---------------------------------------

def process_neighborhood(region_id: int, province_id: int, province_name: str, neighborhood_id: int, neighborhood_name: str) -> Tuple[int, int, int]:
    """Process a single neighborhood and return counts of parcels, transactions, metrics"""
    
    # We need to discover parcels - the API requires subdivision/parcel lookup
    # Since we don't have parcel inventory, we'll rely on transaction-discovered parcels
    # This is a limitation of the API structure
    
    # For now, we'll extract parcels from the transactions endpoint
    # In a real scenario, you'd need a parcel discovery mechanism
    
    parcel_records = []
    transaction_records = []
    metric_records = []
    
    # Note: The API doesn't provide a direct parcel listing endpoint
    # We need to either:
    # 1. Use transaction data to discover parcels (current approach)
    # 2. Use subdivision/parcel number ranges to probe
    # 3. Parse vector tiles (which you've excluded)
    
    # For this implementation, we'll use the neighborhood transactions to discover parcels
    # Then fetch consolidated data for each parcel
    
    page = 0
    discovered_parcel_ids = set()
    
    # First pass: discover parcels through transactions
    while True:
        try:
            tx_resp = session.get(
                "https://api2.suhail.ai/transactions/neighbourhood",
                params={
                    "regionId": region_id,
                    "neighbourhoodId": neighborhood_id,
                    "page": page,
                    "pageSize": PAGE_SIZE
                },
                timeout=30
            )
            tx_resp.raise_for_status()
        except Exception:
            break
        
        transactions = tx_resp.json().get("data", [])
        if not transactions:
            break
        
        for tx in transactions:
            parcel_obj_id = tx.get("parcelObjectId")
            if parcel_obj_id and parcel_obj_id not in seen_parcels:
                discovered_parcel_ids.add(parcel_obj_id)
        
        page += 1
        if TEST and page >= 1:
            break
    
    if not discovered_parcel_ids:
        return 0, 0, 0
    
    discovered_list = list(discovered_parcel_ids)
    
    # Batch fetch consolidated transactions
    tx_requests = [(p_id, region_id) for p_id in discovered_list]
    tx_results = batch_fetch_transactions(tx_requests)
    
    # Batch fetch metrics
    metrics_results = batch_fetch_metrics(discovered_list)
    
    # Process results
    for parcel_obj_id in discovered_list:
        if parcel_obj_id in seen_parcels:
            continue
        
        seen_parcels.add(parcel_obj_id)
        
        # Get consolidated transactions
        tx_data = tx_results.get((parcel_obj_id, region_id), {})
        transactions = tx_data.get("transactions", [])
        
        # Create parcel record from first transaction if available
        if transactions:
            first_tx = transactions[0]
            parcels_in_tx = first_tx.get("parcels", [])
            
            if parcels_in_tx:
                parcel_info = parcels_in_tx[0]
            else:
                parcel_info = first_tx
            
            # Pass both parcel_info and first_tx to get all fields
            parcel_record = parse_parcel(
                parcel_info,
                first_tx,  # Pass transaction data to extract missing fields
                region_id,
                province_id,
                province_name,
                neighborhood_id,
                neighborhood_name,
                has_transactions=True
            )
            parcel_records.append(asdict(parcel_record))
            
            # Parse all transactions
            for tx in transactions:
                tx_record = parse_transaction(
                    tx,
                    parcel_obj_id,
                    region_id,
                    province_id,
                    neighborhood_id,
                    neighborhood_name
                )
                transaction_records.append(asdict(tx_record))
        
        # Parse metrics
        metrics_data = metrics_results.get(parcel_obj_id, [])
        for metric in parse_metrics(metrics_data, parcel_obj_id):
            metric_records.append(asdict(metric))
    
    # Write to CSV files
    if parcel_records:
        append_to_csv(PARCELS_OUTPUT, parcel_records)
    if transaction_records:
        append_to_csv(TRANSACTIONS_OUTPUT, transaction_records)
    if metric_records:
        append_to_csv(METRICS_OUTPUT, metric_records)
    
    return len(parcel_records), len(transaction_records), len(metric_records)

def process_region(region_id: int, region_dict: dict, province_dict: dict) -> Tuple[int, int, int]:
    """Process all neighborhoods in a region"""
    region_start = time.time()
    print(f"\n▶ Region {region_id}")
    
    total_parcels = 0
    total_transactions = 0
    total_metrics = 0
    
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
            items = items[:3]
        
        for item in items:
            neighborhoods_processed += 1
            neighborhood_id = item["neighborhoodId"]
            neighborhood_name = item["neighborhoodName"]
            province_name = item["provinceName"]
            province_id = item.get("provinceId")
            
            sys.stdout.write(f"\n  {neighborhoods_processed}. {neighborhood_name[:40]}")
            sys.stdout.flush()
            
            try:
                p_count, t_count, m_count = process_neighborhood(
                    region_id,
                    province_id,
                    province_name,
                    neighborhood_id,
                    neighborhood_name
                )
                
                total_parcels += p_count
                total_transactions += t_count
                total_metrics += m_count
                
                sys.stdout.write(f" [P:{p_count} T:{t_count} M:{m_count}]")
                sys.stdout.flush()
                
            except Exception as e:
                sys.stdout.write(f" ERROR: {e}")
                sys.stdout.flush()
        
        offset += METRICS_LIMIT
        if TEST:
            break
    
    region_elapsed = time.time() - region_start
    print(f"\n  ✓ Region {region_id}: {total_parcels} parcels, {total_transactions} transactions, {total_metrics} metrics in {region_elapsed:.1f}s")
    
    return total_parcels, total_transactions, total_metrics

# ---------------------------------------
# Main Execution
# ---------------------------------------

if __name__ == "__main__":
    start_time = time.time()
    
    print(f"Starting parcel-centric scraper (TEST mode: {TEST})")
    print(f"Output files:")
    print(f"  - Parcels: {PARCELS_OUTPUT}")
    print(f"  - Transactions: {TRANSACTIONS_OUTPUT}")
    print(f"  - Metrics: {METRICS_OUTPUT}")
    print("="*60)
    
    print("Fetching regions and provinces...")
    region_dict, province_dict = fetch_regions()
    print(f"Fetched {len(region_dict)} regions and {len(province_dict)} provinces.")
    
    total_parcels = 0
    total_transactions = 0
    total_metrics = 0
    
    for region_id in REGION_IDS:
        try:
            p, t, m = process_region(region_id, region_dict, province_dict)
            total_parcels += p
            total_transactions += t
            total_metrics += m
        except KeyboardInterrupt:
            print("\n\n⚠️  Interrupted by user")
            break
        except Exception as e:
            print(f"\n⚠️  Error processing region {region_id}: {e}")
            continue
    
    elapsed = time.time() - start_time
    print(f"\n{'='*60}")
    print(f"✅ Completed in {elapsed:.1f}s ({elapsed/60:.1f} minutes)")
    print(f"📊 Total parcels: {total_parcels}")
    print(f"📊 Total transactions: {total_transactions}")
    print(f"📊 Total metrics: {total_metrics}")
    print(f"📄 Files created: {PARCELS_OUTPUT}, {TRANSACTIONS_OUTPUT}, {METRICS_OUTPUT}")