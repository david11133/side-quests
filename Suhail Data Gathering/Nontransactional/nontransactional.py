################################################################################
import requests
import csv
import time
import sys
import json
import mercantile
import mapbox_vector_tile
import shutil
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from collections import defaultdict
################################################################################

REGION_IDS = [2, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]
MAP_NAMES = {
    2: "makkah_region", 4: "al_qassim", 5: "eastern_region", 6: "asir_region",
    7: "tabuk", 8: "hail", 9: "northern_borders", 10: "riyadh",
    11: "najran", 12: "bahah", 13: "al_madenieh", 14: "jazan", 15: "jawf"
}
ZOOM_LEVEL = 15
PARCELS_FILE = "non_transactional_parcels.csv"
REGIONS_URL = "https://api2.suhail.ai/regions"
PARCEL_DETAILS_URL = "https://api2.suhail.ai/api/parcel/search"

# Performance Tuning
MAX_WORKERS_TILES = 10      # Workers for fetching map tiles
MAX_WORKERS_ENRICH = 20     # Workers for hitting the API (Enrichment)
REQUEST_TIMEOUT = 10        # Seconds before giving up

################################################################################
#  PROFESSIONAL LOGGING & STATS CLASSES
################################################################################

class ConsoleLogger:
    GREEN = '\033[92m'
    BLUE = '\033[94m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    RESET = '\033[0m'
    BOLD = '\033[1m'

    def __init__(self):
        self.start_time = time.time()
        self.last_update = 0

    def _get_timestamp(self):
        return datetime.datetime.now().strftime("%H:%M:%S")

    def info(self, msg):
        self._clear_line()
        print(f"[{self.BLUE}{self._get_timestamp()}{self.RESET}] {msg}")

    def success(self, msg):
        self._clear_line()
        print(f"[{self.GREEN}{self._get_timestamp()}{self.RESET}] {self.GREEN}✔ {msg}{self.RESET}")

    def warning(self, msg):
        self._clear_line()
        print(f"[{self.YELLOW}{self._get_timestamp()}{self.RESET}] {self.YELLOW}⚠ {msg}{self.RESET}")

    def error(self, msg):
        self._clear_line()
        print(f"[{self.RED}{self._get_timestamp()}{self.RESET}] {self.RED}✖ {msg}{self.RESET}")

    def section(self, title):
        self._clear_line()
        width = shutil.get_terminal_size().columns
        print(f"\n{self.BOLD}{'='*width}{self.RESET}")
        print(f"{self.BOLD} {title.center(width)} {self.RESET}")
        print(f"{self.BOLD}{'='*width}{self.RESET}")

    def _clear_line(self):
        sys.stdout.write("\r" + " " * shutil.get_terminal_size().columns + "\r")
        sys.stdout.flush()

    def progress_bar(self, current, total, stats_dict, prefix='Progress'):
        now = time.time()
        # Update at most 5 times per second to prevent IO lag
        if current < total and (now - self.last_update < 0.2):
            return
        self.last_update = now

        percent = float(current) * 100 / total
        bar_length = 25
        filled_length = int(bar_length * current // total)
        bar = '█' * filled_length + '░' * (bar_length - filled_length)
        
        elapsed = time.time() - self.start_time
        rate = current / elapsed if elapsed > 0 else 0
        remaining = total - current
        eta = remaining / rate if rate > 0 else 0
        eta_str = str(datetime.timedelta(seconds=int(eta)))

        # Stats String
        # F = Found, E = Enriched, X = Errors/Timeouts
        stats_str = (f"F:{self.GREEN}{stats_dict['found']}{self.RESET} "
                     f"E:{self.BLUE}{stats_dict['enriched']}{self.RESET} "
                     f"X:{self.RED}{stats_dict['errors']}{self.RESET}")

        sys.stdout.write(f"\r{prefix} |{bar}| {percent:.1f}% [{current}/{total}] {stats_str} | ETA: {eta_str}")
        sys.stdout.flush()

class StatsTracker:
    def __init__(self):
        self.tiles_processed = 0
        self.tiles_with_data = 0
        self.parcels_found = 0
        self.parcels_enriched = 0
        self.errors = 0
    
    def to_dict(self):
        return {
            "found": self.parcels_found,
            "enriched": self.parcels_enriched,
            "errors": self.errors
        }

logger = ConsoleLogger()

################################################################################
#  NETWORK & LOGIC
################################################################################

def create_session():
    s = requests.Session()
    # REDUCED RETRIES: Fail fast if server is blocking
    retries = Retry(total=3, backoff_factor=0.3, status_forcelist=[500, 502, 503, 504])
    s.mount('https://', HTTPAdapter(max_retries=retries, pool_connections=30, pool_maxsize=30))
    return s

session = create_session()

def fetch_region_metadata():
    logger.info("Connecting to Suhail API for region metadata...")
    try:
        resp = session.get(REGIONS_URL, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        data = resp.json().get("data", [])
        
        regions_dict = {}
        provinces_dict = {}
        
        for region in data:
            regions_dict[region["id"]] = region
            for province in region.get("provinces", []):
                provinces_dict[province["id"]] = province
        
        logger.success(f"Loaded metadata for {len(regions_dict)} regions.")
        return regions_dict, provinces_dict
    except Exception as e:
        logger.error(f"Failed to fetch regions: {e}")
        return {}, {}

def fetch_region_boundary(region_id, regions_dict):
    region = regions_dict.get(region_id)
    if not region: return None
    bbox = region.get("restrictBoundaryBox", {})
    sw, ne = bbox.get("southwest", {}), bbox.get("northeast", {})
    if not all([sw.get("x"), sw.get("y"), ne.get("x"), ne.get("y")]): return None
    return [sw.get("x"), sw.get("y"), ne.get("x"), ne.get("y")]

def fetch_and_decode_tile(tile_coords, tile_url_template):
    x, y, z = tile_coords
    url = tile_url_template.format(z=z, x=x, y=y)
    try:
        resp = session.get(url, timeout=5) # Short timeout for tiles
        if resp.status_code != 200: return []
        decoded_tile = mapbox_vector_tile.decode(resp.content)
        for layer_name in ['parcels', 'parcels-base', 'parcel', 'land']:
            if layer_name in decoded_tile:
                return decoded_tile[layer_name]['features']
        return []
    except Exception:
        return []

def fetch_parcel_details(region_id, province_id, subdivision_no, parcel_no):
    try:
        resp = session.get(
            PARCEL_DETAILS_URL,
            params={
                "regionId": region_id, "provinceId": province_id,
                "subdivisionNo": subdivision_no, "parcelNo": parcel_no,
                "offset": 0, "limit": 1
            },
            timeout=REQUEST_TIMEOUT
        )
        if resp.status_code in (404, 410): return None
        resp.raise_for_status()
        details = resp.json().get("data", {}).get("parcelDetails", [])
        return details[0] if details else None
    except Exception:
        return "ERROR" # Distinct flag for connection errors

def process_tile_batch(tiles, tile_url_template):
    parcels_data = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS_TILES) as executor:
        future_to_tile = {executor.submit(fetch_and_decode_tile, tile, tile_url_template): tile for tile in tiles}
        for future in as_completed(future_to_tile):
            features = future.result()
            if features:
                for feature in features:
                    props = feature.get('properties', {})
                    if int(props.get('transactions_count', 0) or 0) > 0: continue
                    
                    parcel_id = props.get('parcel_objectid') or props.get('parcel_id')
                    if not parcel_id: continue
                    
                    parcels_data.append({'properties': props, 'geometry': feature.get('geometry')})
    return parcels_data

def enrich_single_parcel(parcel_data, region_id, province_ids, regions_dict, provinces_dict):
    """Worker function for parallel enrichment"""
    props = parcel_data['properties']
    
    # Extract identifiers
    parcel_obj_id = (props.get('parcel_objectid') or props.get('parcel_id') or props.get('parcelObjectId'))
    parcel_no = (props.get('parcel_no') or props.get('parcelno') or props.get('parcelNo'))
    subdivision_no = (props.get('subdivision_no') or props.get('subdivisionno'))
    
    province_id = ''
    details = None
    enrichment_status = 'SKIPPED' # SKIPPED, ENRICHED, ERROR
    
    # Optimally, only fetch if we have valid IDs
    if parcel_no and subdivision_no and str(subdivision_no).isdigit() and province_ids:
        for prov_id in province_ids:
            details = fetch_parcel_details(region_id, prov_id, subdivision_no, parcel_no)
            if details == "ERROR":
                enrichment_status = 'ERROR'
                details = None
                break # Stop trying provinces if network is down
            if details:
                province_id = prov_id
                enrichment_status = 'ENRICHED'
                break
    
    # Merge data
    geometry = parcel_data.get('geometry')
    total_area = props.get('shape_area') or props.get('total_area')
    
    if details:
        geometry = details.get('geometry')
        if not total_area: total_area = details.get('totalArea')

    region_info = regions_dict.get(region_id, {})
    province_info = provinces_dict.get(province_id, {})
    
    result = {
        'region_id': region_id,
        'region_name': region_info.get('name', ''),
        'province_id': province_id,
        'province_name': province_info.get('name', ''),
        'parcel_objectid': parcel_obj_id or '',
        'parcel_no': parcel_no or '',
        'subdivision_no': subdivision_no or '',
        'neighborhood_id': props.get('neighborhood_id') or '',
        'neighborhood_name': props.get('neighborhood_name') or '',
        'block_no': props.get('block_no') or '---',
        'total_area': total_area or '',
        'land_use_detailed': props.get('landuseadetailed', ''),
        'municipality_name': props.get('municipality_aname', ''),
        'zoning_id': props.get('zoning_id', ''),
        'geometry': json.dumps(geometry, ensure_ascii=False) if geometry else ''
    }
    
    return result, enrichment_status

def init_csv_files():
    parcel_headers = [
        'region_id', 'region_name', 'province_id', 'province_name',
        'parcel_objectid', 'parcel_no', 'subdivision_no', 'block_no',
        'neighborhood_id', 'neighborhood_name', 'total_area',
        'land_use_detailed', 'land_use_group', 'municipality_name',
        'zoning_id', 'geometry'
    ]
    with open(PARCELS_FILE, 'w', newline='', encoding='utf-8-sig') as f:
        csv.DictWriter(f, fieldnames=parcel_headers).writeheader()
    logger.info(f"Initialized output file: {PARCELS_FILE}")

def append_to_csv(data):
    if not data: return
    parcel_headers = [
        'region_id', 'region_name', 'province_id', 'province_name',
        'parcel_objectid', 'parcel_no', 'subdivision_no', 'block_no',
        'neighborhood_id', 'neighborhood_name', 'total_area',
        'land_use_detailed', 'land_use_group', 'municipality_name',
        'zoning_id', 'geometry'
    ]
    with open(PARCELS_FILE, 'a', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=parcel_headers, extrasaction='ignore')
        writer.writerows(data)

################################################################################
#  MAIN LOOP
################################################################################

def main():
    logger.section("NON-TRANSACTIONAL PARCEL SCRAPER (PARALLEL)")
    init_csv_files()
    regions_dict, provinces_dict = fetch_region_metadata()
    if not regions_dict: return
    
    total_parcels_all_regions = 0
    seen_ids = set()
    
    for region_id in REGION_IDS:
        if region_id not in MAP_NAMES: continue
        
        map_name = MAP_NAMES[region_id]
        region_name = regions_dict.get(region_id, {}).get('name', f'ID {region_id}')
        logger.section(f"REGION {region_id}: {region_name}")
        
        stats = StatsTracker()
        logger.start_time = time.time()
        
        bounds = fetch_region_boundary(region_id, regions_dict)
        if not bounds: continue
        
        tiles = list(mercantile.tiles(*bounds, ZOOM_LEVEL))
        logger.info(f"Tiles to Scan: {len(tiles)}")
        
        province_ids = [p['id'] for p in regions_dict[region_id].get('provinces', [])]
        tile_url_template = f"https://tiles.suhail.ai/maps/{map_name}/{{z}}/{{x}}/{{y}}.vector.pbf"
        
        chunk_size = 50
        
        for i in range(0, len(tiles), chunk_size):
            chunk = tiles[i:i + chunk_size]
            logger.progress_bar(i, len(tiles), stats.to_dict())
            
            # 1. Fetch Tiles (Parallel)
            raw_parcels = process_tile_batch(chunk, tile_url_template)
            stats.tiles_processed += len(chunk)
            if raw_parcels: stats.tiles_with_data += 1
            
            # 2. Filter Duplicates
            unique_parcels = []
            for p in raw_parcels:
                pid = p['properties'].get('parcel_objectid') or p['properties'].get('parcel_id')
                if pid and pid not in seen_ids:
                    seen_ids.add(pid)
                    unique_parcels.append(p)
            
            if not unique_parcels: continue
            stats.parcels_found += len(unique_parcels)

            # 3. Enrich Parcels (PARALLELIZED - The Fix)
            # This was previously the bottleneck
            enriched_results = []
            with ThreadPoolExecutor(max_workers=MAX_WORKERS_ENRICH) as executor:
                futures = {
                    executor.submit(
                        enrich_single_parcel, 
                        p, region_id, province_ids, regions_dict, provinces_dict
                    ): p for p in unique_parcels
                }
                
                for future in as_completed(futures):
                    res, status = future.result()
                    enriched_results.append(res)
                    if status == 'ENRICHED':
                        stats.parcels_enriched += 1
                    elif status == 'ERROR':
                        stats.errors += 1
            
            append_to_csv(enriched_results)
        
        logger._clear_line()
        print(f"\nResults for Region {region_id}: Scanned {len(tiles)} | Found {stats.parcels_found} | Enriched {stats.parcels_enriched}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.warning("Scraper interrupted.")
    except Exception as e:
        logger.error(f"Error: {e}")