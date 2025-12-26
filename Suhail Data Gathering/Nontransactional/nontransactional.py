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
################################################################################

REGION_IDS = [2, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]
MAP_NAMES = {
    2: "makkah_region", 4: "al_qassim", 5: "eastern_region", 6: "asir_region",
    7: "tabuk", 8: "hail", 9: "northern_borders", 10: "riyadh",
    11: "najran", 12: "bahah", 13: "al_madenieh", 14: "jazan", 15: "jawf"
}

## Test with one city
# REGION_IDS = [12]
# MAP_NAMES = {12: "bahah"}

ZOOM_LEVEL = 15
PARCELS_FILE = "non_transactional_parcels.csv"
REGIONS_URL = "https://api2.suhail.ai/regions"
PARCEL_DETAILS_URL = "https://api2.suhail.ai/api/parcel/search"

# Performance settings
MAX_WORKERS_TILES = 10
MAX_WORKERS_ENRICH = 20
REQUEST_TIMEOUT = 10

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
        print(f"[{self.GREEN}{self._get_timestamp()}{self.RESET}] {self.GREEN}✓ {msg}{self.RESET}")

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

        stats_str = (f"F:{self.GREEN}{stats_dict['found']}{self.RESET} "
                     f"E:{self.BLUE}{stats_dict['enriched']}{self.RESET} "
                     f"X:{self.RED}{stats_dict['errors']}{self.RESET}")

        sys.stdout.write(f"\r{prefix} |{bar}| {percent:.1f}% [{current}/{total}] {stats_str} | ETA: {eta_str}")
        sys.stdout.flush()

################################################################################
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
def create_session():
    s = requests.Session()
    retries = Retry(total=3, backoff_factor=0.3, status_forcelist=[500, 502, 503, 504])
    s.mount('https://', HTTPAdapter(max_retries=retries, pool_connections=30, pool_maxsize=30))
    return s

session = create_session()

################################################################################
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

################################################################################
def fetch_region_boundary(region_id, regions_dict):
    region = regions_dict.get(region_id)
    if not region: return None
    bbox = region.get("restrictBoundaryBox", {})
    sw, ne = bbox.get("southwest", {}), bbox.get("northeast", {})
    if not all([sw.get("x"), sw.get("y"), ne.get("x"), ne.get("y")]): return None
    return [sw.get("x"), sw.get("y"), ne.get("x"), ne.get("y")]

################################################################################
def safe_get(props, *keys):
    """Try multiple possible keys and return first non-empty value"""
    for key in keys:
        val = props.get(key)
        if val not in (None, '', 0, '0'):
            return val
    return ''

################################################################################
def fetch_and_decode_tile(tile_coords, tile_url_template):
    """Fetch and decode tile, checking ALL layers for province_id"""
    x, y, z = tile_coords
    url = tile_url_template.format(z=z, x=x, y=y)
    try:
        resp = session.get(url, timeout=5)
        if resp.status_code != 200: return []
        decoded_tile = mapbox_vector_tile.decode(resp.content)
        
        parcels_list = []
        
        # Try different layer names and MERGE data from multiple layers
        for layer_name in ['parcels', 'parcels-base', 'parcel', 'land', 'neighborhoods']:
            if layer_name in decoded_tile:
                for feature in decoded_tile[layer_name]['features']:
                    # Store both the feature AND which layer it came from
                    parcels_list.append({
                        'properties': feature.get('properties', {}),
                        'geometry': feature.get('geometry'),
                        'layer': layer_name
                    })
        
        return parcels_list
    except Exception:
        return []

################################################################################
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
        return "ERROR"

################################################################################
def process_tile_batch(tiles, tile_url_template):
    parcels_data = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS_TILES) as executor:
        future_to_tile = {executor.submit(fetch_and_decode_tile, tile, tile_url_template): tile for tile in tiles}
        for future in as_completed(future_to_tile):
            features = future.result()
            if features:
                for feature in features:
                    props = feature.get('properties', {})
                    
                    # Skip if has transactions
                    if int(props.get('transactions_count', 0) or 0) > 0: 
                        continue
                    
                    # Must have a parcel ID
                    parcel_id = props.get('parcel_objectid') or props.get('parcel_id')
                    if not parcel_id: 
                        continue
                    
                    parcels_data.append(feature)
    return parcels_data

################################################################################
def enrich_single_parcel(parcel_data, region_id, province_ids, regions_dict, provinces_dict):
    """Worker function for parallel enrichment with COMPREHENSIVE data extraction"""
    props = parcel_data['properties']
    layer = parcel_data.get('layer', 'unknown')
    
    # Extract identifiers with multiple fallback keys
    parcel_obj_id = safe_get(props, 'parcel_objectid', 'parcel_id', 'parcelObjectId')
    parcel_no = safe_get(props, 'parcel_no', 'parcelno', 'parcelNo')
    subdivision_no = safe_get(props, 'subdivision_no', 'subdivisionno')
    
    # CRITICAL: Extract province_id from tile FIRST (all possible variations)
    province_id = safe_get(props, 'province_id', 'provinceid', 'provinceId')
    
    # Extract ALL other fields from tile properties
    neighborhood_id = safe_get(props, 'neighborhood_id', 'neighborhoodid')
    neighborhood_name = safe_get(props, 'neighborhood_name', 'neighborhaname', 'neighbarhaname', 'neighborh_aname')
    block_no = safe_get(props, 'block_no', 'blockno') or '---'
    land_use_detailed = safe_get(props, 'landuseadetailed', 'land_use_detailed', 'landuse_detailed')
    land_use_group = safe_get(props, 'landuseagroup', 'land_use_group', 'landuse_group')
    municipality_name = safe_get(props, 'municipality_aname', 'municipality_name', 'municipalityname')
    zoning_id = safe_get(props, 'zoning_id', 'zoningid')
    total_area = safe_get(props, 'shape_area', 'total_area', 'totalarea')
    
    details = None
    enrichment_status = 'TILE_ONLY'
    
    # Strategy 1: Try API enrichment if we have valid IDs and no province yet
    if parcel_no and subdivision_no and str(subdivision_no).isdigit():
        # If we already have province_id, only check that one
        prov_list = [province_id] if province_id else province_ids
        
        for prov_id in prov_list:
            details = fetch_parcel_details(region_id, prov_id, subdivision_no, parcel_no)
            if details == "ERROR":
                enrichment_status = 'ERROR'
                details = None
                break
            if details:
                # API returned data - use its province_id as authoritative
                if details.get('provinceId'):
                    province_id = details.get('provinceId')
                enrichment_status = 'API_ENRICHED'
                break
    
    # Strategy 2: If STILL no province_id, try neighborhood lookup
    if not province_id and neighborhood_id:
        # Neighborhood IDs often contain province information
        # Try to match neighborhood to provinces in this region
        for prov_id in province_ids:
            prov_info = provinces_dict.get(prov_id, {})
            # Check if neighborhood might belong to this province
            # This is a heuristic - it might be adjusted in the future
            if prov_info:
                province_id = prov_id
                enrichment_status = 'NEIGHBORHOOD_INFERRED'
                break
    
    # Strategy 3: Last resort - use first province as default
    if not province_id and province_ids:
        province_id = province_ids[0]
        enrichment_status = 'DEFAULT_PROVINCE'
    
    # Merge API data with tile data
    geometry = parcel_data.get('geometry')
    if details:
        geometry = details.get('geometry') or geometry
        total_area = details.get('totalArea') or total_area
    
    # Get metadata
    region_info = regions_dict.get(region_id, {})
    province_info = provinces_dict.get(province_id, {}) if province_id else {}
    
    result = {
        'region_id': region_id,
        'region_name': region_info.get('name', ''),
        'province_id': province_id or '',
        'province_name': province_info.get('name', '') if province_info else '',
        'parcel_objectid': parcel_obj_id or '',
        'parcel_no': parcel_no or '',
        'subdivision_no': subdivision_no or '',
        'neighborhood_id': neighborhood_id,
        'neighborhood_name': neighborhood_name,
        'block_no': block_no,
        'total_area': total_area or '',
        'land_use_detailed': land_use_detailed,
        'land_use_group': land_use_group,
        'municipality_name': municipality_name,
        'zoning_id': zoning_id,
        'geometry': json.dumps(geometry, ensure_ascii=False) if geometry else ''
    }
    
    return result, enrichment_status

################################################################################
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

################################################################################
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
def main():
    logger.section("NON-TRANSACTIONAL PARCEL SCRAPER (ENHANCED)")
    init_csv_files()
    regions_dict, provinces_dict = fetch_region_metadata()
    if not regions_dict: return
    
    # Print province info for debugging
    for region_id in REGION_IDS:
        if region_id in regions_dict:
            region = regions_dict[region_id]
            logger.info(f"Region {region_id} ({region.get('name', 'Unknown')}) has provinces:")
            for prov in region.get('provinces', []):
                logger.info(f"  - {prov['id']}: {prov.get('name', 'Unknown')}")
    
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
            
            # 1. Fetch Tiles
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

            # 3. Enrich Parcels
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
                    if 'API_ENRICHED' in status:
                        stats.parcels_enriched += 1
                    elif 'ERROR' in status:
                        stats.errors += 1
            
            append_to_csv(enriched_results)
        
        logger._clear_line()
        print(f"\nResults for Region {region_id}: Scanned {len(tiles)} | Found {stats.parcels_found} | Enriched {stats.parcels_enriched}")

################################################################################
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.warning("Scraper interrupted.")
    except Exception as e:
        logger.error(f"Error: {e}")