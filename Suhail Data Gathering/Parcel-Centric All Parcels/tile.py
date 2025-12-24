import requests
import mapbox_vector_tile
import csv
import math
import os
from concurrent.futures import ThreadPoolExecutor

# ---------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------
# Choose your city here: 'SHAQRA' or 'RIYADH'
TARGET_CITY = 'SHAQRA' 

CITY_BBOXES = {
    # [min_lon, min_lat, max_lon, max_lat]
    'SHAQRA': [45.150, 25.100, 45.350, 25.350], # Covers the whole city of Shaqra
    'RIYADH': [46.500, 24.400, 46.900, 25.000], # Covers most of Riyadh
}

TILE_URL_TEMPLATE = "https://tiles.suhail.ai/maps/riyadh/15/{x}/{y}.vector.pbf"
OUTPUT_FILE = "master_parcel_inventory.csv"
ZOOM_LEVEL = 15
MAX_WORKERS = 20 # Increase for speed

# ---------------------------------------------------------
# TILE MATH
# ---------------------------------------------------------
def deg2num(lat_deg, lon_deg, zoom):
    lat_rad = math.radians(lat_deg)
    n = 2.0 ** zoom
    xtile = int((lon_deg + 180.0) / 360.0 * n)
    ytile = int((1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * n)
    return (xtile, ytile)

def get_tiles_in_bbox(bbox, zoom):
    min_lon, min_lat, max_lon, max_lat = bbox
    x1, y1 = deg2num(max_lat, min_lon, zoom)
    x2, y2 = deg2num(min_lat, max_lon, zoom)
    
    # Normalize coordinates
    min_x, max_x = min(x1, x2), max(x1, x2)
    min_y, max_y = min(y1, y2), max(y1, y2)
    
    tiles = []
    for x in range(min_x, max_x + 1):
        for y in range(min_y, max_y + 1):
            tiles.append((x, y))
    return tiles

# ---------------------------------------------------------
# PROCESSING
# ---------------------------------------------------------
def fetch_and_parse_tile(tile_coords):
    x, y = tile_coords
    url = TILE_URL_TEMPLATE.format(x=x, y=y)
    
    try:
        resp = requests.get(url, timeout=5)
        if resp.status_code != 200:
            return []
        
        decoded_data = mapbox_vector_tile.decode(resp.content)
        parcels = []
        
        # Check all possible layer names
        target_layers = ['parcels', 'parcels-base', 'parcels-centroids']
        
        for layer_name in target_layers:
            if layer_name in decoded_data:
                features = decoded_data[layer_name].get('features', [])
                for feature in features:
                    props = feature.get('properties', {})
                    p_id = props.get('parcel_objectid')
                    
                    if p_id:
                        record = {
                            'parcelObjectId': p_id, # Normalized Key for main.py
                            'parcelNo': props.get('parcel_no'),
                            'subdivisionNo': props.get('subdivision_no'),
                            'landUseGroup': props.get('landuseagroup'),
                            'area': props.get('shape_area'),
                            'neighborhoodName': props.get('neighborhaname'),
                            'municipality': props.get('municipality_aname'),
                            'source_tile': f"{x}/{y}"
                        }
                        parcels.append(record)
        return parcels

    except Exception as e:
        return []

# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------
if __name__ == "__main__":
    bbox = CITY_BBOXES[TARGET_CITY]
    tiles = get_tiles_in_bbox(bbox, ZOOM_LEVEL)
    print(f"🌍 Scraper Initialized for {TARGET_CITY}")
    print(f"🗺️  Area: {bbox}")
    print(f"📦 Total Tiles to Scan: {len(tiles)}")
    
    all_parcels = {} 
    
    # Using a higher worker count since tiles are small and fast
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = executor.map(fetch_and_parse_tile, tiles)
        
        count = 0
        for i, batch in enumerate(results):
            if i % 100 == 0:
                print(f"   Progress: Scanned {i}/{len(tiles)} tiles...")
                
            for p in batch:
                p_id = p.get('parcelObjectId')
                if p_id:
                    all_parcels[p_id] = p
                    count += 1
    
    print(f"\n✅ Scan Complete.")
    print(f"📊 Found {len(all_parcels)} unique parcels.")
    
    if all_parcels:
        keys = list(all_parcels.values())[0].keys()
        with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(all_parcels.values())
        print(f"💾 Saved inventory to {OUTPUT_FILE}")
    else:
        print("⚠️ No parcels found. Check your coordinates or internet connection.")