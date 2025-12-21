import requests
import csv
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# =========================
# CONFIG
# =========================

TRANSACTIONS_URL = "https://api2.suhail.ai/transactions/neighbourhood"
METRICS_URL = "https://api2.suhail.ai/api/mapMetrics/landMetrics/list"

REGION_IDS = range(10, 16)
METRICS_LIMIT = 500
PAGE_SIZE = 500

OUTPUT_FILE = "neighborhood_transactions.csv"

MAX_RETRIES = 2
REQUEST_TIMEOUT = 15

TEST = False  # True = small run

# =========================
# SESSION
# =========================

def create_session():
    s = requests.Session()
    adapter = HTTPAdapter(
        pool_connections=10,
        pool_maxsize=10,
        max_retries=Retry(
            total=MAX_RETRIES,
            backoff_factor=0.3,
            status_forcelist=[500, 502, 503, 504]
        )
    )
    s.mount("https://", adapter)
    return s

session = create_session()

# =========================
# CSV HELPER
# =========================

def write_rows(rows):
    if not rows:
        return

    file_exists = False
    try:
        with open(OUTPUT_FILE, "r"):
            file_exists = True
    except FileNotFoundError:
        pass

    with open(OUTPUT_FILE, "a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)

# =========================
# MAIN
# =========================

start_time = time.time()
seen_transactions = set()

for region_id in REGION_IDS:
    print(f"\n▶ Region {region_id}")
    offset = 0
    region_rows = []

    while True:
        try:
            r = session.get(
                METRICS_URL,
                params={
                    "regionId": region_id,
                    "offset": offset,
                    "limit": METRICS_LIMIT
                },
                timeout=REQUEST_TIMEOUT
            )
            r.raise_for_status()
        except Exception as e:
            print(f"⚠️ Metrics error: {e}")
            break

        items = r.json().get("data", {}).get("items", [])
        if not items:
            break

        if TEST:
            items = items[:3]

        for item in items:
            neighborhood_id = item["neighborhoodId"]
            neighborhood_name = item["neighborhoodName"]
            province_name = item["provinceName"]

            page = 0
            print(f"  • {neighborhood_name[:35]}")

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
                        timeout=REQUEST_TIMEOUT
                    )
                    tx_resp.raise_for_status()
                except Exception as e:
                    print(f"⚠️ TX error: {e}")
                    break

                txs = tx_resp.json().get("data", [])
                if not txs:
                    break

                rows = []
                for tx in txs:
                    tx_no = tx.get("transactionNumber")
                    key = (region_id, neighborhood_id, tx_no)
                    if key in seen_transactions:
                        continue

                    seen_transactions.add(key)

                    rows.append({
                        "regionId": region_id,
                        "provinceName": province_name,
                        "neighborhoodId": neighborhood_id,
                        "neighborhoodName": neighborhood_name,
                        "رقم الصفقة": tx_no,
                        "رقم المخطط": tx.get("subdivisionNo"),
                        "رقم البلوك": tx.get("blockNo") or "---",
                        "رقم القطعة": tx.get("parcelNo"),
                        "قيمة الصفقة (﷼)": tx.get("transactionPrice"),
                        "سعر المتر (﷼)": tx.get("priceOfMeter"),
                        "تاريخ الصفقة": tx.get("transactionDate"),
                        "نوع الأرض": tx.get("type"),
                        "نوع الاستخدام": tx.get("metricsType"),
                        "المساحة الإجمالية": tx.get("totalArea"),
                        "centroidX": tx.get("centroidX"),
                        "centroidY": tx.get("centroidY"),
                        "sellingType": tx.get("sellingType"),
                        "propertyType": tx.get("propertyType"),
                        "landUseGroup": tx.get("landUseGroup"),
                        "source": tx.get("transactionSource")
                    })

                write_rows(rows)
                print(f"    page {page} → {len(rows)} rows")

                page += 1
                if TEST:
                    break

        offset += METRICS_LIMIT
        if TEST:
            break

elapsed = time.time() - start_time
print("\n" + "=" * 60)
print(f"✅ Done in {elapsed:.1f}s")
print(f"📊 Unique transactions: {len(seen_transactions)}")
print(f"📄 Saved to {OUTPUT_FILE}")
