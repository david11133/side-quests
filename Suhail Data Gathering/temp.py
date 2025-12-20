import requests
import csv
import sys

METRICS_URL = "https://api2.suhail.ai/api/mapMetrics/landMetrics/list"
TRANSACTIONS_URL = "https://api2.suhail.ai/transactions/neighbourhood"

REGION_IDS = range(1, 31)
METRICS_LIMIT = 600
PAGE_SIZE = 1000

OUTPUT_FILE = "neighborhood_transactions.csv"

# Add this line for testing
TEST = True  # Set to False to fetch everything

session = requests.Session()
rows = []

# Deduplication
seen_transactions = set()

for region_id in REGION_IDS:
    print(f"▶ Region {region_id}", flush=True)

    offset = 0
    neighborhoods_counter = 0

    while True:
        metrics_resp = session.get(
            METRICS_URL,
            params={"regionId": region_id, "offset": offset, "limit": METRICS_LIMIT},
            timeout=30
        )
        metrics_resp.raise_for_status()
        items = metrics_resp.json().get("data", {}).get("items", [])

        if not items:
            break

        # Add this for testing: limit to first 5 neighborhoods per batch
        if TEST:
            items = items[:5]

        for item in items:
            neighborhoods_counter += 1

            # 🔹 ONE DOT PER NEIGHBORHOOD
            if neighborhoods_counter % 1 == 0:
                sys.stdout.write(".")
                sys.stdout.flush()

            neighborhood_id = item["neighborhoodId"]
            neighborhood_name = item["neighborhoodName"]
            province_name = item["provinceName"]

            page = 0
            while True:
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
                transactions = tx_resp.json().get("data", [])

                if not transactions:
                    break

                for tx in transactions:
                    tx_number = tx.get("transactionNumber")
                    unique_key = (region_id, neighborhood_id, tx_number)

                    if unique_key in seen_transactions:
                        continue

                    seen_transactions.add(unique_key)

                    rows.append({
                        "regionId": region_id,
                        "provinceName": province_name,
                        "neighborhoodId": neighborhood_id,
                        "neighborhoodName": neighborhood_name,
                        "رقم الصفقة": tx_number,
                        "رقم المخطط": tx.get("subdivisionNo"),
                        "رقم البلوك": tx.get("blockNo") or "---",
                        "رقم القطعة": tx.get("parcelNo"),
                        "قيمة الصفقة (﷼)": tx.get("transactionPrice"),
                        "سعر المتر (﷼)": tx.get("priceOfMeter"),
                        "تاريخ الصفقة": tx.get("transactionDate"),
                    })

                page += 1
                # Add this for testing: limit to first page only
                if TEST:
                    break

        offset += METRICS_LIMIT

        # Add this for testing: limit to first region only
        if TEST:
            break

    print(" done")  # end of region

session.close()

print(f"\n✅ Finished")
print(f"📊 Unique transactions: {len(rows)}")

if rows:
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

print(f"📄 Saved to {OUTPUT_FILE}")