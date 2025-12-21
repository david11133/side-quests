import requests
import csv

URL = "https://api2.suhail.ai/regions"
CSV_FILE = "regions_provinces.csv"

response = requests.get(URL)
response.raise_for_status()

regions = response.json()["data"]

with open(CSV_FILE, mode="w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)

    # CSV Header
    writer.writerow([
        "region_id",
        "region_name",
        "region_centroid_x",
        "region_centroid_y",
        "boundary_sw_x",
        "boundary_sw_y",
        "boundary_ne_x",
        "boundary_ne_y",
        "region_image",
        "province_id",
        "province_name",
        "province_centroid_x",
        "province_centroid_y"
    ])

    for region in regions:
        region_centroid = region.get("centroid", {})
        boundary = region.get("restrictBoundaryBox", {})
        southwest = boundary.get("southwest", {})
        northeast = boundary.get("northeast", {})

        for province in region.get("provinces", []):
            province_centroid = province.get("centroid", {})

            writer.writerow([
                region.get("id"),
                region.get("name"),
                region_centroid.get("x"),
                region_centroid.get("y"),
                southwest.get("x"),
                southwest.get("y"),
                northeast.get("x"),
                northeast.get("y"),
                region.get("image"),
                province.get("id"),
                province.get("name"),
                province_centroid.get("x"),
                province_centroid.get("y"),
            ])

print("CSV file created: regions_provinces.csv")
