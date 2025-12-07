from selenium import webdriver
from filter_tires_updated import *
from scrapy.selector import Selector
import pandas as pd
import os
import urllib.request, sys
import socket
import re
import random
import time

socket.setdefaulttimeout(10)


def extract_filename(url, **kwargs):
    if url:
        try:
            url = url.strip("/")
        except:
            return None
        last_slash_index = url[::-1].index("/")
        return url[-last_slash_index:]


def download_image(url, folder_name, filename):
    try:
        if not os.path.exists(folder_name):
            os.makedirs(folder_name)

        if not os.path.exists(f"./{folder_name}/{filename}"):
            opener = urllib.request.build_opener()
            opener.addheaders = [
                (
                    "User-Agent",
                    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1941.0 Safari/537.36",
                )
            ]
            urllib.request.install_opener(opener)
            urllib.request.urlretrieve(url, f"./{folder_name}/{filename}")
            print(f">>> Image: {filename} downloaded")
    except Exception as e:
        print(f">>> Image: {filename} couldn't be downloaded")
        print("exception",e)
        pass


def get_tyre_category(tyre_type):
    if tyre_type:
        if "SUV MT" in tyre_type or "SUV AT" in tyre_type or "SUsV AT" in tyre_type:
            return "Off Road Tyres"
        if "Car" in tyre_type:
            return "Car Tyres"
        if "SUV" in tyre_type:
            return "SUV Tyres"
        if "Commercial" in tyre_type:
            return "Commercial Tyres"
        else:
            return "Car Tyres"
        
def extract_cashback(cashback_text):
    match = re.search(r'(\d+)', cashback_text)  # Extracts numeric part
    return float(match.group(1)) if match else 0  # Returns 0 if no number found
        
def appyCashback(row):
    if row['cash_back'].strip().lower() == "BUY 3+1 FREE".lower() or row['cash_back'].strip().lower()  == "BUY 3 GET 1 FREE".lower():
        print(f"special price: {float(row['special_price']) - 29} after 3 + 1 offer | orginal price: {row['special_price']}")
        return float(row['special_price']) - 29
    if row['cash_back'].strip().lower() == "BUY 2+2 FREE".lower():
        new_price = float((float(row["special_price"]) * 2 - 85) / 4)
        print(f"special price: {new_price} after 2 + 2 offer | orginal price: {row['special_price']}")
        return new_price
    elif extract_cashback(row['cash_back']) == 100:
        return float(row['special_price']) - 48
    elif extract_cashback(row['cash_back']) == 150:
        return float(row['special_price']) - 40
    elif extract_cashback(row['cash_back']) == 200:
        return float(row['special_price']) - 53
    elif extract_cashback(row['cash_back']) == 250:
        return float(row['special_price']) - 88
    elif extract_cashback(row['cash_back']) == 300:
        return float(row['special_price']) - 80
    elif extract_cashback(row['cash_back']) == 400:
        return float(row['special_price']) - 127
    elif extract_cashback(row['cash_back']) == 450:
        return float(row['special_price']) - 142
    elif extract_cashback(row['cash_back']) == 500:
        return float(row['special_price']) - 156
    elif extract_cashback(row['cash_back']) == 600:
        return float(row['special_price']) - 182
    else:
        return float(row['special_price']) - 22


def scrape_tire(driver, filtered_values, index):
    filtered_width = filtered_values[0]
    filtered_height = filtered_values[1]
    filtered_rimsize = filtered_values[2]
    url = filtered_values[3]

    filtered_rimsize = f"R{filtered_rimsize}"

    driver.get(url)
    time.sleep(2)

    response = Selector(text=(driver.page_source).encode("ascii", "ignore"))

    url_rimsize = filtered_rimsize
    width = filtered_width
    height = filtered_height

    name = response.xpath("//div[@class='brand']/following-sibling::h1/text()").get()
    part_no = response.xpath("//span[@class='part_no']/text()").get()

    if not name:
        return

    if part_no:
        name = (name or '').strip() + ' ' + part_no.strip()

    print(f"[{index}]: Scraping {name}... ")
    sku = response.xpath("//span[@class='sku']/text()").get()
    description = "".join(
        response.xpath("//div[@class='pro_size_detail'][1]//text()").getall()
    )
    if description:
        description = description.replace("\n", " ").strip()
    # check
    extra_saving_column = "".join(
        response.xpath("//div[@class='extra_discount clearfix']//text()").getall()
    ).strip()
    tyre_type = response.xpath("//div[@class='variants']/div/@title").get()
    year_of_manufacture = "".join(
        response.xpath("//div[@title='Year of manufacture']/text()").getall()
    ).strip()

    sidewall = "".join(
        response.xpath(
            "//li/span[contains(text(),'Sidewall')]/parent::li/text()"
        ).getall()
    ).strip()
    country_of_manufacture = "".join(
        response.xpath("//div[@class='menufacture_country']/text()").getall()
    ).strip()
    manufacturer = response.xpath("//div[@class='brand']/a/@title").get()
    service_desc = response.xpath("//div[@class='serv_desc']/text()[2]").get()
    if service_desc:
        service_desc = service_desc.replace("\n", "").strip()
        tyre_load = service_desc[:-1].strip()
        tyre_speed = service_desc[-1].strip()
    else:
        tyre_load = ""
        tyre_speed = ""

    short_description = name
    if response.xpath("//img[@title='Run Flat']"):
        tyre_run_flat = 1
    else:
        tyre_run_flat = 0

    image_link = response.xpath(
        "//div[@class='product_thumbnail_container']//img[@id='zoom_01']/@src"
    ).get()
    image = extract_filename(image_link)
    thumbnail = image
    small_image = image

    # check
    # cash_back = " ".join(
    #     response.xpath('(//div[contains(@class, "topbar")]//p)[1]//text()').getall()
    # )
    # cash_back = " ".join(
    #     response.xpath('//p[@class="prom_text"]/parent::div//text()').getall()
    # )
    #cash_back = " ".join(
    #    response.xpath('//div[@class="offer_block"]//p[@class="offer_cnt"]//span//b//text()').getall()
    #)

    cash_back = " ".join(
        response.xpath('//div[@class="offer_block"]//p[@class="offer_cnt"]//span//text()').getall()
    )

    promo_url = response.xpath('//div[@class="offer_image"]//img/@src').get()
    if promo_url and "buy_3_1" in str(promo_url.lower()):
        buy_3_get_1_free = "yes"
    else:
        buy_3_get_1_free = "no"

    if "Buy 3 + 1 FREE".lower() in str(cash_back).lower() or "Buy 3+1 FREE".lower() in str(cash_back).lower() or "BUY 3 GET 1 FREE".lower() in str(cash_back).lower():
        buy_3_get_1_free = "yes"
    else:
        buy_3_get_1_free = "no"

    type_is_clearance = "".join(
        response.xpath("//div[@class='discount']//text()").getall()
    )
    if type_is_clearance:
        type_is_clearance = type_is_clearance.replace("\n", " ").strip()
    rating_stars = response.xpath(
        "//div[@class='rating-stars']/following-sibling::h3/text()"
    ).get()

    warranty = " ".join(
        response.xpath(
            "//div[@class='warranty']/span[@class='w_year']//text()"
        ).getall()
    )
    special_price = response.xpath(
        "//span[contains(@id, 'product-price')]//text()"
    ).get()
    if special_price:
        special_price = (
            special_price.replace("\n", "").replace("AED", "").replace(",", "").strip()
        )
    price = response.xpath("//span[contains(@id, 'old-price')]//text()").get()
    if price:
        price = price.replace("\n", "").replace("AED", "").replace(",", "").strip()

    if special_price and not price:
        try:
            price = float(special_price) * 1.2
        except:
            price = ""

    output_dict = {
        "URL": url,
        "sku": generate_sku(),
        "name": name.strip() if name else name,
        "attribute_set": "Default",
        "type": "Simple",
        "categories": get_tyre_category(tyre_type),
        "description": description,
        "short_description": short_description.strip() if short_description else short_description,
        "price": price,
        "qty": 500,
        "is_in_stock": 1,
        "manage_stock": 1,
        "use_config_manage_stock": 1,
        "status": 1,
        "visibility": 4,
        "weight": 1,
        "tax_class_id": "Taxable Goods",
        # "image_link": image_link,
        "image": f"https://www.dubaityreshop.com/pro-images/{image}",
        "thumbnail": f"https://www.dubaityreshop.com/pro-images/{thumbnail}",
        "small_image": f"https://www.dubaityreshop.com/pro-images/{small_image}",
        "manufacturer": manufacturer,
        "country_of_manufacture": country_of_manufacture,
        "sidewall": sidewall,
        "special_price": special_price,
        "tyre_width": width.replace("X","").strip() if width else width,
        "tyre_height": height if height else 1,
        "tyre_rim_size": url_rimsize,
        "tyre_load": tyre_load,
        "tyre_speed": tyre_speed,
        "tyre_type": tyre_type if tyre_type else "Car",
        "year_manufacture": year_of_manufacture,
        # "type_is_clearance": type_is_clearance.replace("Off", "") if "Off" in type_is_clearance else type_is_clearance,
        "type_is_clearance": " ",
        "buy_3_get_1": buy_3_get_1_free,
        "tyre_run_flat": tyre_run_flat,
        "extra_saving_column": extra_saving_column,
        "cash_back": cash_back,
        "warranty": "1  Year" if warranty.strip().lower() == "Lifetime".lower() else warranty,
    }

    df = pd.DataFrame([output_dict])

    # df['tyre_type'] = df["tyre_type"].apply(lambda x: x if x is not None or x != '' or x !=' ' else 'Car')
    # df['categories'] = df["categories"].apply(lambda x: x if x is not None or x != '' or x !=' ' else 'Car Tyres')
    # df['tyre_height'] = df["tyre_height"].apply(lambda x: x if x is not None or x != '' or x !=' ' else 1)
    df['tyre_type'] = df["tyre_type"].apply(lambda x: x if x is not None and x != '' and x != ' ' else 'Car')
    df['categories'] = df["categories"].apply(lambda x: x if x is not None and x != '' and x != ' ' else 'Car Tyres')
    df['tyre_height'] = df["tyre_height"].apply(lambda x: x if x is not None and x != '' and x != ' ' else 1)
    df['tyre_height'] = pd.to_numeric(df['tyre_height'], errors='coerce')

    df['description'] = df.apply(lambda row: re.sub(r'SKU: \w+', f"SKU: {row['sku']}", row['description']), axis=1)

    # Fill missing or invalid tyre_height with 1
    df['tyre_height'] = df['tyre_height'].fillna(1)
    # Calculate threshold (50% of the columns)
    threshold = df.shape[1] * 0.5

    # Drop rows where more than 50% of the columns are null
    df = df.dropna(thresh=threshold)
    df.to_csv(FINAL_CSV, mode="a", index=None, header=not os.path.exists(FINAL_CSV))

    df['special_price'] = df.apply(appyCashback, axis=1)
    print("=================================second file===============================")
    df.to_csv(FINAL_CSV_2, mode="a", index=None, header=not os.path.exists(FINAL_CSV_2))
    print("=================================second file===============================")

    try:
        print("======================started============================")
        download_image(image_link, IMAGE_FOLDER, image)
        print("======================ended============================")
    except Exception as e:
        print(f"Failed to download image: {e}")


def extract_data(tire_values):
    driver = create_browser(headless=HEADLESS)

    for i, tire in enumerate(tire_values):
        try:
            index = f"{i}/{len(tire_values)}"
            scrape_tire(driver, tire, index)
        except Exception as e:
            print(f"ERROR: {e} in {tire}")

    driver.quit()


def scrape_all_pitstop_tires():
    all_tire_urls_df = pd.read_csv(TIRE_URLS)
    all_tire_urls_df.drop_duplicates(inplace=True)

    queries = all_tire_urls_df.values

    try:
        scraped_df = pd.read_csv(FINAL_CSV, encoding="unicode_escape")
        scraped_url = scraped_df.URL.values
    except:
        scraped_url = []

    print(f"<<< Number of Scraped Tires: {len(scraped_url)} >>>")
    print(f"<<< Number to Scrape: {len(all_tire_urls_df) - len(scraped_url)} >>>")

    to_scrape_values = []
    for i, query in enumerate(queries):
        url = query[3]
        if url in scraped_url:
            print(f"> Skipping scraped tire: {url}")
            continue
        to_scrape_values.append(query)

    if not len(to_scrape_values) < POOL_SIZE:
        batch_size = len(to_scrape_values) // POOL_SIZE
    else:
        batch_size = POOL_SIZE

    batches = split_to_batches(to_scrape_values, batch_size)

    with Pool(POOL_SIZE) as p:
        p.map(extract_data, batches)
        p.terminate()
        p.join()

def generate_sku():
    rand_part = random.randint(100, 999)
    time_part = int(time.time() * 1000) % 1000000     
    return f"Dts_{time_part}{rand_part}"


if __name__ == "__main__":

    for trial in range(30):
        try:
            get_tires_url()   
        except Exception as e:
            print(f"Error {e} while getting tires")
            continue

        print("\n\n     Extraction of Tires URL complete      ")
        print("---------------------------------------------")
        print("Beginning Extraction of Tires Data from URL\n")

        scrape_all_pitstop_tires()
        break
