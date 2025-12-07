##########################################################################
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from scrapy.selector import Selector
import pandas as pd
import os
import requests # Replaces urllib for better performance
import re
import random
import time
import sys
##########################################################################

# Constants
CURRENT_DATE = time.strftime("%Y-%m-%d")
IMAGE_FOLDER = f"batteries_image_{CURRENT_DATE}"
FINAL_CSV = f"battery_data_{CURRENT_DATE}.csv"
FINAL_CSV_2 = f"battery_data_{CURRENT_DATE}_changed_price.csv"
HEADLESS = True
BASE_URL = "https://www.myzdegree.com/shop/batteries?page="
MAX_PAGES = 14 

# Establish a global session for image downloading (Reuse TCP connections)
img_session = requests.Session()
img_session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36"
})

# ------------------------------------------------------------------ #
# Methods                                                            #
# ------------------------------------------------------------------ #

def create_browser(headless=HEADLESS):
    options = webdriver.ChromeOptions()
    options.add_experimental_option("excludeSwitches", ["enable-logging"])
    options.page_load_strategy = 'eager' 
    
    prefs = {"profile.managed_default_content_settings.images": 2}
    options.add_experimental_option("prefs", prefs)

    if headless:
        options.add_argument("--headless")
        
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--log-level=3")
    
    # Update path if necessary, or use webdriver-manager
    # Using your specific path:
    driver_path = "D:\\Apps\\chromedriver-win64\\chromedriver.exe"
    
    try:
        driver = webdriver.Chrome(executable_path=driver_path, options=options)
    except Exception as e:
        # Fallback if path is wrong or using system PATH
        print(f"trying system PATH for driver.")
        driver = webdriver.Chrome(options=options)
        
    return driver

def extract_filename(url):
    if url:
        try:
            url = url.strip("/")
        except:
            return None
        last_slash_index = url[::-1].index("/")
        return url[-last_slash_index:]
    return None

def download_image(url, folder_name, filename):
    if not url: return

    try:
        if not os.path.exists(folder_name):
            os.makedirs(folder_name)

        file_path = os.path.join(folder_name, filename)

        if not os.path.exists(file_path):
            response = img_session.get(url, timeout=10)
            if response.status_code == 200:
                with open(file_path, 'wb') as f:
                    f.write(response.content)
                # Minimal print to reduce console I/O lag
                # print(f"> Img saved: {filename}") 
            else:
                print(f"> Failed Img: {filename} (Status {response.status_code})")
    except Exception as e:
        print(f"> Err Img: {filename} - {e}")

def generate_sku():
    rand_part = random.randint(100, 999)
    time_part = int(time.time() * 1000) % 1000000
    return f"Dbs_{time_part}{rand_part}"

def apply_cashback(row):
    return float(row['special_price']) if row['special_price'] else 0.0

def scrape_battery(driver, url, index):
    try:
        driver.get(url)
        try:
            WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.XPATH, "//h1[@class='fw-bold h4 m-0']"))
            )
        except:
            print(f"[{index}] Timed out waiting for page load: {url}")
            return

        # Pass page source to Scrapy Selector
        response = Selector(text=driver.page_source)

        name = response.xpath("//h1[@class='fw-bold h4 m-0']/text()").get()
        if not name:
            print(f"[{index}] Name not found")
            return

        print(f"[{index}]: Scraping {name}...")

        sku = generate_sku()
        
        description = "".join(response.xpath("//div[contains(@class, 'col-12 mb-3')]//text()").getall()).strip()
        if description:
            description = re.sub(r'\s+', ' ', description)

        short_description = name

        # Prices
        price = response.xpath("//small/s/text()").get()
        if price:
            price = re.sub(r'[^\d.]', '', price).strip()
        
        special_price = response.xpath("//h3[@class='h5 fw-bold mb-0 text-nowrap']/text()").get()
        if special_price:
            special_price = re.sub(r'[^\d.]', '', special_price).strip()

        if special_price and not price:
            try:
                price = float(special_price) * 1.2 
            except:
                price = ""

        is_in_stock = 1 if response.xpath("//small[@class='fw-bold mb-0'][contains(text(), 'In Stock')]") else 0

        image_link = response.xpath("//img[@id='show-img']/@src").get()
        image = extract_filename(image_link)
        
        # Specs
        def get_spec(label):
            val = response.xpath(f"//span[contains(text(), '{label}')]/following-sibling::span/text()").get()
            return val.strip() if val else ""

        output_dict = {
            "URL": url,
            "sku": sku,
            "Categories": "Batteries",
            "name": name.strip(),
            "description": description,
            "short_description": short_description.strip(),
            "price": price,
            "special_price": special_price,
            "qty": 500,
            "is_in_stock": is_in_stock,
            "brand": get_spec("Brand"),
            "capacity": get_spec("Capacity"),
            "reserve_capacity": get_spec("Reserve capacity"),
            "country_of_ manufacture": get_spec("Origin"),
            "length_mm": get_spec("Length (mm)"),
            "width_mm": get_spec("Width (mm)"),
            "height_mm": get_spec("Height (mm)"),
            "cca": get_spec("CCA"),
            "offer": (response.xpath("//p[contains(@class, 'offer_text')]/text()").get() or "").strip(),
            "warranty": response.xpath("//div[contains(text(), 'Warranty')]/text()").get() or "N/A",
            "image": f"https://www.myzdegree.com/pro-images/{image}" if image else "",
            "thumbnail": f"https://www.myzdegree.com/pro-images/{image}" if image else "",
            "small_image": f"https://www.myzdegree.com/pro-images/{image}" if image else "",
        }

        # Save to CSV
        df = pd.DataFrame([output_dict])
        df.fillna("", inplace=True)
        
        # Check header existence only once ideally, but sticking to safe row-by-row here
        header_mode = not os.path.exists(FINAL_CSV)
        df.to_csv(FINAL_CSV, mode="a", index=None, header=header_mode)

        df['special_price'] = df.apply(apply_cashback, axis=1)
        df.to_csv(FINAL_CSV_2, mode="a", index=None, header=not os.path.exists(FINAL_CSV_2))

        # Download Image
        if image_link:
            download_image(image_link, IMAGE_FOLDER, image)
        time.sleep(random.uniform(1, 2.5))

    except Exception as e:
        print(f"Error processing {url}: {e}")

def get_battery_urls(driver, page):
    url = f"{BASE_URL}{page}"
    try:
        driver.get(url)
        # Wait for grid to appear
        WebDriverWait(driver, 10).until(
             EC.presence_of_element_located((By.XPATH, '//div[contains(@class, "col-xl-3")]'))
        )
    except:
        print(f"Error loading catalog page {page}")
        return []

    response = Selector(text=driver.page_source)
    battery_cards = response.xpath('//div[contains(@class, "col-xl-3 col-lg-4 col-md-6 col-12 col-sm-12 px-1")]')
    
    urls = []
    for card in battery_cards:
        detail_url = card.xpath('.//a[@class="text-decoration-none text-dark"]/@href').get()
        if detail_url:
            if not detail_url.startswith("http"):
                detail_url = f"https://www.myzdegree.com{detail_url}"
            urls.append(detail_url)
    
    return urls

def scrape_all_batteries():
    driver = create_browser(headless=HEADLESS)
    
    all_urls = []
    print("Gathering URLs...")
    
    for page in range(1, MAX_PAGES + 1):
        print(f"Scanning page {page}...")
        page_urls = get_battery_urls(driver, page)
        all_urls.extend(page_urls)
        # Small pause between catalog pages
        time.sleep(1)

    ## Deduplicate
    # all_urls = list(set(all_urls))

    # Check already scraped
    if os.path.exists(FINAL_CSV):
        try:
            scraped_df = pd.read_csv(FINAL_CSV)
            if 'URL' in scraped_df.columns:
                scraped_urls = set(scraped_df['URL'].values)
                to_scrape = [url for url in all_urls if url not in scraped_urls]
            else:
                to_scrape = all_urls
        except:
            to_scrape = all_urls
    else:
        to_scrape = all_urls

    print(f"Total URLs: {len(all_urls)}")
    print(f"To Scrape: {len(to_scrape)}")

    # Main Loop
    for i, url in enumerate(to_scrape):
        
        current_count = (len(all_urls) - len(to_scrape)) + i + 1
        total_count = len(all_urls)
        index = f"{current_count}/{total_count}"
        
        scrape_battery(driver, url, index)

    driver.quit()
    print("Scraping Completed.")

if __name__ == "__main__":
    scrape_all_batteries()