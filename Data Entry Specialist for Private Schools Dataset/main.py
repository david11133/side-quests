from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from bs4 import BeautifulSoup
import csv
import time
from concurrent.futures import ThreadPoolExecutor

CHROMEDRIVER_PATH = r"D:\Apps\chromedriver-win64\chromedriver.exe"
BASE_URL = "https://parents.madares.sa"
PAGE_SIZE = 5
TOTAL_PAGES = 2
CSV_FILE = "schools_with_contacts.csv"
MAX_WORKERS = 4  # Use fewer workers for Selenium to avoid crashing

# Shared: configure driver options
def create_driver():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--log-level=3")
    options.add_experimental_option("excludeSwitches", ["enable-logging"])
    options.page_load_strategy = "eager"
    service = Service(CHROMEDRIVER_PATH)
    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(20)
    return driver

# Contact info from detail page using Selenium
def get_contact_info_with_selenium(url):
    driver = create_driver()
    contact = ""
    try:
        driver.get(url)
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.TAG_NAME, "ms-school-profile-card"))
        )
        soup = BeautifulSoup(driver.page_source, "html.parser")
        contact_card = soup.find("ms-school-profile-card")
        if not contact_card:
            return ""

        contact_lines = []
        for tag in contact_card.find_all(["a", "span"]):
            text = tag.get_text(strip=True)
            href = tag.get("href")
            if href and href.startswith("http"):
                contact_lines.append(href)
            elif text and not text.startswith("التواصل مع المدرسة"):
                contact_lines.append(text)

        contact = " | ".join(contact_lines).strip()
    except Exception as e:
        print(f"[ERROR] Failed to get contact for {url}: {e}")
    finally:
        driver.quit()
    return contact

# Extract schools list from listing pages
def scrape_schools_from_page(driver, page_number):
    if page_number == 1:
        url = f"{BASE_URL}/school-search?pageSize={PAGE_SIZE}"
    else:
        url = f"{BASE_URL}/school-search?page={page_number}&pageSize={PAGE_SIZE}"

    print(f"[INFO] Scraping page {page_number}: {url}")
    driver.get(url)

    try:
        WebDriverWait(driver, 15).until(
            EC.presence_of_all_elements_located((By.TAG_NAME, "ms-school-tile"))
        )
    except Exception:
        print(f"[WARNING] Timeout or no school tiles found on page {page_number}. Skipping...")
        return []

    soup = BeautifulSoup(driver.page_source, "html.parser")
    tiles = soup.find_all("ms-school-tile", class_="block")
    print(f"[INFO] Found {len(tiles)} schools on page {page_number}")

    schools = []
    for tile in tiles:
        name_tag = tile.find("h2")
        location_tag = tile.find("span", class_="school-location")
        link_tag = tile.find("a", href=True)

        name = name_tag.get_text(strip=True) if name_tag else ""
        city = location_tag.get_text(strip=True) if location_tag else ""
        href = link_tag["href"] if link_tag else ""
        full_school_url = BASE_URL + href if href else ""

        schools.append({
            "school_name": name,
            "city": city,
            "contact_info": "",  # To be filled
            "detail_url": full_school_url
        })

    return schools

# Parallel contact info scraping using Selenium in threads
def scrape_contact_infos_parallel(schools, max_workers=4):
    def fetch_contact(school):
        url = school["detail_url"]
        print(f" → fetching contact from: {url}")
        contact_info = get_contact_info_with_selenium(url)
        print(f" ← contact: {contact_info}")
        school["contact_info"] = contact_info
        del school["detail_url"]
        return school

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        return list(executor.map(fetch_contact, schools))

# Save results
def save_to_csv(data, filename):
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["school_name", "city", "contact_info"])
        writer.writeheader()
        writer.writerows(data)

# Main script
def main():
    print("[INFO] Launching Chrome for listing pages...")
    driver = create_driver()
    all_schools = []

    try:
        for page in range(1, TOTAL_PAGES + 1):
            schools = scrape_schools_from_page(driver, page)
            if not schools:
                print(f"[INFO] No schools found on page {page}, stopping early.")
                break
            all_schools.extend(schools)
    finally:
        driver.quit()

    print(f"[INFO] Scraped {len(all_schools)} schools from listing pages.")
    print("[INFO] Fetching contact info in parallel (with Selenium)...")
    all_schools = scrape_contact_infos_parallel(all_schools, max_workers=MAX_WORKERS)

    save_to_csv(all_schools, CSV_FILE)
    print(f"[INFO] Saved all data to {CSV_FILE}")

if __name__ == "__main__":
    main()
