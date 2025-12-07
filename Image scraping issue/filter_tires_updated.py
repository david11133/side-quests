from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.options import Options
import logging, json, os, sys, time, random
from csv import writer
from scrapy.selector import Selector
from selenium import webdriver
import pandas as pd
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import ElementClickInterceptedException
from selenium.webdriver.support.ui import Select,WebDriverWait
from multiprocessing import Pool
from datetime import date



def split_to_batches(full_list, size):
    """Split List in to batches of List
    Eg: [1,2,3,4] = [[1,2],[3,4]]
    """

    batches = []

    for i in range(0, len(full_list), size):
        batch = full_list[i : i + size]
        batches.append(batch)

    return batches


def extract_num(text):
    int_text = ""
    for char in text:
        if char.isalnum():
            int_text += char
    return int(int_text)


def click(element, driver):
    """Use javascript click if selenium click method fails"""
    try:
        element.click()
    except ElementClickInterceptedException:
        driver.execute_script("arguments[0].click();", element)
    time.sleep(0.2)


CURRENT_DATE = str(date.today())

TIRE_VALUES = f"tire_size_values_{CURRENT_DATE}.csv"
TIRE_URLS = f"tire_urls_{CURRENT_DATE}.csv"
IMAGE_FOLDER = f"tires_image_{CURRENT_DATE}"
FINAL_CSV = f"data_{CURRENT_DATE}.csv"
FINAL_CSV_2 = f"data_{CURRENT_DATE}_changed_price.csv"
HEADLESS = True
POOL_SIZE = 3

arguments = sys.argv

for arg in arguments:
    if arg.startswith("image_folder"):
        IMAGE_FOLDER = arg.replace("image_folder=", "").strip()
    elif arg.startswith("tire_urls_csv"):
        TIRE_URLS = arg.replace("tire_url_csv=", "").strip()
    elif arg.startswith("final_csv"):
        FINAL_CSV = arg.replace("final_csv=", "").strip()
    elif arg.startswith("final_csv_2"):
        FINAL_CSV_2 = arg.replace("final_csv_2=", "").strip()


def create_browser(headless=HEADLESS):
    user_agent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36"
    # headless mode setting
    options = Options()
    options.add_experimental_option("excludeSwitches", ["enable-logging"])

    if headless:
        options.add_argument("--headless")
    options.add_argument(f"user-agent={user_agent}")
    # options.add_argument("window-size=1920,1080")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--log-level=3")
    options.add_argument("--excludeSwitches=enable-logging,devtools")

    driver = webdriver.Chrome(options=options)

    # # Specify the path to chromedriver.exe
    # driver = webdriver.Chrome(executable_path="D:\\Apps\\chromedriver-win64\\chromedriver.exe", options=options)
    # return driver


def get_scraped():
    try:
        if os.path.exists(TIRE_VALUES):
            tire_values_df = pd.read_csv(TIRE_VALUES, dtype=str)
            tire_values_df.drop_duplicates(inplace=True)

            scraped_widths = list(tire_values_df.width.unique())
            scraped_widths = scraped_widths[
                :-1
            ]  # do not add last to ensure all sub values(height and rimsizes) are parsed

            scraped_heights = tire_values_df[
                tire_values_df.height == scraped_widths[-1]
            ].height.unique()
            scraped_heights = scraped_heights[
                :-1
            ]  # do not add last to ensure all sub values(rim sizes) are parsed

            print(f">>> Scraped Widths = {scraped_widths}")
        else:
            scraped_widths = []
            scraped_heights = []

    except:
        scraped_widths = []
        scraped_heights = []

    return scraped_widths, scraped_heights


def handle_popup(driver):
    try:
        time.sleep(3)
        popup = driver.find_element(
            by=By.XPATH, value='//a[@id="close_popup_fullyfitted"]'
        )
        click(driver, popup)
        print(f"popup handled")
    except Exception as e:
        print(e)


def parse_filters():
    # try:
    driver = create_browser(headless=HEADLESS)
    # driver.get("https://www.pitstoparabia.com/en/results/f/155-65-14")
    driver.get("https://www.pitstoparabia.com/en/results/f/185-65-15")
    # handle_popup(driver)

    scraped_widths, scraped_heights = get_scraped()

    refine_result_bar = driver.find_element(
        by=By.XPATH, value='//a[@class="toggle_btn "]'
    )  # value="//div[@id='refine_search_container']"

    click(refine_result_bar, driver)
    time.sleep(3)

    update_tyre_size = driver.find_element(
        by=By.XPATH, value='//span[@class="static_content"]'
    )  # value="//div[@id='refine_search_container']"

    click(update_tyre_size, driver)
    time.sleep(3)
    

    width_selection = Select(
        driver.find_element(by=By.XPATH, value='//select[@id="filter-1"]')
    )

    # for option in width_selection.options:
    #     option_text = option.get_attribute('innerText').strip()  # Get the text of the option
    #     option_value = option.get_attribute('value')  # Get the value of the option
    #     print(f"Option Text: {option_text}, Option Value: {option_value}")

    for w, width in enumerate(width_selection.options):
        tire_values = []
        width_value = width_selection.options[w].get_attribute('innerText').strip()
        if width_value.lower() == "select":
            continue
        if str(width_value) in scraped_widths:
            print(f"Skipping scraped width: {width_value}")
            continue

        width_selection.select_by_index(w)
        time.sleep(2)

        height_selection = Select(
            driver.find_element(by=By.XPATH, value='//select[@id="filter-2"]')
        )
        for h, height in enumerate(height_selection.options):
            height_value = height_selection.options[h].text
            if height_value.lower() == "select":
                continue
            if str(height_value) in scraped_heights:
                print(f"Skipping scraped height: {height_value} in {width_value}")
                continue

            height_selection.select_by_index(h)
            time.sleep(2)

            rim_selection = Select(
                driver.find_element(by=By.XPATH, value='//select[@id="filter-3"]')
            )

            for r, rim in enumerate(rim_selection.options):
                rim_value = rim_selection.options[r].text
                if rim_value.lower() == "select":
                    continue

                print("> Extracting Size: ", width_value, height_value, rim_value)

                tire_values.append(
                    {"width": width_value, "height": height_value, "rim": rim_value}
                )

                rim_selection.select_by_index(r)
                rim_selection = Select(
                    driver.find_element(by=By.XPATH, value='//select[@id="filter-3"]')
                )

            height_selection = Select(
                driver.find_element(by=By.XPATH, value='//select[@id="filter-2"]')
            )
        width_df = pd.DataFrame(tire_values)
        width_df.to_csv(
            TIRE_VALUES, mode="a", index=None, header=not os.path.exists(TIRE_VALUES)
        )
        width_selection = Select(
            driver.find_element(by=By.XPATH, value='//select[@id="filter-1"]')
        )

    driver.quit()


def load_all_tires(driver):
    try:
        WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable(
                (
                    By.XPATH,
                    '//div[@id="layer-product-list"]//li[contains(@class, "product")]',
                )
            )
        )
    except:
        return None

    tires_loaded = driver.find_elements(
        by=By.XPATH,
        value='//div[@id="layer-product-list"]//li[contains(@class, "product")]',
    )
    num_tires = driver.find_element(
        by=By.XPATH, value="//span[@id='number_count']"
    ).text
    load_count = 0

    while len(tires_loaded) < extract_num(num_tires):
        if load_count == 0:
            print(f"    ** page scrolling **\n")

        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        load_count += 1
        if load_count > 20:
            break

        tires_loaded = driver.find_elements(
            by=By.XPATH,
            value='//div[@id="layer-product-list"]//li[contains(@class, "product")]',
        )
    print()
    if load_count:
        print("> All Tires loaded")


def parse_listing_result(driver, tire_values, listing_url, index):
    page_response = Selector(text=driver.page_source)

    tires = page_response.xpath(
        '//div[@id="layer-product-list"]//li[contains(@class, "product")]'
    )
    found_tires = []

    for tire in tires:
        url = tire.xpath(".//a[@class='product-item-link']/@href").get()
        if not url.startswith("http"):
            url = url.strip("/")
            url = f"https://www.pitstoparabia.com/{url}"

        found_tires.append(tire_values + [url, listing_url])

    df = pd.DataFrame(
        found_tires, columns=["width", "height", "rim", "url", "listing_url"]
    )
    df.to_csv(TIRE_URLS, mode="a", index=None, header=not os.path.exists(TIRE_URLS))

    print(
        f">>>[{index}] {len(df)} Tires for {', '.join(tire_values)} extracted successfully"
    )


def crawl_tires_listing():
    tire_values_df = pd.read_csv(TIRE_VALUES, dtype=str)
    tire_values_df.drop_duplicates(inplace=True)
    tire_values_df.fillna("None", inplace=True)

    try:
        scraped_listing_df = pd.read_csv(TIRE_URLS)
        scraped_listings = scraped_listing_df.listing_url.unique()
    except Exception:
        scraped_listings = []

    tire_values = tire_values_df.values

    all_listing_urls = []

    for tire_value in tire_values:
        tire_value = list(tire_value)
        listing_url = (
            f"https://www.pitstoparabia.com/en/results/f/{'-'.join(tire_value)}"
        )

        if listing_url in scraped_listings:
            print(f"> Skipping scraped listing in {listing_url} ")
            continue

        all_listing_urls.append({"url": listing_url, "tire_size": tire_value})

    if not len(all_listing_urls) < POOL_SIZE:
        batch_size = len(all_listing_urls) // POOL_SIZE
    else:
        batch_size = POOL_SIZE

    batches = split_to_batches(all_listing_urls, batch_size)

    with Pool(POOL_SIZE) as p:
        p.map(batch_crawl_listing, batches)
        p.terminate()
        p.join()


def batch_crawl_listing(listing_records):
    driver = create_browser(headless=HEADLESS)

    for i, listing in enumerate(listing_records):
        try:
            index = f"{i}/{len(listing_records)}"
            listing_url = listing.get("url")
            tire_value = listing.get("tire_size")
            driver.get(listing_url)
            time.sleep(5)
            load_all_tires(driver)
            parse_listing_result(driver, tire_value, listing_url, index)
        except Exception as e:
            print(f"Error: {e} while crwaling listing: {listing_url}")
    driver.quit()


def get_tires_url(driver_version=None):
    print(">>> PITSTOPARABIA SCRAPE STARTING")
    parse_filters()
    crawl_tires_listing()


if __name__ == "__main__":
    get_tires_url()
