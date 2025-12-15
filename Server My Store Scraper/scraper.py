######################################################################
import requests
from bs4 import BeautifulSoup, Tag
import csv
import os
from datetime import date
import re
import math
import concurrent.futures
from functools import partial
from tqdm import tqdm
import time
import shutil
import random
import string
from datetime import datetime
from requests.exceptions import HTTPError
from collections import defaultdict
import hashlib
######################################################################

def slug(text):
    text = re.sub(r'\s+', ' ', text)
    text = text.strip()
    return re.sub(r'[^a-zA-Z0-9]+', '-', text.lower()).strip('-')

BASE_URL = "https://servermystore.ae"

# Global cache for page fetches
PAGE_CACHE = {}
CACHE_HITS = 0
CACHE_MISSES = 0

#-----------------------------------------------------------------------------------------------
def generate_unique_id(prefix="SMS"):
    """Generate a unique SKU/ID like SMS251215X9P."""
    date_code = datetime.now().strftime("%y%m%d")
    rand_chunk = ''.join(random.choices(string.ascii_uppercase + string.digits, k=3))
    return f"{prefix}{date_code}{rand_chunk}"

#-----------------------------------------------------------------------------------------------
class RateLimiter:
    """Adaptive rate limiter that adjusts delay based on 429 errors."""
    def __init__(self, base_delay=1.0, max_delay=10.0):
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.current_delay = base_delay
        self.last_429_time = 0
        self.success_count = 0
        
    def wait(self):
        """Wait before making a request."""
        time.sleep(random.uniform(self.current_delay * 0.8, self.current_delay * 1.2))
    
    def report_success(self):
        """Report successful request - gradually reduce delay."""
        self.success_count += 1
        if self.success_count >= 10 and self.current_delay > self.base_delay:
            self.current_delay = max(self.base_delay, self.current_delay * 0.9)
            self.success_count = 0
    
    def report_429(self, retry_after=None):
        """Report 429 error - increase delay."""
        self.last_429_time = time.time()
        if retry_after:
            self.current_delay = min(self.max_delay, retry_after + 1)
        else:
            self.current_delay = min(self.max_delay, self.current_delay * 2)
        self.success_count = 0

# Global rate limiter
rate_limiter = RateLimiter()

#-----------------------------------------------------------------------------------------------
def get_cache_key(url):
    """Generate a cache key for a URL."""
    return hashlib.md5(url.encode()).hexdigest()

#-----------------------------------------------------------------------------------------------
def fetch_page(url, session, retries=4, base_delay=5, use_cache=True):
    """
    Fetches a single page with caching and adaptive rate limiting.
    """
    global CACHE_HITS, CACHE_MISSES, PAGE_CACHE
    
    cache_key = get_cache_key(url)
    
    # Check cache first
    if use_cache and cache_key in PAGE_CACHE:
        CACHE_HITS += 1
        return PAGE_CACHE[cache_key]
    
    CACHE_MISSES += 1
    
    # Use adaptive rate limiter
    rate_limiter.wait()
    
    for i in range(retries):
        try:
            response = session.get(url, timeout=15)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Cache the result
            if use_cache:
                PAGE_CACHE[cache_key] = soup
            
            rate_limiter.report_success()
            return soup

        except (requests.exceptions.ConnectionError, requests.exceptions.ReadTimeout) as e:
            delay = base_delay * (i + 1)
            print(f"\n[WARN] Connection error for {url}: {e}. Retrying {i+1}/{retries} in {delay}s...")
            time.sleep(delay)

        except HTTPError as e:
            if e.response.status_code == 429:
                retry_after = e.response.headers.get("Retry-After")
                delay = 0
                
                if retry_after and retry_after.isdigit():
                    delay = int(retry_after) + random.uniform(0, 1)
                else:
                    delay = base_delay * (2 ** i) + random.uniform(0, 1)
                
                delay = min(delay, 300)
                rate_limiter.report_429(delay)
                
                print(f"\n[WARN] 429 Too Many Requests for {url}. Retrying {i+1}/{retries} in {int(delay)}s...")
                time.sleep(delay)
            else:
                print(f"\n[ERROR] Non-retriable HTTP error for {url}: {e}")
                return None
                
        except Exception as e:
            print(f"\n[ERROR] Non-retriable fetch failed for {url}: {e} (Type: {type(e)})")
            return None

    print(f"\n[ERROR] Fetch failed for {url} after {retries} retries.")
    return None

#-----------------------------------------------------------------------------------------------
def get_categories(session):
    """Fetches categories and subcategories from the menu structure."""
    soup = fetch_page(BASE_URL, session, use_cache=False)
    if not soup:
        return []

    categories = []
    
    # Find all top-level menu items with subcategories
    menu_items = soup.find_all('li', class_='menu-item-has-children')
    
    for item in menu_items:
        # Get the main category
        main_link = item.find('a', class_='woodmart-nav-link', recursive=False)
        if not main_link:
            continue
            
        cat_name = main_link.find('span', class_='nav-link-text')
        if cat_name:
            cat_name = cat_name.text.strip()
        else:
            cat_name = main_link.text.strip()
        
        cat_url = main_link.get('href', '')
        if cat_url and not cat_url.startswith('http'):
            cat_url = BASE_URL + cat_url
        
        # Find subcategories
        subcategories = []
        sub_menu = item.find('ul', class_='wd-sub-menu')
        
        if sub_menu:
            # First level subcategories
            for sub_item in sub_menu.find_all('li', recursive=False):
                sub_link = sub_item.find('a', class_='woodmart-nav-link', recursive=False)
                if not sub_link:
                    continue
                
                sub_name = sub_link.text.strip()
                sub_url = sub_link.get('href', '')
                if sub_url and not sub_url.startswith('http'):
                    sub_url = BASE_URL + sub_url
                
                # Check for nested subcategories
                nested_menu = sub_item.find('ul', class_='sub-sub-menu')
                if nested_menu:
                    # Has nested subcategories
                    for nested_item in nested_menu.find_all('li', recursive=False):
                        nested_link = nested_item.find('a', class_='woodmart-nav-link')
                        if nested_link:
                            nested_name = nested_link.text.strip()
                            nested_url = nested_link.get('href', '')
                            if nested_url and not nested_url.startswith('http'):
                                nested_url = BASE_URL + nested_url
                            
                            # Category path includes parent subcategory
                            category_path = f"{cat_name} > {sub_name} > {nested_name}"
                            subcategories.append({
                                'name': nested_name,
                                'url': nested_url,
                                'path': category_path
                            })
                else:
                    # No nested subcategories, this is a leaf category
                    category_path = f"{cat_name} > {sub_name}"
                    subcategories.append({
                        'name': sub_name,
                        'url': sub_url,
                        'path': category_path
                    })
        
        if subcategories:
            categories.append({
                'name': cat_name,
                'url': cat_url,
                'subcategories': subcategories
            })

    return categories

#-----------------------------------------------------------------------------------------------
def get_products_from_category(category_url, session):
    """
    Fetch all products from a category page, handling pagination.
    Returns list of product URLs.
    """
    all_products = []
    page = 1
    
    while True:
        # Build paginated URL
        if page == 1:
            page_url = category_url
        else:
            separator = '&' if '?' in category_url else '?'
            page_url = f"{category_url}{separator}paged={page}"
        
        soup = fetch_page(page_url, session)
        if not soup:
            break
        
        # Find all product cards
        products = soup.find_all('div', class_='wd-product')
        if not products:
            break
        
        for product in products:
            # Find the product link
            link = product.find('a', class_='product-image-link')
            if link and link.get('href'):
                prod_url = link['href']
                if not prod_url.startswith('http'):
                    prod_url = BASE_URL + prod_url
                all_products.append(prod_url)
        
        # Check if there's a next page
        # Look for pagination or "Load More" indicators
        pagination = soup.find('nav', class_='woocommerce-pagination')
        if pagination:
            next_link = pagination.find('a', class_='next')
            if not next_link:
                break
        else:
            # No pagination found, assume single page
            break
        
        page += 1
        
        # Safety limit
        if page > 100:
            print(f"[WARN] Reached page limit for {category_url}")
            break
    
    return list(set(all_products))  # Remove duplicates

#-----------------------------------------------------------------------------------------------
def download_image(img_url, img_path, session):
    """Helper function to download a single image using the session."""
    try:
        time.sleep(random.uniform(0.3, 0.8))
        with session.get(img_url, stream=True, timeout=15) as r:
            r.raise_for_status()
            with open(img_path, 'wb') as f:
                shutil.copyfileobj(r.raw, f)
        return img_path
    except Exception as e:
        return None

#-----------------------------------------------------------------------------------------------
def scrape_product_details(url, session, today_folder, category_path):
    """Scrapes a single product's details from servermystore.ae."""
    soup = fetch_page(url, session, use_cache=False)
    if not soup:
        return None

    data = {}
    
    # Generate unique ID
    unique_code = generate_unique_id(prefix="SMS")
    data['SKU'] = unique_code
    data['ID'] = unique_code
    data['Type'] = 'simple'
    data['Published'] = '1'
    data['Is featured?'] = '0'
    data['Visibility in catalog'] = 'visible'
    data['Categories'] = category_path
    
    # Extract product name
    title_elem = soup.find('h1', class_='product_title')
    if title_elem:
        data['Name'] = title_elem.text.strip()
    else:
        data['Name'] = url.split('/')[-2].replace('-', ' ').title()
    
    # Extract prices
    price_section = soup.find('p', class_='price')
    if price_section:
        # Check for sale price
        sale_price = price_section.find('ins')
        regular_price = price_section.find('del')
        
        if sale_price and regular_price:
            data['price'] = regular_price.text.strip()
            data['special price'] = sale_price.text.strip()
        elif sale_price:
            data['special price'] = sale_price.text.strip()
            # Calculate regular price (reverse calculation)
            try:
                sp_match = re.search(r'[\d,]+', data['special price'])
                if sp_match:
                    sp_value = float(sp_match.group().replace(',', ''))
                    regular_value = sp_value * 1.10
                    currency = re.search(r'[A-Za-z]+', data['special price'])
                    if currency:
                        data['price'] = f"{currency.group()} {regular_value:,.0f}"
                    else:
                        data['price'] = ''
            except:
                data['price'] = ''
        else:
            # No sale, just regular price
            data['price'] = price_section.text.strip()
            data['special price'] = ''
    else:
        data['price'] = ''
        data['special price'] = ''
    
    # Extract images
    img_gallery = soup.find('figure', class_='woocommerce-product-gallery__wrapper')
    images_to_download = []
    seen = set()
    
    if img_gallery:
        img_elements = img_gallery.find_all('img')
        for img in img_elements:
            img_src = None
            
            # Try different image attributes
            if img.get('data-src'):
                img_src = img['data-src']
            elif img.get('src'):
                img_src = img['src']
            
            if img_src:
                # Remove query parameters
                img_src = img_src.split('?')[0]
                if img_src.startswith('//'):
                    img_src = 'https:' + img_src
                elif not img_src.startswith('http'):
                    img_src = BASE_URL + img_src
                
                if img_src not in seen and '-150x' not in img_src and 'thumbnail' not in img_src:
                    seen.add(img_src)
                    images_to_download.append(img_src)
    
    # Download images
    downloaded_paths = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as img_executor:
        futures = []
        for img_url in images_to_download:
            img_name = os.path.basename(img_url.split('?')[0])
            img_path = os.path.join(today_folder, img_name)
            futures.append(img_executor.submit(download_image, img_url, img_path, session))
        
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result:
                downloaded_paths.append(result)
    
    data['Images'] = ','.join(images_to_download) if images_to_download else ''
    data['Meta: _wp_page_template'] = 'default'
    
    # Extract short description
    short_desc = soup.find('div', class_='woocommerce-product-details__short-description')
    product_desc_text = ''
    
    if short_desc:
        # Get all text content from short description
        for elem in short_desc.find_all(['p', 'ul', 'li', 'h2', 'h3']):
            text = elem.get_text(strip=True)
            if text:
                product_desc_text += text + ' '
    
    data['product-description'] = product_desc_text.strip()
    
    # Extract features from the description tab
    features = ''
    desc_tab = soup.find('div', id='tab-description')
    
    if desc_tab:
        # Look for list items in the description
        list_items = desc_tab.find_all('li')
        features_list = []
        
        for li in list_items:
            text = li.get_text(strip=True)
            # Clean up the text
            text = re.sub(r'\s+', ' ', text)
            if text and len(text) > 3:
                # Check if it has key:value format
                if ':' in text:
                    features_list.append(text)
                else:
                    # Add as a feature point
                    features_list.append(text)
        
        if features_list:
            features = '<br/>'.join([f"✅ {line}" for line in features_list])
    
    data['features'] = features
    
    # Build specification from features
    specification = '<ul>\n'
    if data['features']:
        # Extract brand from product name
        brand_match = re.search(r'^(\w+)', data['Name'])
        brand = brand_match.group(1) if brand_match else 'Unknown'
        specification += f'<li>Brand : {brand}</li>\n'
        
        for line in data['features'].split('<br/>'):
            clean_line = line.replace('✅', '').strip()
            if ':' in clean_line and clean_line:
                k, v = clean_line.split(':', 1)
                specification += f'<li>{k.strip()} : {v.strip()}</li>\n'
    specification += '</ul>'
    
    data['specification'] = specification
    
    # Extract categories and tags for attributes
    data['Availability'] = '1'
    
    # Try to extract brand, processor, RAM, etc. from categories or tags
    categories_section = soup.find('span', class_='posted_in')
    tags_section = soup.find('span', class_='tagged_as')
    
    # Initialize attribute fields
    data['Brand'] = ''
    data['Generation(s)'] = ''
    data['Graphics size'] = ''
    data['Operating system'] = ''
    data['Output Wattage'] = ''
    data['Processor'] = ''
    data['Ram size'] = ''
    data['Size'] = ''
    
    # Try to extract from product name or description
    name_lower = data['Name'].lower()
    desc_lower = product_desc_text.lower()
    
    # Extract brand
    for brand in ['HP', 'Dell', 'IBM', 'Lenovo', 'Apple']:
        if brand.lower() in name_lower:
            data['Brand'] = brand
            break
    
    # Extract RAM
    ram_match = re.search(r'(\d+)\s*GB\s*(DDR\d*\s*)?RAM', data['Name'], re.IGNORECASE)
    if ram_match:
        data['Ram size'] = f"{ram_match.group(1)}GB"
    
    # Extract processor
    proc_patterns = [
        r'(Intel\s*®?\s*Core\s*i\d+)', r'(Intel\s*®?\s*Xeon\s*®?\s*E-Series)',
        r'(Xeon\s*E\d+)', r'(Core\s*i\d+)', r'(AMD\s*Ryzen\s*\d+)'
    ]
    for pattern in proc_patterns:
        proc_match = re.search(pattern, data['Name'], re.IGNORECASE)
        if proc_match:
            data['Processor'] = proc_match.group(1)
            break
    
    # Extract generation
    gen_match = re.search(r'(\d+)th\s*Gen', data['Name'], re.IGNORECASE)
    if gen_match:
        data['Generation(s)'] = f"{gen_match.group(1)}th Gen"
    
    # Extract OS
    if 'windows 10' in name_lower or 'windows 10' in desc_lower:
        data['Operating system'] = 'Windows 10'
    elif 'windows 11' in name_lower or 'windows 11' in desc_lower:
        data['Operating system'] = 'Windows 11'
    
    # Initialize 6 attributes with extracted data
    attributes = {}
    attr_names = {
        1: 'Brand',
        2: 'Processor',
        3: 'RAM',
        4: 'Graphics',
        5: 'Generation',
        6: 'Operating System'
    }
    
    for i in range(1, 7):
        attributes[f'Attribute {i} name'] = attr_names.get(i, '')
        attributes[f'Attribute {i} value(s)'] = ''
        attributes[f'Attribute {i} visible'] = '1'
        attributes[f'Attribute {i} global'] = '1'
    
    # Map extracted data to attributes
    attributes['Attribute 1 value(s)'] = data['Brand']
    attributes['Attribute 2 value(s)'] = data['Processor']
    attributes['Attribute 3 value(s)'] = data['Ram size']
    attributes['Attribute 4 value(s)'] = data['Graphics size']
    attributes['Attribute 5 value(s)'] = data['Generation(s)']
    attributes['Attribute 6 value(s)'] = data['Operating system']
    
    data.update(attributes)
    
    return data

#-----------------------------------------------------------------------------------------------
def main():
    today = date.today().strftime("%Y-%m-%d")
    today_folder = today
    os.makedirs(today_folder, exist_ok=True)

    csv_filename = f"{today}_servermystore_products.csv"

    # Create an empty file with UTF-8 BOM
    with open(csv_filename, 'w', encoding='utf-8-sig') as f:
        f.write('\ufeff')
    
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    })

    print("[INFO] Fetching categories...")
    categories = get_categories(session)
    total_cats = len(categories)
    print(f"[INFO] Found {total_cats} main categories")
    
    # Count total subcategories
    total_subcats = sum(len(cat['subcategories']) for cat in categories)
    print(f"[INFO] Total subcategories to scrape: {total_subcats}")

    # Collect all products
    all_products = []
    cat_index = 0
    
    for cat in categories:
        cat_index += 1
        subcats = cat['subcategories']
        cat_name = cat['name']
        
        print(f"\n[{cat_index}/{total_cats}] Processing category: {cat_name}")
        
        for s_index, subcat in enumerate(subcats, start=1):
            sub_name = subcat['name']
            sub_url = subcat['url']
            category_path = subcat['path']
            
            print(f"  [{s_index}/{len(subcats)}] Fetching products from: {category_path}")
            
            products = get_products_from_category(sub_url, session)
            print(f"  └─ Found {len(products)} products")
            
            for prod_url in products:
                all_products.append({
                    'url': prod_url,
                    'category': category_path
                })
    
    print(f"\n[INFO] Total unique products to scrape: {len(all_products)}")
    print(f"[INFO] Cache stats: {CACHE_HITS} hits, {CACHE_MISSES} misses")
    
    # Define CSV fieldnames
    base_fieldnames = [
        'ID', 'Type', 'SKU', 'Name', 'Published', 'Is featured?', 
        'Visibility in catalog', 'Categories', 'Images', 
        'Meta: _wp_page_template', 'product-description', 
        'features', 'specification', 'price', 'special price',
        'Availability', 'Brand', 'Generation(s)', 'Graphics size',
        'Operating system', 'Output Wattage', 'Processor', 'Ram size', 'Size'
    ]
    
    # Add 6 attribute columns (24 fields total)
    attribute_fieldnames = []
    for i in range(1, 7):
        attribute_fieldnames.extend([
            f'Attribute {i} name',
            f'Attribute {i} value(s)',
            f'Attribute {i} visible',
            f'Attribute {i} global'
        ])
    
    fieldnames = base_fieldnames + attribute_fieldnames
    
    # Scrape all product details
    with open(csv_filename, 'w', newline='', encoding='utf-8-sig') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        print("\n[INFO] Scraping product details...")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            future_to_prod = {
                executor.submit(
                    scrape_product_details, 
                    prod['url'], 
                    session, 
                    today_folder,
                    prod['category']
                ): prod
                for prod in all_products
            }
            
            for future in tqdm(concurrent.futures.as_completed(future_to_prod), total=len(all_products)):
                try:
                    prod = future_to_prod[future]
                    details = future.result()
                    
                    if details:
                        writer.writerow(details)
                        csvfile.flush()
                except Exception as e:
                    prod = future_to_prod[future]
                    print(f"\n[ERROR] Failed to process {prod['url']}: {e}")

    print(f"\n[INFO] Done! CSV: {csv_filename} | Images: {today_folder}")
    print(f"[INFO] Final cache stats: {CACHE_HITS} hits, {CACHE_MISSES} misses")

#-----------------------------------------------------------------------------------------------
if __name__ == "__main__":
    main()