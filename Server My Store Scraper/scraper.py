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

# UPDATED: Valid product categories based on your store
VALID_CATEGORIES = {
    'servers', 'workstations', 'networking', 'desktops', 'cisco products',
    'server hard drives', 'processors', 'graphic cards', 'ip phones',
    'server parts & accessories', 'accessories', 'printers', 'storage',
    "led's monitor", 'lighting', 'apple products', 'sfp modules',
    'all in one pc', 'routers', 'cisco switches', 'avaya ip phones',
    'cisco ip phones', 'adapters', 'memory', 'motherboard', 'power supply',
    'sas hard drive', 'sas ssd', 'sata hard drive', 'dell', 'hp', 'ibm',
    'lenovo', 'used servers', 'used workstations', 'dell servers',
    'hp servers', 'dell workstation', 'hp workstations', 'ibm workstations'
}

# UPDATED: Brand extraction mapping
BRANDS = ['HP', 'Dell', 'IBM', 'Lenovo', 'Apple', 'Cisco', 'Avaya', 'Intel', 'AMD']

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
def is_valid_category(category_name):
    """Check if a category name is a valid product category."""
    cat_lower = category_name.lower().strip()
    
    # Exclude language/country selectors
    excluded = ['language', 'country', 'currency', 'english', 'arabic', 'united arab emirates', 'aed']
    if any(exc in cat_lower for exc in excluded):
        return False
    
    # Check if it's in our valid categories
    return cat_lower in VALID_CATEGORIES

#-----------------------------------------------------------------------------------------------
def get_categories(session):
    """Fetches categories and subcategories from the menu structure."""
    soup = fetch_page(BASE_URL, session, use_cache=False)
    if not soup:
        return []

    categories = []
    
    # Find all top-level menu items with subcategories (ORIGINAL WORKING CODE)
    menu_items = soup.find_all('li', class_='menu-item-has-children')
    
    print(f"[DEBUG] Found {len(menu_items)} menu items with children")
    
    # Exclude only Language/Country at top level
    excluded_top_level = ['language', 'country', 'currency']
    
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
        
        print(f"[DEBUG] Processing main category: '{cat_name}'")
        
        # Skip Language/Country/Currency at top level
        if any(exc in cat_name.lower() for exc in excluded_top_level):
            print(f"[INFO] Skipping non-product category: {cat_name}")
            continue
        
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
                
                # FIXED: Skip only language/country selectors, not valid categories
                if any(exc in sub_name.lower() for exc in excluded_top_level):
                    continue
                
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
                            
                            # FIXED: Skip only language/country selectors
                            if any(exc in nested_name.lower() for exc in excluded_top_level):
                                continue
                            
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
        pagination = soup.find('nav', class_='woocommerce-pagination')
        if pagination:
            next_link = pagination.find('a', class_='next')
            if not next_link:
                break
        else:
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
def extract_brand(product_name, category_path):
    """Extract brand from product name or category."""
    name_lower = product_name.lower()
    cat_lower = category_path.lower()
    
    # UPDATED: Check against expanded brand list
    for brand in BRANDS:
        if brand.lower() in name_lower or brand.lower() in cat_lower:
            return brand
    
    # Try to extract first word as brand if it looks like a brand name
    first_word = product_name.split()[0] if product_name.split() else ''
    if first_word and len(first_word) > 2 and first_word[0].isupper():
        return first_word
    
    return ''

#-----------------------------------------------------------------------------------------------
def extract_ram(text):
    """Extract RAM size from text."""
    # Pattern: 8GB, 16 GB, 32GB RAM, etc.
    ram_match = re.search(r'(\d+)\s*GB\s*(DDR\d*\s*)?(RAM)?', text, re.IGNORECASE)
    if ram_match:
        return f"{ram_match.group(1)}GB"
    return ''

#-----------------------------------------------------------------------------------------------
def extract_processor(text):
    """Extract processor from text."""
    # UPDATED: More comprehensive processor patterns
    proc_patterns = [
        r'(Intel\s*®?\s*Core\s*i\d+[\s\-]\d+\w*)',  # Intel Core i5-12400
        r'(Intel\s*®?\s*Xeon\s*®?\s*[EW][\-\s]\d+\w*)',  # Xeon E-2236, Xeon W-2245
        r'(Intel\s*®?\s*Core\s*i\d+)',  # Intel Core i5
        r'(Xeon\s*[EW][\-\s]?\d+\w*)',  # Xeon E5-2690
        r'(AMD\s*Ryzen\s*\d+\s*\d+\w*)',  # AMD Ryzen 5 5600X
        r'(AMD\s*EPYC\s*\d+\w*)',  # AMD EPYC 7543
        r'(Core\s*i\d+)',  # Core i7
    ]
    for pattern in proc_patterns:
        proc_match = re.search(pattern, text, re.IGNORECASE)
        if proc_match:
            return proc_match.group(1)
    return ''

#-----------------------------------------------------------------------------------------------
def extract_generation(text):
    """Extract generation from text."""
    gen_match = re.search(r'(\d+)(?:th|st|nd|rd)?\s*Gen(?:eration)?', text, re.IGNORECASE)
    if gen_match:
        return f"{gen_match.group(1)}th Gen"
    return ''

#-----------------------------------------------------------------------------------------------
def extract_os(text):
    """Extract operating system from text."""
    text_lower = text.lower()
    if 'windows 11' in text_lower:
        return 'Windows 11'
    elif 'windows 10' in text_lower:
        return 'Windows 10'
    elif 'windows server' in text_lower:
        # Extract version if present
        ws_match = re.search(r'Windows\s*Server\s*(\d+)', text, re.IGNORECASE)
        if ws_match:
            return f"Windows Server {ws_match.group(1)}"
        return 'Windows Server'
    elif 'linux' in text_lower:
        return 'Linux'
    elif 'macos' in text_lower or 'mac os' in text_lower:
        return 'macOS'
    return ''

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
                features_list.append(text)
        
        if features_list:
            features = '<br/>'.join([f"✅ {line}" for line in features_list])
    
    data['features'] = features
    
    # Build specification from features
    specification = '<ul>\n'
    
    # UPDATED: Extract brand using improved function
    brand = extract_brand(data['Name'], category_path)
    if brand:
        specification += f'<li>Brand : {brand}</li>\n'
    
    if data['features']:
        for line in data['features'].split('<br/>'):
            clean_line = line.replace('✅', '').strip()
            if ':' in clean_line and clean_line:
                k, v = clean_line.split(':', 1)
                specification += f'<li>{k.strip()} : {v.strip()}</li>\n'
    specification += '</ul>'
    
    data['specification'] = specification
    
    # UPDATED: Extract attributes using improved functions
    combined_text = f"{data['Name']} {product_desc_text} {category_path}"
    
    data['Brand'] = brand
    data['Processor'] = extract_processor(combined_text)
    data['Ram size'] = extract_ram(combined_text)
    data['Generation(s)'] = extract_generation(combined_text)
    data['Operating system'] = extract_os(combined_text)
    
    # Initialize remaining fields
    data['Graphics size'] = ''
    data['Output Wattage'] = ''
    data['Size'] = ''
    data['Availability'] = '1'
    
    # Extract graphics card if mentioned
    gpu_patterns = [
        r'(NVIDIA\s*(?:GeForce\s*)?(?:RTX|GTX)\s*\d+\w*)',
        r'(AMD\s*Radeon\s*(?:RX|Pro)?\s*\d+\w*)',
        r'(Intel\s*(?:UHD|Iris)\s*Graphics\s*\d*)',
    ]
    for pattern in gpu_patterns:
        gpu_match = re.search(pattern, combined_text, re.IGNORECASE)
        if gpu_match:
            data['Graphics size'] = gpu_match.group(1)
            break
    
    # Extract power supply wattage for relevant categories
    if 'power supply' in category_path.lower():
        watt_match = re.search(r'(\d+)\s*W(?:att)?', combined_text, re.IGNORECASE)
        if watt_match:
            data['Output Wattage'] = f"{watt_match.group(1)}W"
    
    # UPDATED: Initialize 6 attributes with extracted data
    attributes = {}
    attr_config = [
        ('Brand', data['Brand']),
        ('Processor', data['Processor']),
        ('RAM', data['Ram size']),
        ('Graphics', data['Graphics size']),
        ('Generation', data['Generation(s)']),
        ('Operating System', data['Operating system'])
    ]
    
    for i, (attr_name, attr_value) in enumerate(attr_config, 1):
        attributes[f'Attribute {i} name'] = attr_name
        attributes[f'Attribute {i} value(s)'] = attr_value
        attributes[f'Attribute {i} visible'] = '1'
        attributes[f'Attribute {i} global'] = '1'
    
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

    # Collect all unique products with their first category
    products_dict = {}  # url -> category_path mapping
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
                # Only add product if not already seen (keeps first category)
                if prod_url not in products_dict:
                    products_dict[prod_url] = category_path
    
    # Convert to list
    all_products = [{'url': url, 'category': cat} for url, cat in products_dict.items()]
    
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