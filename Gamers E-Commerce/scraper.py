######################################################################
import requests
from bs4 import BeautifulSoup, Tag
import csv
import os
from datetime import date
import re
import math
import concurrent.futures
from functools import partial, lru_cache
from tqdm import tqdm
import time
import shutil
import random
import string
from datetime import datetime
from requests.exceptions import HTTPError
from collections import defaultdict
import hashlib
import threading
######################################################################

def slug(text):
    text = re.sub(r'\s+', ' ', text)
    text = text.strip()
    return re.sub(r'[^a-zA-Z0-9]+', '-', text.lower()).strip('-')

BASE_URL = "https://arabgamers.ae"

# Global cache for page fetches
PAGE_CACHE = {}
CACHE_HITS = 0
CACHE_MISSES = 0

#-----------------------------------------------------------------------------------------------
def generate_unique_id(prefix="AG"):
    """Generate a unique SKU/ID like AG251106X9P."""
    date_code = datetime.now().strftime("%y%m%d")
    rand_chunk = ''.join(random.choices(string.ascii_uppercase + string.digits, k=3))
    return f"{prefix}{date_code}{rand_chunk}"

#-----------------------------------------------------------------------------------------------
class RateLimiter:
    """Adaptive rate limiter that adjusts delay based on 429 errors."""
    def __init__(self, base_delay=2.0, max_delay=60.0):
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.current_delay = base_delay
        self.last_429_time = 0
        self.success_count = 0
        self.lock = threading.Lock()
        
    def wait(self):
        """Wait before making a request."""
        with self.lock:
            delay = self.current_delay
        time.sleep(random.uniform(delay * 0.8, delay * 1.2))
    
    def report_success(self):
        """Report successful request - gradually reduce delay."""
        with self.lock:
            self.success_count += 1
            if self.success_count >= 10 and self.current_delay > self.base_delay:
                self.current_delay = max(self.base_delay, self.current_delay * 0.9)
                self.success_count = 0
    
    def report_429(self, retry_after=None):
        """Report 429 error - increase delay."""
        with self.lock:
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
def fetch_page(url, session, retries=8, base_delay=5, use_cache=True):
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
def extract_filters_from_page(soup):
    """
    Extracts filter groups and their values from a category page.
    Returns a dict like: {'Brand': [{'value': 'Sony', 'param': 'filter.p.vendor'}, ...]}
    """
    filters = {}
    
    if not soup:
        return filters
    
    filter_groups = soup.find_all('details', class_='filter-group')
    
    for group in filter_groups:
        title_span = group.find('span')
        if not title_span:
            continue
            
        filter_name = title_span.text.strip()
        param_name = group.get('data-param-name', '')
        
        checkboxes = group.find_all('input', {'type': 'checkbox', 'class': 'filter'})
        
        filter_values = []
        for checkbox in checkboxes:
            value = checkbox.get('value', '').strip()
            if value:
                filter_values.append({
                    'value': value,
                    'param': param_name
                })
        
        if filter_values:
            filters[filter_name] = filter_values
    
    return filters

#-----------------------------------------------------------------------------------------------
def get_categories(session):
    """Fetches categories using the provided session."""
    soup = fetch_page(BASE_URL, session, use_cache=False)
    if not soup:
        return []

    categories = []
    menu_list = soup.find('ul', class_='menu-list')
    if not menu_list:
        print("[WARNING] No categories found")
        return []

    for item in menu_list.find_all('li', class_='menu-item', recursive=False):
        link = item.find('a')
        if not link:
            continue
        
        cat_name = None
        if link.get('href'):
            href = link['href']
            match = re.search(r'/collections/([^/?]+)', href)
            if match:
                cat_name = match.group(1).replace('-', ' ').title()
        
        if not cat_name:
            full_text = link.get_text(separator=' ', strip=True)
            cat_name = re.sub(r'\s+', ' ', full_text).strip()
        
        if not cat_name or cat_name == '':
            cat_name = link.get('href', 'unknown').split('/')[-1].replace('-', ' ').title()

        cat_url = BASE_URL + link['href'] if not link['href'].startswith('http') else link['href']
        subcategories = []

        sub_menu = item.find('div', class_='dropdown-menu')
        if sub_menu:
            for sub_item in sub_menu.find_all('li', class_='menu-item'):
                sub_link = sub_item.find('a')
                if sub_link:
                    sub_name = None
                    if sub_link.get('href'):
                        href = sub_link['href']
                        match = re.search(r'/collections/([^/?]+)', href)
                        if match:
                            sub_name = match.group(1).replace('-', ' ').title()
                    
                    if not sub_name:
                        sub_text = sub_link.get_text(separator=' ', strip=True)
                        sub_name = re.sub(r'\s+', ' ', sub_text).strip()
                    
                    if not sub_name or sub_name == '':
                        sub_name = sub_link.get('href', 'unknown').split('/')[-1].replace('-', ' ').title()
                    
                    sub_url = BASE_URL + sub_link['href'] if not sub_link['href'].startswith('http') else sub_link['href']
                    subcategories.append({'name': sub_name, 'url': sub_url})

        categories.append({'name': cat_name, 'url': cat_url, 'subcategories': subcategories})

    return categories

#-----------------------------------------------------------------------------------------------
def _parse_product_cards(soup):
    """Helper function to parse product cards from a soup object."""
    products = []
    if not soup:
        return products
        
    cards = soup.find_all('section', class_='product-card')
    for card in cards:
        title = card.find('h3', class_='product-card_title')
        if title and title.find('a'):
            link = title.find('a')
            prod_name = link.text.strip()
            prod_url = BASE_URL + link['href'] if not link['href'].startswith('http') else link['href']
            products.append({'name': prod_name, 'url': prod_url})
    return products

#-----------------------------------------------------------------------------------------------
def _parse_total_pages(soup):
    """Helper function to find the Total number of pages for each category."""
    if not soup:
        return 1
        
    pagination_text = soup.get_text()
    total_match = re.search(r'Showing \d+ - \d+ of (\d+) items', pagination_text)
    if total_match:
        total_items = int(total_match.group(1))
        return math.ceil(total_items / 20)
    return 1

#-----------------------------------------------------------------------------------------------
def get_all_products_from_category(base_url, session):
    """
    Fetch ALL products from a category (no filters) to get complete product list.
    This is faster than checking each filter individually.
    """
    print(f"      → Fetching all products from category...")
    
    all_products = []
    
    # Fetch first page
    first_soup = fetch_page(base_url, session)
    if not first_soup:
        return []
    
    # Parse page 1
    page_1_products = _parse_product_cards(first_soup)
    all_products.extend(page_1_products)
    
    num_pages = _parse_total_pages(first_soup)
    
    # Fetch remaining pages in parallel
    if num_pages > 1:
        page_urls = [f"{base_url}?page={p}" for p in range(2, num_pages + 1)]
        
        fetch_with_session = partial(fetch_page, session=session)
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            soups = list(executor.map(fetch_with_session, page_urls))
            
            for soup in soups:
                if soup:
                    all_products.extend(_parse_product_cards(soup))
    
    # Return only unique product URLs
    unique_products = {p['url']: p for p in all_products}
    return list(unique_products.values())

#-----------------------------------------------------------------------------------------------
def check_product_has_filter(product_url, filter_param, filter_value, session):
    """
    Quick check: fetch the filtered page and see if this product appears.
    Returns True if product matches this filter.
    """
    # We already have the product URL, so we can check if it appears in filtered results
    # But this is still expensive. Better approach: fetch filter page once and check all products
    pass

#-----------------------------------------------------------------------------------------------
def get_products_with_filter_batch(base_url, filter_items, session):
    """
    Optimized: Fetch products for multiple filter values in parallel batches.
    Returns: dict mapping filter_value -> [product_urls]
    """
    results = {}
    
    def fetch_filter_products(filter_item):
        filter_value = filter_item['value']
        filter_param = filter_item['param']
        filter_url = f"{base_url}?{filter_param}={filter_value}"
        
        all_products = []
        first_soup = fetch_page(filter_url, session)
        if not first_soup:
            return filter_value, []
        
        page_1_products = _parse_product_cards(first_soup)
        all_products.extend(page_1_products)
        
        num_pages = _parse_total_pages(first_soup)
        
        if num_pages > 1:
            page_urls = [f"{filter_url}&page={p}" for p in range(2, num_pages + 1)]
            fetch_with_session = partial(fetch_page, session=session)
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
                soups = list(executor.map(fetch_with_session, page_urls))
                for soup in soups:
                    if soup:
                        all_products.extend(_parse_product_cards(soup))
        
        unique_urls = list(set([p['url'] for p in all_products]))
        return filter_value, unique_urls
    
    # Process filters in parallel with controlled concurrency
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(fetch_filter_products, item) for item in filter_items]
        
        for future in concurrent.futures.as_completed(futures):
            try:
                filter_value, product_urls = future.result()
                results[filter_value] = product_urls
            except Exception as e:
                print(f"\n[ERROR] Filter batch processing failed: {e}")
    
    return results

#-----------------------------------------------------------------------------------------------
def normalize_product_url(url):
    """Normalize product URLs for consistent matching."""
    # Remove query parameters and trailing slashes
    url = url.split('?')[0].rstrip('/')
    # Remove BASE_URL if present to get relative path
    if url.startswith(BASE_URL):
        url = url[len(BASE_URL):]
    return url

#-----------------------------------------------------------------------------------------------
def build_filter_mapping_for_category(category_url, session):
    """
    Optimized approach with proper URL matching:
    1. Fetch ALL products from category first (no filters)
    2. For each filter type, fetch filtered pages in parallel batches
    3. Build reverse mapping: product -> filters
    """
    print(f"    ├─ Extracting filters...")
    category_soup = fetch_page(category_url, session)
    if not category_soup:
        return {}, set()
    
    filters = extract_filters_from_page(category_soup)
    
    if not filters:
        print(f"    └─ No filters found")
        return {}, set()
    
    print(f"    ├─ Found {len(filters)} filter types: {', '.join(filters.keys())}")
    
    # Get all products from category (baseline)
    all_products = get_all_products_from_category(category_url, session)
    print(f"    ├─ Found {len(all_products)} total products")
    
    # Build mapping with NORMALIZED URLs
    product_filter_map = {}
    url_to_original = {}  # Map normalized URL back to original
    
    for p in all_products:
        normalized = normalize_product_url(p['url'])
        product_filter_map[normalized] = {}
        url_to_original[normalized] = p['url']
    
    # Process each filter type
    total_mapped = 0
    for filter_name, filter_items in filters.items():
        print(f"    ├─ Processing filter: {filter_name} ({len(filter_items)} values)")
        
        # Fetch all filter values in parallel batches
        filter_results = get_products_with_filter_batch(category_url, filter_items, session)
        
        # Map products to their filter values with NORMALIZED URLs
        filter_matches = 0
        for filter_value, product_urls in filter_results.items():
            for url in product_urls:
                normalized = normalize_product_url(url)
                if normalized in product_filter_map:
                    product_filter_map[normalized][filter_name] = filter_value
                    filter_matches += 1
        
        print(f"    │  └─ Matched {filter_matches} product-filter pairs")
    
    # Count products with at least one filter
    products_with_filters = sum(1 for filters in product_filter_map.values() if filters)
    
    print(f"    └─ Mapped {len(product_filter_map)} products to filters")
    print(f"    └─ Products with filters: {products_with_filters}/{len(product_filter_map)}")
    print(f"    └─ Cache stats: {CACHE_HITS} hits, {CACHE_MISSES} misses (hit rate: {CACHE_HITS/(CACHE_HITS+CACHE_MISSES)*100:.1f}%)")
    
    # Return with ORIGINAL URLs
    result_map = {}
    for normalized_url, filters in product_filter_map.items():
        original_url = url_to_original[normalized_url]
        result_map[original_url] = filters
    
    return result_map, set(filters.keys())

#-----------------------------------------------------------------------------------------------
def download_image(img_url, img_path, session):
    """Helper function to download a single image using the session."""
    try:
        time.sleep(random.uniform(1.0, 2.0))  # Increased delay
        with session.get(img_url, stream=True, timeout=15) as r:
            r.raise_for_status()
            with open(img_path, 'wb') as f:
                shutil.copyfileobj(r.raw, f)
        return img_path
    except Exception as e:
        return None

#-----------------------------------------------------------------------------------------------
def scrape_product_details(url, session, today_folder, fallback_name, category_path):
    """Scrapes a single product's details."""
    soup = fetch_page(url, session, use_cache=False)  # Don't cache product pages
    if not soup:
        return None

    data = {}
    handle = slug(url.split('/')[-1])

    # NOTE: Removed 'reference URL' as per new format

    sale_price_elem = soup.find('div', class_='price-sale js-price')
    original_price_elem = soup.find('del', class_='price-compare js-price-compare')

    if original_price_elem and sale_price_elem:
        data['price'] = original_price_elem.text.strip()
        data['special price'] = sale_price_elem.text.strip()
    elif sale_price_elem:
        data['special price'] = sale_price_elem.text.strip()

        try:
            num = float(re.sub(r'[^\d.]', '', data['special price']))
            new_price = num * 1.10
            currency = re.search(r'[A-Za-z]+', data['special price']).group()
            data['price'] = f"{currency} {new_price:,.2f}"
        except Exception:
            data['price'] = ''
    else:
        data['price'] = ''
        data['special price'] = ''

    overview_h3 = soup.find('h3')
    if overview_h3 and 'Product Overview for' in overview_h3.text:
        data['Name'] = overview_h3.text.replace('Product Overview for ', '').strip()
    else:
        title_elem = soup.find('h1', class_='product-details_title') or soup.find('title')
        data['Name'] = title_elem.text.strip() if title_elem else fallback_name

    unique_code = generate_unique_id(prefix="AG")
    data['SKU'] = unique_code
    data['ID'] = unique_code
    data['Type'] = 'simple'
    data['Published'] = '1'
    data['Is featured?'] = '0'
    data['Visibility in catalog'] = 'visible'
    data['Categories'] = category_path

    img_elements = soup.select('.product-media_main img[data-srcset]')
    images_to_download = []
    seen = set()
    for img in img_elements:
        if 'data-srcset' in img.attrs:
            img_src = img['data-srcset'].split()[0]
            # Remove ALL query parameters
            img_src = img_src.split('?')[0]
            img_src = img_src.split('&')[0]
            if img_src.startswith('//'):
                img_src = 'https:' + img_src
            if img_src not in seen:
                seen.add(img_src)
                images_to_download.append(img_src)

    downloaded_paths = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as img_executor:
        futures = []
        for img_url in images_to_download:
            clean_url = re.sub(r'[\?&].*$', '', img_url)
            img_name = os.path.basename(clean_url)
            img_path = os.path.join(today_folder, img_name)
            futures.append(img_executor.submit(download_image, img_url, img_path, session))
        
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result:
                downloaded_paths.append(result)

    data['Images'] = ','.join(images_to_download) if images_to_download else ''
    data['Meta: _wp_page_template'] = 'default'

    desc_elem = soup.find('div', id='tab-content-description')
    
    product_desc_text = ''
    if desc_elem:
        all_children = desc_elem.find_all(recursive=False)
        for child in all_children:
            if child.get('id') == 'full_specs' or 'item_content' in child.get('class', []):
                break
            if isinstance(child, Tag):
                if child.name not in ['h2', 'h3', 'div', 'table', 'ul']:
                    product_desc_text += child.text.strip() + ' '
            else:
                product_desc_text += str(child).strip() + ' '

    product_desc_text = product_desc_text.replace('+971 58 665 1195', '+971 55 390 2843')
    product_desc_text = product_desc_text.replace('+971586651195', '+971553902843')
    product_desc_text = product_desc_text.replace('971586651195', '971553902843')

    # Fix missing space in "WarrantyContact"
    product_desc_text = re.sub(r'Warranty\s*Contact', 'Warranty Contact', product_desc_text, flags=re.IGNORECASE)
    product_desc_text = product_desc_text.replace('WarrantyContact', 'Warranty Contact')

    data['product-description'] = product_desc_text.strip()
    
    features = ''
    
    alt_table = soup.find('table', id='product-attribute-specs-table')
    if alt_table:
        tbody = alt_table.find('tbody')
        if not tbody:
            tbody = alt_table
            
        for row in tbody.find_all('tr'):
            key_elem = row.find('th', class_='label')
            val_elem = row.find('td', class_='data')
            
            if key_elem and val_elem:
                key = key_elem.text.strip()
                if key.lower() == 'sku':
                    continue
                value = ' '.join(val_elem.text.split())
                if key and value:
                    features += f"{key}: {value}\n"

    if not features.strip() and desc_elem:
        spec_table = desc_elem.find('table')
        spec_ul = desc_elem.find('ul')

        if spec_table:
            for row in spec_table.find_all('tr'):
                cells = row.find_all('td')
                if len(cells) == 2:
                    features += f"{cells[0].text.strip()}: {cells[1].text.strip()}\n"
        
        elif desc_elem.find('div', class_=['item_name', 'item_content']):
            all_items = desc_elem.find_all('div', class_=['item_name', 'item_content'])
            current_key = None
            
            ports_strong = desc_elem.find(lambda tag: tag.name == 'strong' and 'Standard Ports' in tag.text)
            if ports_strong:
                ports_ul = ports_strong.find_next('ul')
                if ports_ul:
                    list_items = [li.text.strip() for li in ports_ul.find_all('li')]
                    value = ', '.join(list_items)
                    features += f"Standard Ports: {value}\n"

            for item in all_items:
                strong_tag = item.find('strong')
                item_text = ' '.join(item.text.split())
                
                if strong_tag:
                    key_text = strong_tag.text.strip()
                    if key_text:
                        current_key = key_text
                        if key_text.lower() == 'standard ports':
                            current_key = None
                
                elif current_key and item_text:
                    features += f"{current_key}: {item_text}\n"
                    current_key = None
                
                elif current_key and item.find('ul'):
                    ul = item.find('ul')
                    list_items = [li.text.strip() for li in ul.find_all('li')]
                    value = ', '.join(list_items)
                    if value:
                        features += f"{current_key}: {value}\n"
                        current_key = None
            
        elif desc_elem.find('p') and desc_elem.find('strong'):
            current_key = None
            found_features_in_this_block = False
            for child in desc_elem.children:
                if not isinstance(child, Tag):
                    continue

                if child.name == 'p' and child.find('strong'):
                    strong_tag = child.find('strong')
                    if strong_tag.next_sibling and ':' in strong_tag.next_sibling.text:
                         continue

                    current_key = strong_tag.text.strip()
                    br_tag = child.find('br')
                    
                    if br_tag:
                        value = ''
                        next_node = br_tag.next_sibling
                        while next_node:
                            if isinstance(next_node, str):
                                value += next_node
                            elif next_node.name in ['span', 'a']:
                                value += next_node.text
                            elif next_node.name == 'br':
                                value += ' '
                            else:
                                break
                            next_node = next_node.next_sibling
                        
                        value = ' '.join(value.split())
                        if current_key and value:
                            features += f"{current_key}: {value}\n"
                            found_features_in_this_block = True
                            current_key = None
                
                elif child.name == 'ul' and current_key:
                    list_items = [li.text.strip() for li in child.find_all('li')]
                    value = ', '.join(list_items)
                    if current_key and value:
                        features += f"{current_key}: {value}\n"
                        found_features_in_this_block = True
                    current_key = None
            
            if not found_features_in_this_block:
                features = '' 

        if not features.strip() and spec_ul:
            for li in spec_ul.find_all('li'):
                text = li.text.strip()
                if ':' in text:
                    try:
                        key, value = text.split(':', 1)
                        key = key.strip()
                        value = value.strip()
                        if key and value:
                           features += f"{key}: {value}\n"
                    except ValueError:
                        continue
        
        if not features.strip():
            for br in desc_elem.find_all('br'):
                br.replace_with('\n')
            
            text_content = desc_elem.get_text()
            lines = text_content.split('\n')
            
            for line in lines:
                line = line.strip()
                
                if line.startswith('✅'):
                    line = line.lstrip('✅').strip()
                
                if ':' in line:
                    try:
                        key, value = line.split(':', 1)
                        key = key.strip()
                        value = value.strip()
                        if key and value:
                            features += f"{key}: {value}\n"
                    except ValueError:
                        continue

    final_features_list = []
    seen_keys = set()
    for line in features.split('\n'):
        if ':' in line and line.strip():
            try:
                key, value = line.split(':', 1)
                key_clean = key.strip()
                value_clean = value.strip()
                key_lower = key_clean.lower()
                
                if key_lower not in seen_keys and key_clean and value_clean:
                    seen_keys.add(key_lower)
                    final_features_list.append(f"{key_clean}: {value_clean}")
            except ValueError:
                continue
    
    data['features'] = '<br/>'.join([f"✅ {line}" for line in final_features_list])

    specification = ''
    if data['features']:
        brand_match = re.search(r'^(\w+)', data['Name'])
        brand = brand_match.group(1) if brand_match else 'Unknown'
        specification += f'<li>Brand : {brand}</li>\n'
        for line in data['features'].split('\n'):
            if ':' in line and line.strip():
                k, v = line.split(':', 1)
                specification += f'<li>{k.strip()} : {v.strip()}</li>\n'
    specification += '</ul>'
    
    data['specification'] = specification

    return data

#-----------------------------------------------------------------------------------------------
def map_filters_to_attributes(product_filters, all_filter_names):
    """
    Maps filter data to 6 predefined attributes.
    Returns a dict with attribute columns.
    
    Mapping:
    - Attribute 1: Brand
    - Attribute 2: Processor
    - Attribute 3: RAM
    - Attribute 4: Graphics
    - Attribute 5: Generation
    - Attribute 6: Operating System
    """
    # Initialize all 6 attributes
    attributes = {}
    
    # Define the mapping of filter names to attribute positions
    filter_to_attr_map = {
        'Brand': 1,
        'Processor': 2,
        'Ram size': 3,
        'RAM': 3,  # Alternative name
        'Graphics size': 4,
        'Graphics': 4,  # Alternative name
        'Generation(s)': 5,
        'Generation': 5,  # Alternative name
        'Operating system': 6,
        'Operating System': 6  # Alternative name
    }
    
    # Attribute names for each position
    attr_names = {
        1: 'Brand',
        2: 'Processor',
        3: 'RAM',
        4: 'Graphics',
        5: 'Generation',
        6: 'Operating System'
    }
    
    # Initialize all 24 columns (6 attributes × 4 fields)
    for i in range(1, 7):
        attributes[f'Attribute {i} name'] = attr_names.get(i, '')
        attributes[f'Attribute {i} value(s)'] = ''
        attributes[f'Attribute {i} visible'] = '1'
        attributes[f'Attribute {i} global'] = '1'
    
    # Map product filters to attributes
    for filter_name, filter_value in product_filters.items():
        if filter_name in filter_to_attr_map:
            attr_num = filter_to_attr_map[filter_name]
            attributes[f'Attribute {attr_num} value(s)'] = filter_value
    
    return attributes

#-----------------------------------------------------------------------------------------------
def main():
    today = date.today().strftime("%Y-%m-%d")
    today_folder = today
    os.makedirs(today_folder, exist_ok=True)

    csv_filename = f"{today}_products.csv"

    # create an empty file with UTF-8 BOM so Excel / WP importer show emojis correctly
    with open(csv_filename, 'w', encoding='utf-8-sig') as f:
        f.write('\ufeff')
    
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'MyClientScraper/1.0 (contact@clientdomain.com; +http://clientdomain.com/bot-info)'
    })

    print("[INFO] Fetching categories...")
    categories = get_categories(session)
    total_cats = len(categories)
    print(f"[INFO] Found {total_cats} categories")

    # Build complete filter mapping for all products
    all_products_dict = {}  # product_url -> {'category': path, 'filters': {}}
    global_filter_map = {}  # product_url -> {filter_name: filter_value}
    all_filter_names = set()
    
    cat_index = 0
    for cat in categories:
        cat_index += 1
        subcats = cat['subcategories']
        cat_name = cat['name']
        
        if not subcats:
            print(f"\n[{cat_index}/{total_cats}] Processing category: {cat_name}")
            
            # Build filter mapping for this category
            filter_map, filter_names = build_filter_mapping_for_category(cat['url'], session)
            all_filter_names.update(filter_names)
            
            # Merge into global map
            for prod_url, filters in filter_map.items():
                # Add product if new
                if prod_url not in all_products_dict:
                    all_products_dict[prod_url] = {
                        'category': cat_name,
                        'filters': {}
                    }
                
                # Always update filters
                if prod_url not in global_filter_map:
                    global_filter_map[prod_url] = {}
                global_filter_map[prod_url].update(filters)
                
        else:
            for s_index, sub in enumerate(subcats, start=1):
                sub_name = sub['name']
                category_path = f"{cat_name} > {sub_name}"
                print(f"\n[{cat_index}/{total_cats}] {cat_name} → [{s_index}/{len(subcats)}] Processing: {sub_name}")
                
                # Build filter mapping for this subcategory
                filter_map, filter_names = build_filter_mapping_for_category(sub['url'], session)
                all_filter_names.update(filter_names)
                
                # Merge into global map
                for prod_url, filters in filter_map.items():
                    # Add product if new
                    if prod_url not in all_products_dict:
                        all_products_dict[prod_url] = {
                            'category': category_path,
                            'filters': {}
                        }
                    
                    # Always update filters
                    if prod_url not in global_filter_map:
                        global_filter_map[prod_url] = {}
                    global_filter_map[prod_url].update(filters)
    
    # Convert to list for scraping
    all_products_to_scrape = [
        {'url': url, 'category': data['category']}
        for url, data in all_products_dict.items()
    ]
    
    print(f"\n[INFO] Total unique products to scrape: {len(all_products_to_scrape)}")
    print(f"[INFO] Products with filters: {len([p for p in global_filter_map.values() if p])}")
    print(f"[INFO] Products without filters: {len([p for p in global_filter_map.values() if not p])}")
    print(f"[INFO] Total unique filter types found: {len(all_filter_names)}")
    if all_filter_names:
        print(f"[INFO] Filter types: {', '.join(sorted(all_filter_names))}")
    
    print(f"[INFO] Final cache stats: {CACHE_HITS} hits, {CACHE_MISSES} misses")
    if CACHE_HITS + CACHE_MISSES > 0:
        hit_rate = CACHE_HITS / (CACHE_HITS + CACHE_MISSES) * 100
        print(f"[INFO] Cache hit rate: {hit_rate:.1f}% (saved ~{CACHE_HITS * 2}s of requests)")
    
    # NEW FORMAT: Define fieldnames with attributes instead of filter columns
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
    
    # Scrape all product details in parallel
    with open(csv_filename, 'w', newline='', encoding='utf-8-sig') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            future_to_prod = {
                executor.submit(
                    scrape_product_details, 
                    prod['url'], 
                    session, 
                    today_folder, 
                    prod['url'].split('/')[-1],
                    prod['category']
                ): prod
                for prod in all_products_to_scrape
            }
            
            print("\n[INFO] Scraping product details in parallel...")
            for future in tqdm(concurrent.futures.as_completed(future_to_prod), total=len(all_products_to_scrape)):
                try:
                    prod = future_to_prod[future]
                    details = future.result()
                    
                    if details:
                        # Get filter data from our mapping
                        product_filters = global_filter_map.get(prod['url'], {})
                        
                        # Add legacy filter columns (for backwards compatibility)
                        details['Availability'] = '1'
                        details['Brand'] = product_filters.get('Brand', '')
                        details['Generation(s)'] = product_filters.get('Generation(s)', product_filters.get('Generation', ''))
                        details['Graphics size'] = product_filters.get('Graphics size', product_filters.get('Graphics', ''))
                        details['Operating system'] = product_filters.get('Operating system', product_filters.get('Operating System', ''))
                        details['Output Wattage'] = product_filters.get('Output Wattage', '')
                        details['Processor'] = product_filters.get('Processor', '')
                        details['Ram size'] = product_filters.get('Ram size', product_filters.get('RAM', ''))
                        details['Size'] = product_filters.get('Size', '')
                        
                        # Map filters to new attribute format
                        attributes = map_filters_to_attributes(product_filters, all_filter_names)
                        details.update(attributes)
                        
                        writer.writerow(details)
                        csvfile.flush()
                except Exception as e:
                    prod = future_to_prod[future]
                    print(f"\n[ERROR] Failed to process {prod['url']}: {e} (Type: {type(e)})")

    print(f"\n[INFO] Done! CSV: {csv_filename} | Images: {today_folder}")

#-----------------------------------------------------------------------------------------------
if __name__ == "__main__":
    main()