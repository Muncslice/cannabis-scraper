import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import json
import csv
import logging
from urllib.parse import urljoin
import time
import random
import cloudscraper
from playwright.sync_api import sync_playwright
import backoff
import os
from datetime import datetime, timedelta

# Checkpoint configuration
CHECKPOINT_INTERVAL = 10  # Save progress every 10 stores
CHECKPOINT_DIR = 'checkpoints'
CHECKPOINT_RETENTION_DAYS = 1  # Keep checkpoints for 1 day

# Proxy configuration
USE_PROXIES = False  # Disabled due to reliability issues

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)

BASE_URL = "https://askmaryj.com/en-za/listings/cannabis"

# Expanded user agents
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 16_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Mobile/15E148 Safari/604.1'
]

def get_random_user_agent():
    return random.choice(USER_AGENTS)

def ensure_checkpoint_dir():
    """Create checkpoint directory if it doesn't exist"""
    if not os.path.exists(CHECKPOINT_DIR):
        os.makedirs(CHECKPOINT_DIR)
        logging.info(f"Created checkpoint directory: {CHECKPOINT_DIR}")

def get_latest_checkpoint():
    """Get the most recent checkpoint file"""
    ensure_checkpoint_dir()
    checkpoints = [f for f in os.listdir(CHECKPOINT_DIR) if f.endswith('.json')]
    if not checkpoints:
        return None
        
    # Get the most recent checkpoint
    latest = max(checkpoints, key=lambda f: os.path.getmtime(os.path.join(CHECKPOINT_DIR, f)))
    return os.path.join(CHECKPOINT_DIR, latest)

def save_checkpoint(data, store_count):
    """Save current progress to a checkpoint file"""
    ensure_checkpoint_dir()
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"checkpoint_{timestamp}_store{store_count}.json"
    filepath = os.path.join(CHECKPOINT_DIR, filename)
    
    try:
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
        logging.info(f"Saved checkpoint: {filename}")
    except Exception as e:
        logging.error(f"Failed to save checkpoint: {str(e)}")

def cleanup_old_checkpoints():
    """Remove checkpoint files older than retention period"""
    ensure_checkpoint_dir()
    now = datetime.now()
    cutoff = now - timedelta(days=CHECKPOINT_RETENTION_DAYS)
    
    for filename in os.listdir(CHECKPOINT_DIR):
        filepath = os.path.join(CHECKPOINT_DIR, filename)
        if os.path.isfile(filepath):
            file_time = datetime.fromtimestamp(os.path.getmtime(filepath))
            if file_time < cutoff:
                try:
                    os.remove(filepath)
                    logging.info(f"Removed old checkpoint: {filename}")
                except Exception as e:
                    logging.error(f"Failed to remove checkpoint {filename}: {str(e)}")

HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Referer': 'https://www.google.com/',
    'DNT': '1',
    'Upgrade-Insecure-Requests': '1'
}

@backoff.on_exception(
    backoff.expo,
    (requests.exceptions.RequestException, requests.exceptions.Timeout),
    max_tries=3
)
def make_request(url):
    """Make HTTP request with retry logic and fallback to Playwright"""
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    headers = HEADERS.copy()
    headers['User-Agent'] = get_random_user_agent()
    headers['Accept-Language'] = 'en-US,en;q=0.9'
    headers['Sec-Ch-Ua'] = '"Not_A Brand";v="8", "Chromium";v="120"'
    headers['Sec-Ch-Ua-Mobile'] = '?0'
    headers['Sec-Ch-Ua-Platform'] = '"Windows"'
    headers['Sec-Fetch-Dest'] = 'document'
    headers['Sec-Fetch-Mode'] = 'navigate'
    headers['Sec-Fetch-Site'] = 'same-origin'
    headers['Sec-Fetch-User'] = '?1'
    
    try:
        # First attempt with requests
        response = session.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        return response
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 403:
            # Fallback to Playwright if blocked
            logging.info(f"Request blocked, falling back to Playwright for {url}")
            return make_request_with_playwright(url)
        raise
    except Exception as e:
        logging.error(f"Request failed for {url}: {str(e)}")
        raise

def make_request_with_playwright(url):
    """Make request using Playwright to bypass blocks"""
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        try:
            page.goto(url, timeout=30000)
            content = page.content()
            browser.close()
            return MockResponse(content)
        except Exception as e:
            browser.close()
            raise Exception(f"Playwright request failed: {str(e)}")

class MockResponse:
    """Mock response object for Playwright content"""
    def __init__(self, content):
        self.content = content.encode('utf-8')
        self.status_code = 200

def scrape_store(store_url):
    """Scrape individual store details"""
    response = make_request(store_url)
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Extract store details
    store_data = {
        'name': soup.find('h1').text.strip(),
        'address': soup.select_one('.store-address').text.strip(),
        'phone': soup.select_one('.store-phone').text.strip(),
        'website': soup.select_one('.store-website')['href'] if soup.select_one('.store-website') else None
    }
    return store_data

def scrape_all_stores():
    """Main function to scrape all stores with checkpointing"""
    # Clean up old checkpoints before starting
    cleanup_old_checkpoints()
    
    # Check for existing checkpoint
    checkpoint_file = get_latest_checkpoint()
    if checkpoint_file:
        logging.info(f"Resuming from checkpoint: {checkpoint_file}")
        with open(checkpoint_file, 'r') as f:
            data = json.load(f)
        stores = data['stores']
        start_index = data['last_index'] + 1
    else:
        logging.info("Starting new scrape session")
        stores = []
        start_index = 0
    
    # Get list of store URLs using Playwright to handle JavaScript rendering
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(BASE_URL, timeout=30000)
        
        # Wait for store listings to load with longer timeout
        try:
            # Set browser-like headers
            page.set_extra_http_headers({
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Referer': 'https://www.google.com/',
                'DNT': '1',
                'Upgrade-Insecure-Requests': '1'
            })
            
            # Add random delays to mimic human behavior
            time.sleep(random.uniform(1, 3))
            
            # Navigate with longer timeout and wait for network idle
            page.goto(BASE_URL, timeout=120000, wait_until='networkidle')
            
            # Handle Cloudflare challenge if present
            try:
                page.wait_for_selector('#challenge-running', state='attached', timeout=10000)
                logging.info("Cloudflare challenge detected, waiting for completion...")
                page.wait_for_selector('#challenge-running', state='detached', timeout=60000)
                logging.info("Cloudflare challenge completed")
            except Exception as e:
                logging.info("No Cloudflare challenge detected")
            
            # Save initial state
            try:
                page.screenshot(path='page_screenshot_initial.png')
                logging.info("Saved initial page screenshot")
            except Exception as e:
                logging.warning(f"Could not save initial screenshot: {str(e)}")
            
            # Save page content regardless of selector success
            content = page.content()
            with open('page_content.html', 'w', encoding='utf-8') as f:
                f.write(content)
            logging.info("Saved page content to page_content.html")
            
            # Try multiple selector approaches with increased timeout
            try:
                # First attempt with original selector
                page.wait_for_selector('div.store-listing', state='attached', timeout=30000)
            except Exception as e:
                logging.warning(f"Original selector failed: {str(e)}")
                # Fallback to more generic container
                try:
                    page.wait_for_selector('.listings-container', state='attached', timeout=30000)
                except Exception as e:
                    logging.warning(f"Fallback selector failed: {str(e)}")
                    # Final fallback to body content
                    page.wait_for_selector('body', state='attached', timeout=30000)
            
            # Save final state
            try:
                page.screenshot(path='page_screenshot_final.png')
                logging.info("Saved final page screenshot")
            except Exception as e:
                logging.warning(f"Could not save final screenshot: {str(e)}")
            
            # Extract store links
            store_links = page.eval_on_selector_all(
                'div.store-listing > a',
                'elements => elements.map(el => el.href)'
            )
        except Exception as e:
            logging.error(f"Failed to load store listings: {str(e)}")
            raise
        browser.close()
    
    # Convert relative URLs to absolute
    store_links = [urljoin(BASE_URL, link) for link in store_links]
    
    total_stores = len(store_links)
    logging.info(f"Found {total_stores} stores to scrape")
    
    for i in range(start_index, total_stores):
        try:
            store_url = store_links[i]
            logging.info(f"Scraping store {i+1}/{total_stores}: {store_url}")
            
            store_data = scrape_store(store_url)
            stores.append(store_data)
            
            # Save checkpoint every CHECKPOINT_INTERVAL stores
            if (i + 1) % CHECKPOINT_INTERVAL == 0:
                save_checkpoint({
                    'stores': stores,
                    'last_index': i
                }, i + 1)
                
        except Exception as e:
            logging.error(f"Failed to scrape store {i+1}: {str(e)}")
            continue
    
    # Final save after completion
    save_checkpoint({
        'stores': stores,
        'last_index': total_stores - 1
    }, total_stores)
    
    return stores

def save_results(stores):
    """Save scraped data to JSON and CSV files"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Save to JSON
    json_file = f'cannabis_stores_{timestamp}.json'
    with open(json_file, 'w') as f:
        json.dump(stores, f, indent=2)
    logging.info(f"Saved results to {json_file}")
    
    # Save to CSV
    csv_file = f'cannabis_stores_{timestamp}.csv'
    with open(csv_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['name', 'address', 'phone', 'website'])
        writer.writeheader()
        writer.writerows(stores)
    logging.info(f"Saved results to {csv_file}")

if __name__ == '__main__':
    try:
        stores = scrape_all_stores()
        save_results(stores)
        logging.info("Scraping completed successfully")
    except KeyboardInterrupt:
        logging.info("Scraping interrupted by user")
    except Exception as e:
        logging.error(f"Scraping failed: {str(e)}")
