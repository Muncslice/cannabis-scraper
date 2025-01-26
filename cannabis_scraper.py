import requests
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

# Updated proxy list with fresh proxies
PROXIES = [
    'http://45.88.108.149:3128',
    'http://45.88.108.150:3128', 
    'http://45.88.108.151:3128',
    'http://45.88.108.152:3128',
    'http://45.88.108.153:3128'
]

def get_random_proxy():
    return random.choice(PROXIES)

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

HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Referer': 'https://www.google.com/',
    'DNT': '1',
    'Upgrade-Insecure-Requests': '1'
}

@backoff.on_exception(backoff.expo,
                     (requests.exceptions.RequestException,
                      requests.exceptions.Timeout),
                     max_tries=3)
# Rate limiter to prevent overwhelming the server
class RateLimiter:
    def __init__(self, max_requests=5, per_seconds=10):
        self.max_requests = max_requests
        self.per_seconds = per_seconds
        self.request_times = []
        
    def wait(self):
        now = time.time()
        # Remove old request times
        self.request_times = [t for t in self.request_times if now - t < self.per_seconds]
        
        if len(self.request_times) >= self.max_requests:
            sleep_time = self.per_seconds - (now - self.request_times[0])
            logging.info(f"Rate limit reached, sleeping for {sleep_time:.1f} seconds")
            time.sleep(sleep_time)
            
        self.request_times.append(time.time())

rate_limiter = RateLimiter(max_requests=5, per_seconds=10)

def get_page_content(url):
    try:
        # Apply rate limiting
        rate_limiter.wait()
        
        # Try with cloudscraper and proxies first
        scraper = cloudscraper.create_scraper()
        
        headers = HEADERS.copy()
        headers['User-Agent'] = get_random_user_agent()
        headers['Sec-Fetch-Dest'] = 'document'
        headers['Sec-Fetch-Mode'] = 'navigate'
        headers['Sec-Fetch-Site'] = 'same-origin'
        headers['Sec-Fetch-User'] = '?1'
        
        # Add random delay between 1-3 seconds
        time.sleep(random.uniform(1, 3))
        
        # Try with proxy first
        proxy = get_random_proxy()
        proxies = {
            'http': proxy,
            'https': proxy
        }
        
        try:
            # First request to get cookies
            response = scraper.get(
                BASE_URL,
                headers=headers,
                proxies=proxies,
                timeout=10
            )
            
            # Second request with cookies
            response = scraper.get(
                url,
                headers=headers,
                proxies=proxies,
                timeout=10
            )
            
            # Check for anti-bot protection
            if response.status_code == 403 or 'cf-chl-bypass' in response.text.lower():
                logging.warning("Anti-bot protection detected - trying headless browser")
                return get_page_content_headless(url)
                
            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException:
            # If proxy fails, try without proxy
            response = scraper.get(
                url,
                headers=headers,
                timeout=10
            )
            return response.text
            
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching {url}: {e}")
        return get_page_content_headless(url)

def get_page_content_headless(url):
    """Fallback to headless browser when requests fail"""
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, timeout=30000)  # 30 second timeout
            context = browser.new_context(
                user_agent=get_random_user_agent(),
                viewport={'width': 1920, 'height': 1080}
            )
            page = context.new_page()
            
            # Add random delay before navigation
            time.sleep(random.uniform(1, 3))
            
            # Add additional navigation options
            page.goto(url, timeout=30000, wait_until='domcontentloaded')
            
            # Wait for page to load with more robust checks
            try:
                page.wait_for_load_state('networkidle', timeout=10000)
            except:
                page.wait_for_load_state('load', timeout=10000)
            
            # Verify page content
            content = page.content()
            if not content or len(content) < 100:
                raise ValueError("Page content too short or empty")
                
            return content
    except Exception as e:
        logging.error(f"Headless browser failed for {url}: {str(e)}")
        return None
    finally:
        if 'browser' in locals():
            try:
                browser.close()
            except Exception as e:
                logging.warning(f"Error closing browser: {str(e)}")

def validate_store_data(store_data):
    """Validate the scraped store data meets quality standards"""
    if not store_data:
        return False
        
    # Validate required fields
    required_fields = ['name', 'address', 'phone']
    for field in required_fields:
        if not store_data.get(field) or len(store_data[field].strip()) < 2:
            logging.warning(f"Missing or invalid {field} in store data")
            return False
            
    # Validate phone number format
    phone = store_data['phone']
    if not any(char.isdigit() for char in phone):
        logging.warning(f"Invalid phone number format: {phone}")
        return False
        
    # Validate social media URLs
    if 'social_media' in store_data:
        for platform, links in store_data['social_media'].items():
            for link in links:
                if not link.startswith(('http://', 'https://')):
                    logging.warning(f"Invalid URL format in {platform}: {link}")
                    return False
                    
    # Validate minimum length requirements
    if len(store_data['name']) < 2:
        logging.warning(f"Store name too short: {store_data['name']}")
        return False
        
    if len(store_data['address']) < 10:
        logging.warning(f"Address too short: {store_data['address']}")
        return False
        
    return True

def parse_store(store_url):
    store_data = {}
    html = get_page_content(store_url)
    if not html:
        return None
        
    soup = BeautifulSoup(html, 'html.parser')
    
    try:
        # Extract store name
        store_data['name'] = soup.find('h1', class_='store-title').text.strip()
        
        # Extract address
        address = soup.find('div', class_='store-address')
        if address:
            store_data['address'] = address.text.strip()
        
        # Extract phone
        phone = soup.find('div', class_='store-phone')
        if phone:
            store_data['phone'] = phone.text.strip()
        
        # Extract social media - specifically look for Instagram
        social_links = soup.find_all('a', class_='social-link')
        store_data['social_media'] = {
            'instagram': [link['href'] for link in social_links if 'instagram.com' in link['href']],
            'other': [link['href'] for link in social_links if 'instagram.com' not in link['href']]
        }
        
        # Validate the data before returning
        if not validate_store_data(store_data):
            logging.warning(f"Invalid store data for {store_url}")
            return None
            
        return store_data
    except Exception as e:
        logging.error(f"Error parsing {store_url}: {e}")
        return None

def scrape_store_with_retries(store_url, max_retries=3):
    """Scrape a store with retry mechanism"""
    retry_count = 0
    base_delay = 2  # Initial delay in seconds
    
    while retry_count < max_retries:
        try:
            store_data = parse_store(store_url)
            if store_data:
                return store_data
                
            retry_count += 1
            if retry_count < max_retries:
                delay = base_delay * (2 ** retry_count)  # Exponential backoff
                delay += random.uniform(0, 1)  # Add jitter
                logging.warning(f"Retry {retry_count} for {store_url} in {delay:.1f} seconds")
                time.sleep(delay)
                
        except Exception as e:
            retry_count += 1
            if retry_count < max_retries:
                delay = base_delay * (2 ** retry_count)
                delay += random.uniform(0, 1)
                logging.warning(f"Error on retry {retry_count} for {store_url}: {str(e)}")
                time.sleep(delay)
                
    logging.error(f"Failed to scrape {store_url} after {max_retries} attempts")
    return None

class ProgressTracker:
    def __init__(self):
        self.start_time = time.time()
        self.total_pages = 0
        self.total_stores = 0
        self.completed_pages = 0
        self.completed_stores = 0
        self.successful_stores = 0
        self.failed_stores = 0
        
    def estimate_total_pages(self, first_page_html):
        """Estimate total pages by checking pagination"""
        soup = BeautifulSoup(first_page_html, 'html.parser')
        pagination = soup.find('div', class_='pagination')
        if pagination:
            page_links = pagination.find_all('a', class_='page-link')
            if page_links:
                try:
                    return int(page_links[-2].text)  # Second last is usually last page number
                except (ValueError, IndexError):
                    pass
        return 13  # Default to known page count
    
    def estimate_total_stores(self, first_page_html):
        """Estimate total stores from first page"""
        soup = BeautifulSoup(first_page_html, 'html.parser')
        store_cards = soup.find_all('div', class_='store-card')
        return len(store_cards) * self.total_pages  # Assume consistent store count per page
    
    def get_progress(self):
        """Calculate and return progress metrics"""
        elapsed = time.time() - self.start_time
        page_progress = self.completed_pages / self.total_pages * 100 if self.total_pages else 0
        store_progress = self.completed_stores / self.total_stores * 100 if self.total_stores else 0
        
        # Calculate estimated time remaining
        if self.completed_pages > 0:
            avg_time_per_page = elapsed / self.completed_pages
            remaining_time = avg_time_per_page * (self.total_pages - self.completed_pages)
        else:
            remaining_time = 0
            
        return {
            'pages': {
                'completed': self.completed_pages,
                'total': self.total_pages,
                'progress': page_progress
            },
            'stores': {
                'completed': self.completed_stores,
                'total': self.total_stores,
                'progress': store_progress,
                'successful': self.successful_stores,
                'failed': self.failed_stores
            },
            'time': {
                'elapsed': elapsed,
                'remaining': remaining_time
            }
        }
        
    def log_progress(self):
        """Log current progress status"""
        progress = self.get_progress()
        logging.info(
            f"Progress: Pages {progress['pages']['completed']}/{progress['pages']['total']} "
            f"({progress['pages']['progress']:.1f}%) | "
            f"Stores {progress['stores']['completed']}/{progress['stores']['total']} "
            f"({progress['stores']['progress']:.1f}%) | "
            f"Success: {progress['stores']['successful']} | "
            f"Failures: {progress['stores']['failed']} | "
            f"Elapsed: {progress['time']['elapsed']/60:.1f}m | "
            f"Remaining: {progress['time']['remaining']/60:.1f}m"
        )

def cleanup_old_checkpoints():
    """Remove checkpoint files older than 1 day"""
    try:
        checkpoint_files = [f for f in os.listdir() 
                          if f.startswith('checkpoint_') and f.endswith('.json')]
        
        now = time.time()
        for f in checkpoint_files:
            file_time = os.path.getctime(f)
            if now - file_time > 86400:  # 1 day in seconds
                try:
                    os.remove(f)
                    logging.info(f"Removed old checkpoint: {f}")
                except Exception as e:
                    logging.warning(f"Error removing {f}: {e}")
    except Exception as e:
        logging.error(f"Error cleaning up checkpoints: {e}")

def scrape_all_stores():
    all_stores = []
    page_number = 1
    progress = ProgressTracker()
    
    # Try to load from last checkpoint
    checkpoint_data = load_last_checkpoint()
    if checkpoint_data:
        all_stores = checkpoint_data
        progress.completed_stores = len(all_stores)
        progress.successful_stores = len(all_stores)
        logging.info(f"Resuming from checkpoint with {len(all_stores)} stores")
    
    # Get first page to estimate totals
    first_page_url = f"{BASE_URL}?page={page_number}"
    first_page_html = get_page_content(first_page_url)
    if not first_page_html:
        return all_stores
        
    progress.total_pages = progress.estimate_total_pages(first_page_html)
    progress.total_stores = progress.estimate_total_stores(first_page_html)
    
    # Clean up old checkpoints
    cleanup_old_checkpoints()
    
    while True:
        page_url = f"{BASE_URL}?page={page_number}"
        logging.info(f"Scraping page {page_number}/{progress.total_pages}")
        
        html = get_page_content(page_url)
        if not html:
            break
            
        soup = BeautifulSoup(html, 'html.parser')
        store_cards = soup.find_all('div', class_='store-card')
        
        if not store_cards:
            break
            
        for card in store_cards:
            store_link = card.find('a', class_='store-link')['href']
            full_store_url = urljoin(BASE_URL, store_link)
            
            store_data = scrape_store_with_retries(full_store_url)
            progress.completed_stores += 1
            if store_data:
                all_stores.append(store_data)
                progress.successful_stores += 1
            else:
                progress.failed_stores += 1
                
            # Save checkpoint and log progress every 10 stores
            if progress.completed_stores % 10 == 0:
                if save_data(all_stores, checkpoint=True):
                    logging.info(f"Checkpoint saved at {progress.completed_stores} stores")
                progress.log_progress()
        
        # Check for next page
        next_button = soup.find('a', class_='next-page')
        if not next_button:
            break
            
        page_number += 1
        progress.completed_pages += 1
        time.sleep(random.uniform(2, 5))  # Random delay between pages
        
    # Final progress report
    progress.log_progress()
        
    return all_stores

def save_data(stores, format='both', checkpoint=False):
    """Save store data with optional checkpointing"""
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    prefix = 'checkpoint_' if checkpoint else ''
    
    if format in ('csv', 'both'):
        csv_filename = f'{prefix}cannabis_stores_{timestamp}.csv'
        try:
            with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['name', 'address', 'phone', 'social_media']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                for store in stores:
                    # Flatten social media data for CSV
                    store_csv = store.copy()
                    store_csv['social_media'] = '\n'.join(
                        [f"Instagram: {', '.join(store['social_media']['instagram'])}",
                         f"Other: {', '.join(store['social_media']['other'])}"]
                    )
                    writer.writerow(store_csv)
            logging.info(f"Data saved to {csv_filename}")
        except Exception as e:
            logging.error(f"Error saving CSV: {e}")
            return False
            
    if format in ('json', 'both'):
        json_filename = f'{prefix}cannabis_stores_{timestamp}.json'
        try:
            with open(json_filename, 'w', encoding='utf-8') as jsonfile:
                json.dump(stores, jsonfile, indent=2)
            logging.info(f"Data saved to {json_filename}")
        except Exception as e:
            logging.error(f"Error saving JSON: {e}")
            return False
            
    return True

def load_last_checkpoint():
    """Load the most recent checkpoint file"""
    try:
        # Find all checkpoint files
        checkpoint_files = [f for f in os.listdir() 
                          if f.startswith('checkpoint_') and f.endswith('.json')]
        
        if not checkpoint_files:
            return None
            
        # Get the most recent file
        latest_file = max(checkpoint_files, key=os.path.getctime)
        
        with open(latest_file, 'r', encoding='utf-8') as f:
            return json.load(f)
            
    except Exception as e:
        logging.error(f"Error loading checkpoint: {e}")
        return None

def main():
    logging.info("Starting cannabis store scraper")
    stores = scrape_all_stores()
    
    if stores:
        save_data(stores)
        logging.info(f"Successfully scraped {len(stores)} stores")
    else:
        logging.warning("No stores were scraped")
    
    logging.info("Scraping complete")

if __name__ == "__main__":
    main()
