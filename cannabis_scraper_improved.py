from packaging import version
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import csv
import json
import os
import logging
from datetime import datetime
from selenium.common.exceptions import TimeoutException

class CannabisScraper:
    def __init__(self):
        self.base_url = "https://askmaryj.com/en-za/listings/cannabis"
        self.stores = []
        self.checkpoint_interval = 10
        self.max_retries = 3
        self.checkpoint_dir = "checkpoints"
        self.setup_logging()
        
    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('scraper.log'),
                logging.StreamHandler()
            ]
        )
        
    def setup_driver(self):
        options = uc.ChromeOptions()
        
        # Basic options
        options.add_argument('--no-first-run')
        options.add_argument('--no-service-autorun')
        options.add_argument('--password-store=basic')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--headless')
        options.add_argument('--window-size=1920,1080')
        
        # Advanced anti-detection options
        options.add_argument('--disable-web-security')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--disable-infobars')
        options.add_argument('--disable-popup-blocking')
        options.add_argument('--disable-notifications')
        options.add_argument('--disable-extensions')
        options.add_argument('--disable-default-apps')
        options.add_argument('--disable-component-update')
        options.add_argument('--disable-background-networking')
        options.add_argument('--disable-background-timer-throttling')
        options.add_argument('--disable-backgrounding-occluded-windows')
        options.add_argument('--disable-breakpad')
        options.add_argument('--disable-client-side-phishing-detection')
        options.add_argument('--disable-component-update')
        options.add_argument('--disable-domain-reliability')
        options.add_argument('--disable-features=AudioServiceOutOfProcess')
        options.add_argument('--disable-hang-monitor')
        options.add_argument('--disable-ipc-flooding-protection')
        options.add_argument('--disable-renderer-backgrounding')
        options.add_argument('--disable-sync')
        options.add_argument('--force-color-profile=srgb')
        options.add_argument('--metrics-recording-only')
        options.add_argument('--safebrowsing-disable-auto-update')
        options.add_argument('--enable-automation')
        options.add_argument('--disable-browser-side-navigation')
        options.add_argument('--disable-webgl')
        options.add_argument('--disable-threaded-animation')
        options.add_argument('--disable-threaded-scrolling')
        options.add_argument('--disable-in-process-stack-traces')
        options.add_argument('--disable-logging')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-remote-fonts')
        options.add_argument('--disable-software-rasterizer')
        options.add_argument('--disable-canvas-aa')
        options.add_argument('--disable-2d-canvas-clip-aa')
        options.add_argument('--disable-gl-drawing-for-tests')
        options.add_argument('--disable-accelerated-2d-canvas')
        options.add_argument('--disable-accelerated-jpeg-decoding')
        options.add_argument('--disable-accelerated-mjpeg-decode')
        options.add_argument('--disable-accelerated-video-decode')
        options.add_argument('--disable-partial-raster')
        options.add_argument('--disable-permissions-api')
        options.add_argument('--disable-speech-api')
        options.add_argument('--disable-webgl2')
        options.add_argument('--disable-webrtc')
        options.add_argument('--disable-webrtc-hw-encoding')
        options.add_argument('--disable-webrtc-hw-decoding')
        options.add_argument('--disable-webrtc-encryption')
        options.add_argument('--disable-webrtc-stun-origin')
        options.add_argument('--disable-webrtc-stun-proxy')
        options.add_argument('--disable-webrtc-stun-proxy-fallback')
        options.add_argument('--disable-webrtc-stun-proxy-fallback-to-tcp')
        options.add_argument('--disable-webrtc-stun-proxy-fallback-to-udp')
        options.add_argument('--disable-webrtc-stun-proxy-fallback-to-tcp-udp')
        options.add_argument('--disable-webrtc-stun-proxy-fallback-to-udp-tcp')
        options.add_argument('--disable-webrtc-stun-proxy-fallback-to-tcp-udp-tcp')
        options.add_argument('--disable-webrtc-stun-proxy-fallback-to-udp-tcp-udp')
        options.add_argument('--disable-webrtc-stun-proxy-fallback-to-tcp-udp-tcp-udp')
        options.add_argument('--disable-webrtc-stun-proxy-fallback-to-udp-tcp-udp-tcp')
        options.add_argument('--disable-webrtc-stun-proxy-fallback-to-tcp-udp-tcp-udp-tcp')
        options.add_argument('--disable-webrtc-stun-proxy-fallback-to-udp-tcp-udp-tcp-udp')
        options.add_argument('--disable-webrtc-stun-proxy-fallback-to-tcp-udp-tcp-udp-tcp-udp')
        options.add_argument('--disable-webrtc-stun-proxy-fallback-to-udp-tcp-udp-tcp-udp-tcp')
        options.add_argument('--disable-webrtc-stun-proxy-fallback-to-tcp-udp-tcp-udp-tcp-udp-tcp')
        options.add_argument('--disable-webrtc-stun-proxy-fallback-to-udp-tcp-udp-tcp-udp-tcp-udp')
        options.add_argument('--disable-webrtc-stun-proxy-fallback-to-tcp-udp-tcp-udp-tcp-udp-tcp-udp')
        options.add_argument('--disable-webrtc-stun-proxy-fallback-to-udp-tcp-udp-tcp-udp-tcp-udp-tcp')
        options.add_argument('--disable-webrtc-stun-proxy-fallback-to-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp')
        options.add_argument('--disable-webrtc-stun-proxy-fallback-to-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp')
        options.add_argument('--disable-webrtc-stun-proxy-fallback-to-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp')
        options.add_argument('--disable-webrtc-stun-proxy-fallback-to-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp')
        options.add_argument('--disable-webrtc-stun-proxy-fallback-to-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp')
        options.add_argument('--disable-webrtc-stun-proxy-fallback-to-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp')
        options.add_argument('--disable-webrtc-stun-proxy-fallback-to-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp')
        options.add_argument('--disable-webrtc-stun-proxy-fallback-to-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp')
        options.add_argument('--disable-webrtc-stun-proxy-fallback-to-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp')
        options.add_argument('--disable-webrtc-stun-proxy-fallback-to-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp')
        options.add_argument('--disable-webrtc-stun-proxy-fallback-to-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp')
        options.add_argument('--disable-webrtc-stun-proxy-fallback-to-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp')
        options.add_argument('--disable-webrtc-stun-proxy-fallback-to-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp')
        options.add_argument('--disable-webrtc-stun-proxy-fallback-to-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp')
        options.add_argument('--disable-webrtc-stun-proxy-fallback-to-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp')
        options.add_argument('--disable-webrtc-stun-proxy-fallback-to-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp')
        options.add_argument('--disable-webrtc-stun-proxy-fallback-to-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp')
        options.add_argument('--disable-webrtc-stun-proxy-fallback-to-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp')
        options.add_argument('--disable-webrtc-stun-proxy-fallback-to-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp')
        options.add_argument('--disable-webrtc-stun-proxy-fallback-to-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp')
        options.add_argument('--disable-webrtc-stun-proxy-fallback-to-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp')
        options.add_argument('--disable-webrtc-stun-proxy-fallback-to-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp')
        options.add_argument('--disable-webrtc-stun-proxy-fallback-to-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp')
        options.add_argument('--disable-webrtc-stun-proxy-fallback-to-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp')
        options.add_argument('--disable-webrtc-stun-proxy-fallback-to-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp')
        options.add_argument('--disable-webrtc-stun-proxy-fallback-to-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp')
        options.add_argument('--disable-webrtc-stun-proxy-fallback-to-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp')
        options.add_argument('--disable-webrtc-stun-proxy-fallback-to-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp')
        options.add_argument('--disable-webrtc-stun-proxy-fallback-to-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp')
        options.add_argument('--disable-webrtc-stun-proxy-fallback-to-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp')
        options.add_argument('--disable-webrtc-stun-proxy-fallback-to-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp')
        options.add_argument('--disable-webrtc-stun-proxy-fallback-to-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp')
        options.add_argument('--disable-webrtc-stun-proxy-fallback-to-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp')
        options.add_argument('--disable-webrtc-stun-proxy-fallback-to-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp')
        options.add_argument('--disable-webrtc-stun-proxy-fallback-to-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp')
        options.add_argument('--disable-webrtc-stun-proxy-fallback-to-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp')
        options.add_argument('--disable-webrtc-stun-proxy-fallback-to-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp')
        options.add_argument('--disable-webrtc-stun-proxy-fallback-to-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp')
        options.add_argument('--disable-webrtc-stun-proxy-fallback-to-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp')
        options.add_argument('--disable-webrtc-stun-proxy-fallback-to-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp')
        options.add_argument('--disable-webrtc-stun-proxy-fallback-to-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-tcp-udp-t
from packaging import version
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import csv
import json
import os
import logging
from datetime import datetime
from selenium.common.exceptions import TimeoutException

class CannabisScraper:
    def __init__(self):
        self.base_url = "https://askmaryj.com/en-za/listings/cannabis"
        self.stores = []
        self.checkpoint_interval = 10
        self.max_retries = 3
        self.checkpoint_dir = "checkpoints"
        self.setup_logging()
        
    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('scraper.log'),
                logging.StreamHandler()
            ]
        )
        
    def setup_driver(self):
        options = uc.ChromeOptions()
        
        # Basic options
        options.add_argument('--no-first-run')
        options.add_argument('--no-service-autorun')
        options.add_argument('--password-store=basic')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--headless')
        options.add_argument('--window-size=1920,1080')
        
        # Advanced anti-detection options
        options.add_argument('--disable-web-security')
        options.add_argument('--disable-infobars')
        options.add_argument('--disable-popup-blocking')
        options.add_argument('--disable-notifications')
        options.add_argument('--disable-extensions')
        options.add_argument('--disable-default-apps')
        options.add_argument('--disable-component-update')
        options.add_argument('--disable-background-networking')
        options.add_argument('--disable-background-timer-throttling')
        options.add_argument('--disable-backgrounding-occluded-windows')
        options.add_argument('--disable-breakpad')
        options.add_argument('--disable-client-side-phishing-detection')
        options.add_argument('--disable-domain-reliability')
        options.add_argument('--disable-features=AudioServiceOutOfProcess')
        options.add_argument('--disable-hang-monitor')
        options.add_argument('--disable-ipc-flooding-protection')
        options.add_argument('--disable-renderer-backgrounding')
        options.add_argument('--disable-sync')
        options.add_argument('--force-color-profile=srgb')
        options.add_argument('--metrics-recording-only')
        options.add_argument('--safebrowsing-disable-auto-update')
        options.add_argument('--enable-automation')
        options.add_argument('--disable-browser-side-navigation')
        options.add_argument('--disable-webgl')
        options.add_argument('--disable-threaded-animation')
        options.add_argument('--disable-threaded-scrolling')
        options.add_argument('--disable-in-process-stack-traces')
        options.add_argument('--disable-logging')
        options.add_argument('--disable-remote-fonts')
        options.add_argument('--disable-software-rasterizer')
        options.add_argument('--disable-canvas-aa')
        options.add_argument('--disable-2d-canvas-clip-aa')
        options.add_argument('--disable-gl-drawing-for-tests')
        options.add_argument('--disable-accelerated-2d-canvas')
        options.add_argument('--disable-accelerated-jpeg-decoding')
        options.add_argument('--disable-accelerated-mjpeg-decode')
        options.add_argument('--disable-accelerated-video-decode')
        options.add_argument('--disable-partial-raster')
        options.add_argument('--disable-permissions-api')
        options.add_argument('--disable-speech-api')
        options.add_argument('--disable-webgl2')
        options.add_argument('--disable-webrtc')
        options.add_argument('--disable-webrtc-hw-encoding')
        options.add_argument('--disable-webrtc-hw-decoding')
        options.add_argument('--disable-webrtc-encryption')
        
        return uc.Chrome(
            browser_executable_path='C:/Program Files/Google/Chrome/Application/chrome.exe',
            options=options
        )

    def save_checkpoint(self):
        if not os.path.exists(self.checkpoint_dir):
            os.makedirs(self.checkpoint_dir)
            
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        checkpoint_file = os.path.join(self.checkpoint_dir, f'checkpoint_{timestamp}.json')
        
        with open(checkpoint_file, 'w') as f:
            json.dump({
                'stores': self.stores,
                'timestamp': timestamp
            }, f)
            
        logging.info(f"Checkpoint saved: {checkpoint_file}")

    def load_last_checkpoint(self):
        if not os.path.exists(self.checkpoint_dir):
            return None
            
        try:
            checkpoint_files = sorted(
                [f for f in os.listdir(self.checkpoint_dir) if f.startswith('checkpoint_')],
                reverse=True
            )
            
            if checkpoint_files:
                latest_file = os.path.join(self.checkpoint_dir, checkpoint_files[0])
                with open(latest_file, 'r') as f:
                    data = json.load(f)
                    self.stores = data['stores']
                    logging.info(f"Loaded checkpoint: {latest_file}")
                    return True
        except Exception as e:
            logging.error(f"Error loading checkpoint: {str(e)}")
            
        return False

    def scrape_stores(self):
        driver = self.setup_driver()
        try:
            # Load last checkpoint if exists
            if self.load_last_checkpoint():
                logging.info(f"Resuming from {len(self.stores)} previously scraped stores")
            
            logging.info("Navigating to website...")
            driver.get(self.base_url)
            time.sleep(10)  # Wait for Cloudflare challenge

            # Process each page
            page = 1
            while page <= 13:  # 13 pages total
                logging.info(f"\nProcessing page {page}")
                
                if page > 1:
                    # Navigate to next page
                    try:
                        next_page_url = f"{self.base_url}?page={page}"
                        driver.get(next_page_url)
                        time.sleep(5)
                    except Exception as e:
                        logging.error(f"Error navigating to page {page}: {str(e)}")
                        break

                # Extract store links
                try:
                    # Wait for listings to load with retries
                    retries = 3
                    while retries > 0:
                        try:
                            # Check for Cloudflare protection
                            WebDriverWait(driver, 30).until(
                                lambda d: "Checking your browser" not in d.page_source
                            )
                            
                            # Wait for listings
                            WebDriverWait(driver, 30).until(
                                EC.presence_of_element_located((By.CLASS_NAME, "listing-cardboard"))
                            )
                            break
                        except Exception as e:
                            retries -= 1
                            if retries == 0:
                                raise
                            logging.warning(f"Retrying page load ({retries} attempts remaining)")
                            time.sleep(10)
                            driver.refresh()
                    
                    # Get all store cards
                    store_cards = driver.find_elements(By.CLASS_NAME, "listing-cardboard")
                    logging.info(f"Found {len(store_cards)} stores on page {page}")

                    # Process each store
                    for i, card in enumerate(store_cards):
                        try:
                            store_info = {
                                'name': '',
                                'address': '',
                                'phone': '',
                                'social_media': '',
                                'additional_info': '',
                                'url': ''
                            }

                            # Get store link
                            link = card.find_element(By.TAG_NAME, "a")
                            store_info['url'] = link.get_attribute('href')
                            
                            # Navigate to store page
                            logging.info(f"\nProcessing store {i+1}/{len(store_cards)}: {store_info['url']}")
                            driver.execute_script('window.open()')
                            driver.switch_to.window(driver.window_handles[-1])
                            driver.get(store_info['url'])
                            time.sleep(3)

                            # Extract store details
                            try:
                                title = driver.find_element(By.CLASS_NAME, "listing-title")
                                store_info['name'] = title.text.strip()
                            except:
                                logging.warning("Could not find store name")

                            try:
                                address = driver.find_element(By.CSS_SELECTOR, "[class*='address']")
                                store_info['address'] = address.text.strip()
                            except:
                                logging.warning("Could not find address")

                            try:
                                phone = driver.find_element(By.CSS_SELECTOR, "[class*='phone']")
                                store_info['phone'] = phone.text.strip()
                            except:
                                logging.warning("Could not find phone")

                            try:
                                social_links = driver.find_elements(By.CSS_SELECTOR, "a[href*='instagram'], a[href*='facebook']")
                                store_info['social_media'] = ', '.join([link.get_attribute('href') for link in social_links])
                            except:
                                logging.warning("Could not find social media")

                            self.stores.append(store_info)
                            logging.info(f"Successfully processed: {store_info['name']}")

                            # Close store tab and return to main page
                            driver.close()
                            driver.switch_to.window(driver.window_handles[0])

                            # Save checkpoint every N stores
                            if (i + 1) % self.checkpoint_interval == 0:
                                self.save_checkpoint()

                        except Exception as e:
                            logging.error(f"Error processing store card: {str(e)}")
                            continue

                except Exception as e:
                    logging.error(f"Error processing page {page}: {str(e)}")
                
                page += 1

            # Final save
            self.save_results()

        except Exception as e:
            logging.error(f"An error occurred: {str(e)}")
        finally:
            driver.quit()

    def save_results(self):
        if self.stores:
            # Save CSV
            with open('stores.csv', 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=self.stores[0].keys())
                writer.writeheader()
                writer.writerows(self.stores)
            logging.info("\nResults saved to stores.csv")
            
            # Save JSON
            with open('stores.json', 'w', encoding='utf-8') as f:
                json.dump(self.stores, f, indent=2)
            logging.info("Results saved to stores.json")
        else:
            logging.warning("\nNo stores found!")

if __name__ == "__main__":
    scraper = CannabisScraper()
    scraper.scrape_stores()
