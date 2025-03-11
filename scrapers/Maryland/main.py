import requests
from bs4 import BeautifulSoup
import time
import csv
import os
import logging
import random
import string
from threading import Lock
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import urllib3

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('crawler.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class LicenseCrawler:
    def __init__(self):
        """Initialize the crawler."""
        self.base_url = "https://www.mbp.state.md.us/bpqapp/"
        self.csv_lock = Lock()
        self.processed_license_numbers = set()

        # Disable SSL warnings
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }

    def create_session(self):
        """Create a new session with retry mechanism."""
        session = requests.Session()
        retry = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('https://', adapter)
        return session

    def fetch_tokens(self, soup):
        """Extract dynamic tokens from the initial page response."""
        return {
            "__VIEWSTATE": soup.find('input', attrs={'name': '__VIEWSTATE'})['value'],
            "__VIEWSTATEGENERATOR": soup.find('input', attrs={'name': '__VIEWSTATEGENERATOR'})['value'],
            "__EVENTVALIDATION": soup.find('input', attrs={'name': '__EVENTVALIDATION'})['value'],
        }

    def fetch_profile(self, selected_name, tokens, session):
        """Fetch profile details for a selected name."""
        payload_profile = {
            "__EVENTTARGET": "",
            "__EVENTARGUMENT": "",
            "__VIEWSTATE": tokens["__VIEWSTATE"],
            "__VIEWSTATEGENERATOR": tokens["__VIEWSTATEGENERATOR"],
            "__EVENTVALIDATION": tokens["__EVENTVALIDATION"],
            "listbox_Names": selected_name,
            "Btn_Name": "Get Profile"
        }

        try:
            profile_response = session.post(
                self.base_url, 
                data=payload_profile, 
                headers=self.headers, 
                verify=False
            )
            if profile_response.status_code == 200:
                profile_soup = BeautifulSoup(profile_response.text, 'html.parser')
                return {
                    "Full_Name": profile_soup.find(id="Name").text.strip(),
                    "License_Type": profile_soup.find(id="Lic_Type").text.strip(),
                    "License_Number": profile_soup.find(id="Lic_no").text.strip(),
                    "Professional": "",  # Not available in Maryland data
                    "Status": profile_soup.find(id="Lic_Status").text.strip(),
                    "Issued": profile_soup.find(id="Org_Lic_Date").text.strip(),
                    "Expired": profile_soup.find(id="Expiration_Date").text.strip()
                }
            else:
                logger.error(f"Profile request for {selected_name} failed with status: {profile_response.status_code}")
                return None
        except Exception as e:
            logger.error(f"Error fetching profile for {selected_name}: {e}")
            return None

    def append_to_csv(self, profile_data):
        """Append profile data to CSV file in a thread-safe manner."""
        if profile_data:
            with self.csv_lock:
                with open('result/result.csv', 'a', newline='', encoding='utf-8') as csvfile:
                    fieldnames = ["Full_Name", "License_Type", "License_Number", "Professional", "Status", "Issued", "Expired"]
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writerow(profile_data)

    def process_lastname(self, lastname, session):
        """Process all profiles for a given last name initial."""
        try:
            response = session.get(self.base_url, headers=self.headers, verify=False)
            if response.status_code != 200:
                logger.error(f"Initial session request failed for {lastname} with status: {response.status_code}")
                return

            soup = BeautifulSoup(response.text, 'html.parser')
            tokens = self.fetch_tokens(soup)
            
            payload = {
                "__EVENTTARGET": "",
                "__EVENTARGUMENT": "",
                "__VIEWSTATE": tokens["__VIEWSTATE"],
                "__VIEWSTATEGENERATOR": tokens["__VIEWSTATEGENERATOR"],
                "__EVENTVALIDATION": tokens["__EVENTVALIDATION"],
                "LastName": lastname,
                "btnLastName": "Submit",
                "Lic_No": ""
            }

            search_response = session.post(
                self.base_url, 
                data=payload, 
                headers=self.headers, 
                verify=False
            )
            if search_response.status_code != 200:
                logger.error(f"Search request failed for {lastname}")
                return

            search_soup = BeautifulSoup(search_response.text, 'html.parser')
            tokens.update({
                input_tag['name']: input_tag['value'] 
                for input_tag in search_soup.find_all('input', type='hidden')
            })
            
            listbox = search_soup.find('select', id='listbox_Names')
            if not listbox:
                logger.warning(f"No results found for lastname {lastname}")
                return

            options = listbox.find_all('option')
            logger.info(f"Processing {len(options)} profiles for lastname {lastname}")
            processed_count = 0
            skipped_count = 0
            
            for option in options:
                license_key = option['value']
                with self.csv_lock:
                    if license_key in self.processed_license_numbers:
                        logger.debug(f"Skipping already processed license: {license_key}")
                        skipped_count += 1
                        continue
                    self.processed_license_numbers.add(license_key)
                
                profile_data = self.fetch_profile(option['value'], tokens, session)
                if profile_data:
                    self.append_to_csv(profile_data)
                    processed_count += 1
                time.sleep(random.uniform(1, 3))

            logger.info(f"Lastname {lastname}: Processed {processed_count} new profiles, Skipped {skipped_count} duplicates")

        except Exception as e:
            logger.error(f"Error processing lastname {lastname}: {e}")

    def worker(self, lastname_range):
        """Worker function to process a range of lastnames."""
        session = self.create_session()
        for lastname in lastname_range:
            logger.info(f"Processing lastname: {lastname}")
            self.process_lastname(lastname, session)
            time.sleep(random.uniform(1, 5))

    def load_existing_licenses(self):
        """Load existing license numbers from CSV if it exists."""
        try:
            if os.path.exists('result/result.csv'):
                with open('result/result.csv', 'r', newline='', encoding='utf-8') as csvfile:
                    reader = csv.DictReader(csvfile)
                    for row in reader:
                        self.processed_license_numbers.add(row['License_Number'])
                logger.info(f"Loaded {len(self.processed_license_numbers)} existing license numbers")
        except Exception as e:
            logger.error(f"Error loading existing licenses: {e}")

    def run(self):
        """Run the crawler with multiple threads."""
        os.makedirs('result', exist_ok=True)
        
        # Load existing licenses
        self.load_existing_licenses()
        
        # Initialize CSV file if it doesn't exist
        if not os.path.exists('result/result.csv'):
            with open('result/result.csv', 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ["Full_Name", "License_Type", "License_Number", "Professional", "Status", "Issued", "Expired"]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()

        # Split work among 5 threads
        all_letters = list(string.ascii_uppercase)
        chunk_size = len(all_letters) // 5
        letter_chunks = [all_letters[i:i + chunk_size] for i in range(0, len(all_letters), chunk_size)]

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(self.worker, chunk) for chunk in letter_chunks]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Thread failed: {str(e)}")

if __name__ == "__main__":
    crawler = LicenseCrawler()
    crawler.run()