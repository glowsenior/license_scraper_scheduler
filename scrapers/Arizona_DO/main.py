#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
import csv
import time
import requests
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from dataclasses import dataclass

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

PROXY_URL = 'http://ptm24webscraping:ptmxx248Dime_country-us@us.proxy.iproyal.com:12323'  
proxies = {  
    "http": PROXY_URL,  
    "https": PROXY_URL  
} 

@dataclass
class ProgressStats:
    total_licenses: int = 0
    processed_licenses: int = 0
    successful_fetches: int = 0
    failed_fetches: int = 0
    new_records: int = 0
    start_time: float = time.time()

    def print_progress(self, license_type=None):
        elapsed_time = time.time() - self.start_time
        if license_type:
            print(f"\n[{license_type}] Progress Update:")
        else:
            print("\nProgresse:")
        print(f"Processed Records: {self.processed_licenses}")
       
        print(f"New Records Added: {self.new_records}")
       
        print("-" * 50)

# Constants
LICENSE_TYPES = [
    'Locum Tenens', 'Osteopathic Physician (DO) Temporary License', 
    'Osteopathic Physician License', 'Osteopathic Physician LOQ',
    'Osteopathic Telehealth Registration', 'Pro Bono Registration',
    'Teaching License', 'Transitional Training Permit'
]
BASE_SEARCH_URL = "https://azdo.portalus.thentiacloud.net/rest/public/profile/search/?keyword=all&skip={}&take=20&lang=en-us&licenseType={}&licenseStatus=all&disciplined=false"
DETAILS_URL = "https://azdo.portalus.thentiacloud.net/rest/public/custom-public-register/profile/individual/"
HEADERS = {
    'accept': 'application/json, text/plain, */*',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36'
}
MAX_WORKERS = 10
RATE_LIMIT = 0.1  # Seconds between requests
PROGRESS_UPDATE_INTERVAL = 20  # Update progress every X records

class LicenseCrawler:
    def __init__(self):
        self.csv_lock = Lock()
        self.stats_lock = Lock()
        self.csv_file = 'result/result.csv'
        self.stats = ProgressStats()
        
        os.makedirs('result', exist_ok=True)
        if not os.path.exists(self.csv_file):
            with open(self.csv_file, 'w', newline='', encoding='utf-8') as file:
                writer = csv.DictWriter(file, fieldnames=self.get_fieldnames())
                writer.writeheader()

    @staticmethod
    def get_fieldnames():
        return ["Full_Name", "License_Type", "License_Number", "Professional", "Status", "Issued", "Expired"]

    def update_stats(self, **kwargs):
        """Thread-safe update of progress statistics"""
        with self.stats_lock:
            for key, value in kwargs.items():
                if hasattr(self.stats, key):
                    setattr(self.stats, key, getattr(self.stats, key) + value)
                    # Print progress update every PROGRESS_UPDATE_INTERVAL records
                    if key == 'processed_licenses' and self.stats.processed_licenses % PROGRESS_UPDATE_INTERVAL == 0:
                        self.stats.print_progress()

    def fetch_total_count(self, license_type):
        """Fetch total record count for a given license type"""
        time.sleep(RATE_LIMIT)
        try:
            url = BASE_SEARCH_URL.format(0, license_type.replace(' ', '%20'))
            response = requests.get(url, headers=HEADERS, proxies=proxies)
            response.raise_for_status()
            count = response.json().get('resultCount', 0)
            print(f"\nFound {count} records for {license_type}")
            return count
        except Exception as e:
            logger.error(f"Error fetching count for {license_type}: {str(e)}")
            return 0

    def fetch_records(self, license_type, skip):
        """Fetch a batch of records"""
        time.sleep(RATE_LIMIT)
        try:
            url = BASE_SEARCH_URL.format(skip, license_type.replace(' ', '%20'))
            response = requests.get(url, headers=HEADERS, proxies=proxies)
            response.raise_for_status()
            self.update_stats(successful_fetches=1)
            return response.json().get('result', {}).get('dataResults', [])
        except Exception as e:
            self.update_stats(failed_fetches=1)
            logger.error(f"Error fetching records: {str(e)}")
            return []

    def fetch_expiry_date(self, record_id):
        """Fetch expiry date for a specific record"""
        time.sleep(RATE_LIMIT)
        try:
            payload = f'{{"id":"{record_id}"}}'
            response = requests.post(DETAILS_URL, headers=HEADERS, data=payload, proxies=proxies)
            response.raise_for_status()
            result = response.json().get('result', {}).get('nameValuePairs', [])
            return result[4].get('value') if len(result) > 4 else ''
        except Exception as e:
            logger.error(f"Error fetching expiry date: {str(e)}")
            return ''

    def format_date(self, raw_date):
        """Format date string"""
        try:
            return datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%S.%fZ').strftime('%m/%d/%Y')
        except (ValueError, TypeError):
            return raw_date

    def process_record(self, record):
        """Process a single record"""
        try:
            columns = record.get('columnValues', [])
            issued_date = self.format_date(columns[5].get('data'))
            expiry_date = self.format_date(self.fetch_expiry_date(record.get('id')))
            
            self.update_stats(processed_licenses=1)
            return {
                'Full_Name': f"{columns[1].get('data', '')} {columns[2].get('data', '')}",
                'License_Type': columns[3].get('data', ''),
                'License_Number': columns[0].get('data', ''),
                'Professional': '',
                'Status': 'Active' if 'ACTIVE' in columns[4].get('data', '') else columns[4].get('data', ''),
                'Issued': issued_date,
                'Expired': expiry_date
            }
        except Exception as e:
            logger.error(f"Error processing record: {str(e)}")
            return None

    def append_to_csv(self, data):
        """Append data to CSV with duplicate checking"""
        with self.csv_lock:
            existing_records = set()
            if os.path.exists(self.csv_file):
                with open(self.csv_file, 'r', newline='', encoding='utf-8') as file:
                    reader = csv.DictReader(file)
                    existing_records = {row['License_Number'] for row in reader}
            
            with open(self.csv_file, 'a', newline='', encoding='utf-8') as file:
                writer = csv.DictWriter(file, fieldnames=self.get_fieldnames())
                for entry in data:
                    if entry and entry['License_Number'] not in existing_records:
                        writer.writerow(entry)
                        self.update_stats(new_records=1)

    def process_license_type(self, license_type):
        """Process data for a given license type"""
        print(f"\nStarting to process: {license_type}")
        total_count = self.fetch_total_count(license_type)
        data_entries = []
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            batch_futures = [
                executor.submit(self.fetch_records, license_type, skip)
                for skip in range(0, total_count, 20)
            ]
            
            for batch_future in as_completed(batch_futures):
                records = batch_future.result()
                record_futures = [
                    executor.submit(self.process_record, record)
                    for record in records
                ]
                
                for future in as_completed(record_futures):
                    result = future.result()
                    if result:
                        data_entries.append(result)
        
        self.append_to_csv(data_entries)
        print(f"\nCompleted processing {license_type}")
        self.stats.print_progress(license_type)

    def run(self):
        """Main execution function"""
        print("Starting license data collection...")
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(self.process_license_type, license_type): license_type 
                for license_type in LICENSE_TYPES
            }
            
            for future in as_completed(futures):
                license_type = futures[future]
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Error processing {license_type}: {str(e)}")
        
        print("\nData collection completed!")
        self.stats.print_progress()

if __name__ == "__main__":
    fetcher = LicenseCrawler()
    fetcher.run()