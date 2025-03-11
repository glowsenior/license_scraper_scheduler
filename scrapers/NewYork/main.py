import csv
import logging
import os
import time
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

import urllib3

# Suppress specific warnings
warnings.filterwarnings('ignore', category=urllib3.exceptions.InsecureRequestWarning)

import requests

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class LicenseCrawler:

    def __init__(self):

        """Initialize the crawler."""
        self.base_url = 'https://api.nysed.gov/rosa/V2/byProfessionAndName'
        self.detail_url = 'https://api.nysed.gov/rosa/V2/byProfessionAndLicenseNumber'
        self.output_file = 'results/results.csv'
        self.csv_lock = Lock()
        self.max_workers = 4
        self.proxy_url = "http://cruexuku-US-rotate:c3h2jphwjv7y@p.webshare.io:80"
        self.proxy = {"http": self.proxy_url,
                      "https": self.proxy_url}
        self.headers = {
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
            'Origin': 'https://eservices.nysed.gov',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-site',
            'Sec-GPC': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36',
            'sec-ch-ua': '"Not A(Brand";v="8", "Chromium";v="132", "Brave";v="132"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'x-oapi-key': 'BRJF4D6U646A5PNMIB77AAW9544QFQKAYAEWI9EPU0TNP72CEEO3L4KGVN5K3R44',
        }

        self.page_size = 200
        # PHYSICIAN
        self.target_types = ["060"]
        self.search_filters = []
        for target_type in self.target_types:
            for letter in list("ABCDEFGHIJKLMNOPQRSTUVWXYZ"):
                self.search_filters.append((target_type, letter))
        os.makedirs('results', exist_ok=True)

    def fetch_licensee_data(self, specialty_value, search_letter, page_num='0', retries=5):
        """Submit the form for a given specialty, search filter and fetch the results page."""

        while retries:
            retries -= 1
            params = {
                'name': search_letter,
                'professionCode': specialty_value,
                'pageNumber': page_num,
                'pageSize': '200',
            }

            logger.info(
                f"Fetching listing page={page_num} for search filter {search_letter}")

            try:
                response = requests.get(self.base_url, headers=self.headers, params=params, proxies=self.proxy)
                response.raise_for_status()
                return response.json()
            except Exception as e:
                logger.error(f"Retrying ... Failed to fetch data for search_filter {search_letter}: {e}")
                time.sleep(5)
        return None

    def save_to_csv(self, results):
        """Save results to CSV in a thread-safe manner."""
        with self.csv_lock:
            with open(self.output_file, 'a', newline='', encoding='utf-8') as csvfile:
                fieldnames = ["Full_Name", "License_Type", "License_Number", "Status", "Professional", "Issued",
                              "Expired"]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                for result in results:
                    writer.writerow(result)

    def parse_listing_page(self, json_content, current_page_num=1):
        total_pages = None
        records = json_content.get("content", [])
        if current_page_num == 1:
            total_pages = int(json_content.get("totalPages", 0))

        license_numbers = set()

        for record in records:
            license_number = record.get("licenseNumber", {}).get("value", "")
            license_numbers.add(license_number)
        return license_numbers, total_pages

    def fetch_detail_page(self, detail_page_id, index, specialty_value, search_letter, retries=5):
        """Submit the form for a given details page."""

        while retries:
            retries -= 1
            logger.info(f"Fetching details {index} For ID: {detail_page_id} Letter: {search_letter}")
            try:

                params = {
                    'licenseNumber': detail_page_id,
                    'professionCode': specialty_value,
                }
                response = requests.get(self.detail_url, headers=self.headers, params=params, proxies=self.proxy)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as e:
                logger.error(
                    f"Retrying __ Failed to fetch data for detail page {index} for ID: {detail_page_id} : {e}")
                time.sleep(5)
        return None

    def parse_licensee_data(self, record):
        """Parse the license information from JSON Details"""
        if not record:
            return None
        extracted_data = {
            "Full_Name": record.get("name", {}).get("value", {}),
            "License_Type": "MD/DO",
            "License_Number": record.get("licenseNumber", {}).get("value", {}),
            "Status": record.get("status", {}).get("value", {}),
            "Professional": record.get("profession", {}).get("value", {}),
            "Issued": record.get("dateOfLicensure", {}).get("value", {}),
            "Expired": record.get("registeredThroughDate", {}).get("value", {})
        }
        if extracted_data:
            self.save_to_csv([extracted_data])
        return True

    # A function for processing each results_id.
    def process_license_id(self, index, results_id, specialty_value, search_letter):
        return self.parse_licensee_data(
            self.fetch_detail_page(results_id, index, specialty_value, search_letter)
        )

    def extract_data(self, results_ids, specialty_value, search_letter):

        # Start threading with ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks and store the futures.
            futures = {
                executor.submit(self.process_license_id, i, results_id, specialty_value, search_letter): results_id
                for i, results_id in enumerate(results_ids, start=1)}

            # Wait for all tasks to complete
            for x in as_completed(futures):
                pass

    def remove_duplicates_in_csv(self):
        """Remove duplicate rows in the CSV based on all columns."""
        with self.csv_lock:
            # Read existing data from the file
            try:
                with open(self.output_file, 'r', encoding='utf-8') as csvfile:
                    reader = csv.DictReader(csvfile)
                    existing_data = list(reader)
            except FileNotFoundError:
                existing_data = []

            duplicate_count = 0
            # Remove duplicates based on all columns
            seen_rows = set()
            unique_data = []
            for row in existing_data:
                # Create a tuple of all values in the row to track duplicates
                row_tuple = tuple(row.items())  # Hashable representation of the row
                if row_tuple not in seen_rows:
                    seen_rows.add(row_tuple)
                    unique_data.append(row)
                else:
                    duplicate_count += 1

            logger.info(f"Duplicates found: {duplicate_count}")

            # Write the unique data back to the CSV
            if existing_data:  # Ensure there is data to infer fieldnames
                fieldnames = existing_data[0].keys()
            else:
                fieldnames = []

            with open(self.output_file, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(unique_data)

    def crawl_specialty(self, search_letter, specialty_value):
        """Crawl and process licensee data for a given search filter."""
        logger.info(f"Crawling data for specialty: {specialty_value} letter: {search_letter}")
        page_content = self.fetch_licensee_data(specialty_value, search_letter)

        license_numbers = set()
        if page_content:
            results, pages_counts = self.parse_listing_page(page_content)
            logger.info(f"Found Rows: {len(results)}")
            if results:
                for _ in results:
                    license_numbers.add(_)

            # Covering Pagination
            for next_page_num in range(1, pages_counts):
                page_content = self.fetch_licensee_data(specialty_value, search_letter, str(next_page_num))
                results, _ = self.parse_listing_page(page_content, next_page_num)
                logger.info(f"Found More Rows: {len(results)}")
                if results:
                    for _ in results:
                        license_numbers.add(_)

                logger.info(f"Total Found Rows Till Now: {len(license_numbers)}")

        else:
            logger.info(f"Page content not found: {search_letter}")

        logger.info(f"Total Found Rows: {len(license_numbers)}")

        # use threading to process each id
        if license_numbers:
            license_numbers = list(license_numbers)
            self.extract_data(license_numbers, specialty_value, search_letter)
        return True

    def run(self):
        """Run the crawler concurrently for all search filters."""

        # Write CSV headers
        with open(self.output_file, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ["Full_Name", "License_Type", "License_Number", "Status", "Professional", "Issued", "Expired"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

        # Process each specialty concurrently
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = {
                executor.submit(self.crawl_specialty, search_letter, specialty_value): (
                    specialty_value, search_letter) for specialty_value, search_letter in self.search_filters
            }

            # Wait for all futures to complete
            for future in as_completed(futures):
                specialty_value, search_letter = futures[future]
                try:
                    # Optionally, you can get the result if needed
                    _ = future.result()  # This will raise an exception if the task failed
                except Exception as e:
                    print(f"Task generated an exception: {e} | {search_letter}")

        logger.info("Removing Duplicates if any")
        self.remove_duplicates_in_csv()


if __name__ == '__main__':
    crawler = LicenseCrawler()
    crawler.run()
