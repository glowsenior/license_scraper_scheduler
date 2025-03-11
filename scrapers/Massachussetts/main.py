import csv
import itertools
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
        self.base_url = 'https://api.medboard.mass.gov/api-public/search'

        self.output_file = 'results/results.csv'
        self.csv_lock = Lock()
        proxy_url = "http://cruexuku-US-rotate:c3h2jphwjv7y@p.webshare.io:80"
        self.proxies = {
            "http": proxy_url,
            "https": proxy_url
        }
        self.headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'en-US,en;q=0.6',
            'content-type': 'application/json',
            'origin': 'https://findmydoctor.mass.gov',
            'priority': 'u=1, i',
            'referer': 'https://findmydoctor.mass.gov/',
            'sec-ch-ua': '"Brave";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-site',
            'sec-gpc': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        }

        self.first_name_values = [''.join(comb) for comb in itertools.product('ABCDEFGHIJKLMNOPQRSTUVWXYZ', repeat=2)]

        os.makedirs('results', exist_ok=True)

    def remove_duplicates_in_csv(self, field_name):
        """Remove duplicate rows in the CSV based on a specific field name."""
        with self.csv_lock:
            # Read existing data from the file
            try:
                with open(self.output_file, 'r', encoding='utf-8') as csvfile:
                    reader = csv.DictReader(csvfile)
                    existing_data = list(reader)
            except FileNotFoundError:
                existing_data = []

            duplicate_count = 0
            # Remove duplicates
            seen_values = set()
            unique_data = []
            for row in existing_data:
                value = row.get(field_name)
                if value not in seen_values:
                    seen_values.add(value)
                    unique_data.append(row)
                else:
                    duplicate_count += 1
            logger.info(f"Duplicates found: {duplicate_count}")

            # Write the unique data back to the CSV
            fieldnames = ["Full_Name", "License_Type", "License_Number", "Status", "Professional", "Issued", "Expired"]
            with open(self.output_file, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(unique_data)

    def fetch_licensee_data(self, first_name,
                            retries=3):
        """Submit the form for a given specialty and fetch the results page."""

        json_data = {
            'firstName': first_name,
            'lastName': '',
            'specialties': [],
            'cities': [],
            'searchType': 'BY_PHYSICIAN_NAME',
        }

        logger.info(f"Fetching listing page")

        while retries:
            retries -= 1
            try:
                response = requests.post(self.base_url, headers=self.headers, json=json_data,
                                         proxies=self.proxies)

                response.raise_for_status()

                return response.json()
            except Exception as e:
                logger.error(f"Retrying __ Failed to fetch listing data: {e}")
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

    def extract_license_type(self, base_type):
        """Check degree value to determine the type of license."""
        if not base_type:
            return ""
        if "M.D" in base_type:
            return "MD"
        elif "D.O" in base_type:
            return "DO"
        else:
            return base_type

    def extract_and_save_licensee_data(self, j_data):
        """Extract and save licensee data from JSON response to CSV"""
        results = j_data.get('results', {})
        data_list = results.get("data", [])

        logger.info(f"Found rows: {len(data_list)}")
        active_rows = []

        for data in data_list:

            row = {
                "Full_Name": None,
                "License_Type": None,
                "License_Number": None,
                "Issued": None,
                "Expired": None,
                "Status": None,
                "Professional": None
            }

            status = data.get("profileStatus", "")
            if status == "Active":
                l_id = str(data.get("licenseNumber", ""))
                if l_id:
                    row["Full_Name"] = data.get("fullName", "")
                    row["License_Number"] = l_id
                    row["Professional"] = data.get("specialties", "")
                    row["License_Type"] = self.extract_license_type(data.get("degree", ""))
                    row["Status"] = status
                    row["Issued"] = data.get("originalDate", "")
                    row["Expired"] = data.get("expirationDate", "")

                    if row:
                        active_rows.append(row)

        logger.info(f"Found Active Rows: {len(active_rows)}")
        if active_rows:
            self.save_to_csv(active_rows)

    def crawl_search_filter(self, first_name, index, total):
        """Crawl and process licensee data for a given ID."""

        logger.info(
            f"Crawling data for search filter({index}/{total}) First Name: {first_name}")

        page_content = self.fetch_licensee_data(first_name)

        if page_content:
            self.extract_and_save_licensee_data(page_content)
        else:
            logger.info(f"Page content not found for First Name: {first_name}")

    def run(self):
        """Run the crawler concurrently for all search filters."""

        # Write CSV headers
        with open(self.output_file, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ["Full_Name", "License_Type", "License_Number", "Status", "Professional", "Issued", "Expired"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

        total_search_filters = len(self.first_name_values)
        logger.info(f"Total search filters found: {total_search_filters}")


        # Process each specialty concurrently
        with ThreadPoolExecutor(max_workers=1) as executor:
            futures = {
                executor.submit(self.crawl_search_filter, first_name, index, total_search_filters): (index, first_name)
                for index, first_name in enumerate(self.first_name_values, start=1)
            }

            # Wait for all futures to complete
            for future in as_completed(futures):
                try:
                # Optionally, you can get the result if needed
                    _ = future.result()  # This will raise an exception if the task failed
                except Exception as e:
                    _, first_name = futures[future]
                    logger.error(
                        f"Task generated an exception: {e} | First Name: {first_name}")
            logger.info("Completed")
            logger.info("Removing duplicates if any.")
            self.remove_duplicates_in_csv("License_Number")

if __name__ == '__main__':
    crawler = LicenseCrawler()
    crawler.run()
