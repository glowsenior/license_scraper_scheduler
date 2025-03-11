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
        self.base_url = 'https://nsbme.us.thentiacloud.net/rest/public/registrant/search/'
        self.detail_url = 'https://nsbme.us.thentiacloud.net/rest/public/registrant/get/'
        self.output_file = 'results/results.csv'
        self.csv_lock = Lock()
        self.proxy_url = "http://cruexuku-US-rotate:c3h2jphwjv7y@p.webshare.io:80"
        self.proxy = {"http": self.proxy_url,
                      "https": self.proxy_url}
        self.headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'en-US,en;q=0.9',
            'priority': 'u=1, i',
            'referer': 'https://nsbme.us.thentiacloud.net/webs/nsbme/register/',
            'sec-ch-ua': '"Brave";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'sec-gpc': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        }
        self.license_type = "MD"
        self.max_workers = 4
        self.target_status = ["Active"]
        os.makedirs('results', exist_ok=True)

    def fetch_licensee_data(self, target_status_index, target_status, take='20', retries=3):
        """Submit the form for a given license status and fetch the results page."""

        params = {
            'keyword': 'all',
            'skip': '0',
            'take': take,
            'license_status': target_status,

        }

        if take != "20":
            logger.info(f"Fetching record IDs for license status#{target_status_index} {target_status}")
        else:
            logger.info(f"Fetching listing page for license status#{target_status_index} {target_status}")

        while retries:
            retries -= 1
            try:
                response = requests.get(self.base_url, headers=self.headers, params=params,proxies=self.proxy)

                response.raise_for_status()

                return response.json()
            except requests.exceptions.RequestException as e:
                logger.error(f"Retrying __ Failed to fetch data for license status {target_status}: {e}")
                time.sleep(1)
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

    def extract_records_count(self, json_content):
        """Extract the number of records from the JSON content of the response"""
        return json_content.get("resultCount", 0)

    def extract_records_ids(self, json_content):
        """Extract the ids from the JSON content of the response"""
        results = set()
        records = json_content.get("result", [])
        for record in records:
            results.add(record.get("id", ""))
        return list(results)

    def parse_licensee_data_and_save(self, data, license_status):
        """Parse the licensee detail page and save data"""
        extracted_data = {
            "Full_Name": None,
            "License_Type": None,
            "License_Number": None,
            "Status": None,
            "Professional": None,
            "Issued": None,
            "Expired": None,
        }

        # Extract full name
        first_name = data.get('firstName', "")
        second_name = data.get('middleName', "")
        last_name = data.get('lastName', "")
        full_name = full_name = " ".join(filter(None, [first_name, second_name, last_name]))

        # Extract data
        extracted_data["Full_Name"] = full_name
        extracted_data["Professional"] = data.get('licenseCategory', "")
        extracted_data["License_Number"] = data.get('licenseNumber', "")
        extracted_data["Issued"] = data.get('initialLicenseDate', "")
        extracted_data["Expired"] = data.get('licenseExpirationDate', "")

        extracted_data["License_Type"] = self.license_type
        extracted_data["Status"] = license_status

        if extracted_data:
            self.save_to_csv([extracted_data])

    def fetch_detail_page(self, detail_page_id, total_recs, target_status_index, target_status, index, retries=5):
        """Submit the form for a given details page."""

        params = {
            'id': detail_page_id,
        }

        while retries:
            retries -= 1

            logger.info(
                f"Fetching details {index}/({total_recs}) for license status {target_status} detail: {detail_page_id}")
            try:

                response = requests.get(self.detail_url, headers=self.headers, params=params,proxies=self.proxy)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as e:
                logger.error(
                    f"Retrying __ Failed to fetch data for detail page {index} for license status {target_status} detail: {detail_page_id} : {e}")
                time.sleep(1)
        return None

    # A function for processing each results_id.
    def process_id(self, index, total_recs, results_id, target_status_index, target_status):
        "Process each detail page"
        self.parse_licensee_data_and_save(
            self.fetch_detail_page(results_id, total_recs, target_status_index, target_status, index), target_status)

    def extract_data(self, results_ids, target_status_index, target_status, total_recs, start):
        "Extract data from detail pages in parallel"
        # Start threading with ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks and store the futures.
            futures = {
                executor.submit(self.process_id, start + i, total_recs, results_id, target_status_index,
                                target_status): results_id
                for i, results_id in enumerate(results_ids)}

            # As each task completes, wait for completion
            for future in as_completed(futures):
                pass

    def crawl_license_status(self, target_status_index, target_status):
        """Crawl and process licensee data for a given license status."""

        logger.info(f"Crawling data for license status: {target_status}")
        page_content = self.fetch_licensee_data(target_status_index, target_status)

        if page_content:
            total_records = self.extract_records_count(page_content)
            if total_records:
                page_content = self.fetch_licensee_data(target_status_index, target_status, take=str(total_records),
                                                        retries=3)
                records_ids = self.extract_records_ids(page_content)
                total_recs = len(records_ids)
                logger.info(f"Fetching each record for total: {total_recs}")
                # Extracting details data in threading
                self.extract_data(records_ids, target_status_index, target_status, total_recs, start=1)

            else:
                logger.info(f"No records found: {target_status}")



        else:
            logger.info(f"Page content not found: {target_status}")

    def run(self):
        """Run the crawler for all specialties."""

        # Write CSV headers
        with open(self.output_file, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ["Full_Name", "License_Type", "License_Number", "Status", "Professional", "Issued", "Expired"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

        # Process each specialty concurrently
        with ThreadPoolExecutor(max_workers=1) as executor:
            futures = {
                executor.submit(self.crawl_license_status, target_status_index, target_status): (
                    target_status_index, target_status) for target_status_index, target_status in
                enumerate(self.target_status, start=1)
            }

            # Wait for all futures to complete
            for future in as_completed(futures):
                target_status_index, target_status = futures[future]
                try:
                    # Optionally, you can get the result if needed
                    result = future.result()  # This will raise an exception if the task failed
                except Exception as e:
                    logger.info(f"Task generated an exception: {e} | {target_status}")
            logger.info("Completed")


if __name__ == '__main__':
    crawler = LicenseCrawler()
    crawler.run()
