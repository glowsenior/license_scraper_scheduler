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
        self.base_url = 'https://osboe.us.thentiacloud.net/rest/public/registrant/search/'
        self.detail_url = 'https://osboe.us.thentiacloud.net/rest/public/registrant/get/'

        self.output_file = 'results/results.csv'
        self.csv_lock = Lock()
        proxy_url = "http://cruexuku-US-rotate:c3h2jphwjv7y@p.webshare.io:80"
        self.proxies = {
            "http": proxy_url,
            "https": proxy_url
        }
        self.headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'en-US,en;q=0.7',
            'priority': 'u=1, i',
            'referer': 'https://osboe.us.thentiacloud.net/webs/osboe/register/',
            'sec-ch-ua': '"Brave";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'sec-gpc': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        }

        self.keyword = 'all'
        self.skip = '0'
        self.take = '7746'
        self.type = 'name'

        os.makedirs('results', exist_ok=True)

    def fetch_detail_page(self, licensee_id,
                          retries=3):
        """Submit the form for a given specialty and fetch the results page."""

        params = {

            'id': licensee_id

        }

        while retries:
            retries -= 1
            try:
                response = requests.get(self.detail_url, headers=self.headers, params=params,
                                        proxies=self.proxies)

                response.raise_for_status()
                return response.json()
            except Exception as e:
                logger.error(f"Retrying __ Failed to fetch licensee data: {e}")
                time.sleep(5)
        return None

    def fetch_licensee_data(self,
                            retries=3):
        """Submit the form for a given specialty and fetch the results page."""

        params = {

            'keyword': self.keyword,
            'skip': self.skip,
            'take': self.take,
            'type': self.type,

        }
        logger.info(f"Fetching listing page")

        while retries:
            retries -= 1
            try:
                response = requests.get(self.base_url, headers=self.headers, params=params,
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

    def extract_and_save_licensee_data(self, data):
        """Extract and save licensee data from JSON response to CSV"""

        row = {
            "Full_Name": None,
            "License_Type": None,
            "License_Number": None,
            "Issued": None,
            "Expired": None,
            "Status": None,
            "Professional": None,

        }

        first_name = data.get("firstName", "")
        last_name = data.get("lastName", "")
        middle_name = data.get("middleName", "")
        full_name = " ".join(
            name for name in [first_name, middle_name, last_name]
            if name not in (None, "null", "")
        )
        row["Full_Name"] = full_name
        row["License_Number"] = str(data.get("licenseNumber", ""))
        professional = data.get("licenseCategory", "")
        row["Professional"] = row["License_Type"] = professional
        row["Status"] = data.get("licenseStatus", "")
        row["Issued"] = data.get("initialLicenseDate", "")
        row["Expired"] = data.get("licenseExpirationDate", "")

        if row:
            self.save_to_csv([row])

    def crawl_licensee(self, licensee_id, index, total):
        """Crawl and process licensee data for a given ID."""

        logger.info(
            f"Crawling data for Licensee({index}/{total}) ID: {licensee_id}")

        page_content = self.fetch_detail_page(licensee_id)

        if page_content:
            self.extract_and_save_licensee_data(page_content)
        else:
            logger.info(f"Page content not found for ID: {licensee_id}")

    def extract_result_ids(self, json_content):
        """Parse JSON and extract licensee IDs"""

        result_ids = []
        results = json_content.get("result", [])
        for result in results:
            license_number = result.get("licenseNumber", "")
            if license_number:
                status = result.get("licenseStatus", "")
                if status == "Active":
                    id = result.get("id", "")
                    if id:
                        result_ids.append(id)

        return list(set(result_ids))

    def run(self):
        """Run the crawler concurrently for all specialties."""
        result_ids = []
        page_content = self.fetch_licensee_data()
        if page_content:
            result_ids = self.extract_result_ids(page_content)

        if not result_ids:
            logger.error(f"Could not establish a scraper.")
            return

        # Write CSV headers
        with open(self.output_file, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ["Full_Name", "License_Type", "License_Number", "Status", "Professional", "Issued", "Expired"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

        total_results = len(result_ids)
        logger.info(f"Total results found: {total_results}")

        # Process each specialty concurrently
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {
                executor.submit(self.crawl_licensee, licensee_id, index, total_results): (index, licensee_id)
                for index, licensee_id in enumerate(result_ids, start=1)
            }

            # Wait for all futures to complete
            for future in as_completed(futures):

                try:
                    # Optionally, you can get the result if needed
                    _ = future.result()  # This will raise an exception if the task failed
                except Exception as e:
                    _, licensee_id = futures[future]
                    logger.error(
                        f"Task generated an exception: {e} | ID: {licensee_id}")


if __name__ == '__main__':
    crawler = LicenseCrawler()
    crawler.run()
