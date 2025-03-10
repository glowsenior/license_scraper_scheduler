import csv
import itertools
import logging
import os
import re
import time
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

import urllib3
from bs4 import BeautifulSoup

# Suppress specific warnings
warnings.filterwarnings('ignore', category=urllib3.exceptions.InsecureRequestWarning)

import requests

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class LicenseCrawler:

    def __init__(self):

        """Initialize the crawler."""
        self.base_url = 'http://cgi.docboard.org/cgi-shl/nhayer.exe'

        self.output_file = 'results/results.csv'
        self.csv_lock = Lock()
        self.proxy_url = "http://ptm24webscraping:ptmxx248Dime_country-us@us.proxy.iproyal.com:12323"
        self.proxy = {"http": self.proxy_url,
                      "https": self.proxy_url}
        self.headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Origin': 'http://docfinder.docboard.org',
            'Referer': 'http://docfinder.docboard.org/',
            'Sec-GPC': '1',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        }
        self.max_workers = 2
        self.parser = 'html.parser'
        os.makedirs('results', exist_ok=True)
        self.target_searches = [''.join(comb) for comb in itertools.product('ABCDEFGHIJKLMNOPQRSTUVWXYZ', repeat=2)]

        self.target_types = ["Medical Doctor", "Physician Assistant", "Osteopathic Physician"]

    def fetch_licensee_data(self, target_search_index, target_search, retries=3):
        """Submit the form for a given license status and fetch the results page."""

        data = {
            'form_id': 'medname',
            'state': 'nm',
            'medlname': target_search,
            'medfname': '',
        }

        logger.info(f"Fetching listing page for search#{target_search_index} {target_search}")

        while retries:
            retries -= 1
            try:
                response = requests.post(self.base_url, headers=self.headers, data=data, proxies=self.proxy,
                                         verify=False)

                response.raise_for_status()
                return response.text
            except requests.exceptions.RequestException as e:
                logger.error(f"Retrying __ Failed to fetch data for search: {target_search}: {e}")
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

    def extract_records_ids(self, html_content):
        """Extract the ids from the JSON content of the response"""
        soup = BeautifulSoup(html_content, self.parser)

        results = set()
        mednumb_element = soup.find("select", attrs={"name": "mednumb"})
        records = mednumb_element.find_all("option") if mednumb_element else []
        for record in records:
            results.add(record["value"].strip())
        return list(results)

    def replace_multiple_whitespace(self, text):
        """ Replace consecutive spaces in full name if any"""

        # Replace multiple whitespaces with a single space
        return re.sub(r'\s+', ' ', text).strip()

    def parse_licensee_data_and_save(self, html_content):
        """Parse the licensee detail page and save data"""
        soup = BeautifulSoup(html_content, "html.parser")
        extracted_data = {
            "Full_Name": None,
            "License_Type": None,
            "License_Number": None,
            "Status": None,
            "Professional": None,
            "Issued": None,
            "Expired": None,
        }

        # Parse the fields
        rows = soup.find_all("tr")
        for row in rows:
            cells = row.find_all("td")
            if len(cells) >= 4:
                key_1 = cells[0].get_text(strip=True)
                value_1 = cells[1].get_text(strip=True)
                key_2 = cells[2].get_text(strip=True)
                value_2 = cells[3].get_text(strip=True)

                # Map keys to the extracted_data fields
                if "Licensee" in key_1:
                    extracted_data["Full_Name"] = self.replace_multiple_whitespace(value_1)
                if "License Type" in key_2:
                    extracted_data["License_Type"] = value_2
                if "License Number" in key_2:
                    extracted_data["License_Number"] = value_2
                if "License Status" in key_2:
                    extracted_data["Status"] = value_2
                if "License Date" in key_2:
                    extracted_data["Issued"] = value_2
                if "License Expires" in key_2:
                    extracted_data["Expired"] = value_2
                if "*Specialty" in key_1:
                    extracted_data["Professional"] = value_1

        if extracted_data and extracted_data["License_Number"]:
            self.save_to_csv([extracted_data])

        return True

    def fetch_detail_page(self, detail_page_id, total_recs, target_search_index, target_search, index, retries=5):
        """Submit the form for a given details page."""

        data = {
            'form_id': 'medname',
            'state': 'NM',
            'mednumb': detail_page_id,
            'lictype': '??',
            'medlname': target_search,
            'medfname': '',
        }

        while retries:
            retries -= 1

            logger.info(
                f"Fetching details {index}/({total_recs}) for search {target_search} detail: {detail_page_id.strip()}")
            try:

                response = requests.post(self.base_url, headers=self.headers, data=data, proxies=self.proxy,
                                         verify=False)
                response.raise_for_status()
                return response.text
            except requests.exceptions.RequestException as e:
                logger.error(
                    f"Retrying __ Failed to fetch data for detail page {index} for search {target_search} detail: {detail_page_id} : {e}")
                time.sleep(1)

        return None

    # A function for processing each results_id.
    def process_id(self, index, total_recs, results_id, target_search_index, target_search):
        "Process each detail page"
        self.parse_licensee_data_and_save(
            self.fetch_detail_page(results_id, total_recs, target_search_index, target_search, index))
        return True

    def extract_data(self, results_ids, target_search_index, target_search, total_recs, start):
        "Extract data from detail pages in parallel"
        # Start threading with ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks and store the futures.
            futures = {
                executor.submit(self.process_id, start + i, total_recs, results_id, target_search_index,
                                target_search): results_id
                for i, results_id in enumerate(results_ids)}

            # As each task completes, wait for completion
            for future in as_completed(futures):
                pass

    def crawl_target_search(self, target_search_index, target_search):
        """Crawl and process licensee data for a given license status."""

        logger.info(f"Crawling data for search: {target_search}")
        page_content = self.fetch_licensee_data(target_search_index, target_search)

        if page_content:
            records_ids = self.extract_records_ids(page_content)
            total_recs = len(records_ids)
            logger.info(f"Fetching each record for total: {total_recs}")
            # Extracting details data in threading
            self.extract_data(records_ids, target_search_index, target_search, total_recs, start=1)
        else:
            logger.info(f"Page content not found: {target_search}")
        return True

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
                executor.submit(self.crawl_target_search, target_search_index, target_search): (
                    target_search_index, target_search) for target_search_index, target_search in
                enumerate(self.target_searches, start=1)
            }

            # Wait for all futures to complete
            for future in as_completed(futures):

                try:
                    # Optionally, you can get the result if needed
                    _ = future.result()  # This will raise an exception if the task failed
                except Exception as e:
                    _, target_search = futures[future]
                    logger.info(f"Task generated an exception: {e} | {target_search}")
        logger.info("Completed")


if __name__ == '__main__':
    crawler = LicenseCrawler()
    crawler.run()
