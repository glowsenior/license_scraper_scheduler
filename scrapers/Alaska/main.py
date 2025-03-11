import csv
import io
import logging
import os
import time
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

import pandas as pd
import urllib3

# Suppress specific warnings
warnings.filterwarnings('ignore', category=urllib3.exceptions.InsecureRequestWarning)

import requests
from bs4 import BeautifulSoup

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class LicenseCrawler:

    def __init__(self):

        """Initialize the crawler."""
        self.base_url = 'https://www.commerce.alaska.gov/cbp/DBDownloads/ProfessionalLicenseDownload.CSV'
        self.output_file = 'results/results.csv'
        self.csv_lock = Lock()

        self.headers = {
            'Referer': 'https://www.commerce.alaska.gov/cbp/main/',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36',
            'sec-ch-ua': '"Not A(Brand";v="8", "Chromium";v="132", "Brave";v="132"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
        }
        self.target_types = [("Medical",["Osteopathic Physician", "Physician"],)]
        os.makedirs('results', exist_ok=True)


    def fetch_licensee_data(self):
        """Submit the form for a given specialty and fetch the results page."""
        logger.info(f"Fetching listings")

        try:
            response = requests.get(self.base_url, headers=self.headers)

            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch data: {e}")
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

    def get_license_type_from_professional_types(self,prof_type):
        """Returns the license type based on the professional type."""
        if prof_type == "Physician":
            return "MD"
        elif prof_type == "Osteopathic Physician":
            return "DO"
        else:
            return None

    def parse_listing_page(self, content,professional_name, professional_types):
        """Parse the CSV and extract the target results"""

        results = []
        logger.info("Reading CSV")
        df = pd.read_csv(io.StringIO(content), encoding="utf-8", low_memory=False)

        logger.info(f"Filtering Records: {professional_types}")
        filtered_df = df[
            (df["Program"] == professional_name) &
            (df["Status"] == "Active") &
            (df["ProfType"].isin(professional_types))
            ].copy()
        df = None

        filtered_df["LicenseType"] = filtered_df["ProfType"].apply(self.get_license_type_from_professional_types)

        logger.info("Collecting Records")
        for _, row in filtered_df.iterrows():
            fields = {
                "Full_Name": row.get("Owners", ""),
                "Professional": row.get("Program", ""),
                "License_Type": row.get("LicenseType", ""),
                "License_Number": row.get("LicenseNum", ""),
                "Status": "Active",
                "Issued": row.get("DateIssued", ""),
                "Expired": row.get("DateExpired", ""),
            }
            results.append(fields)

        return results


    def crawl_specialty(self, specialty, index):
        professional_name, professional_types = specialty
        """Crawl and process licensee data for a given search filter."""
        logger.info(f"Crawling data for professional#{index}: {professional_name}")

        page_content = self.fetch_licensee_data()

        if page_content:
            logger.info("Extracting Records")
            results = self.parse_listing_page(page_content,professional_name, professional_types)
            logger.info(f"Records found: {len(results)}.")
            if results:
                self.save_to_csv(results)
            return True

        else:
            logger.info(f"Page content not found: {professional_name}")
            return False


    def run(self):
        """Run the crawler concurrently for all specialties."""

        # Write CSV headers
        with open(self.output_file, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ["Full_Name", "License_Type", "License_Number", "Status", "Professional", "Issued", "Expired"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

        # Process each specialty concurrently
        with ThreadPoolExecutor(max_workers=1) as executor:
            futures = {
                executor.submit(self.crawl_specialty, specialty, index): specialty for index, specialty in
                enumerate(self.target_types, start=1)
            }

            # Wait for all futures to complete
            for future in as_completed(futures):
                specialty = futures[future]
                try:
                    # Optionally, you can get the result if needed
                    _ = future.result()  # This will raise an exception if the task failed
                except Exception as e:
                    print(f"Task generated an exception: {e} | {specialty}")


if __name__ == '__main__':
    crawler = LicenseCrawler()
    crawler.run()
