import csv
import logging
import os
import re
import time
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

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
        self.base_url = 'https://web1.ky.gov/GenSearch/LicenseSearch.aspx'
        self.list_url = 'https://web1.ky.gov/GenSearch/LicenseList.aspx'
        self.output_file = 'results/results.csv'

        self.csv_lock = Lock()
        self.headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'accept-language': 'en-US,en;q=0.6',

            'priority': 'u=0, i',
            'sec-ch-ua': '"Brave";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'none',
            'sec-fetch-user': '?1',
            'sec-gpc': '1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        }

        os.makedirs('results', exist_ok=True)
        self.parser = "html.parser"

    def fetch_specialties(self):
        """Fetch initial page data including states"""

        params = {
            'AGY': '5',
        }

        response = requests.get(self.base_url, headers=self.headers, params=params, verify=False)
        if response.status_code != 200:
            logger.error(f"Failed to fetch states")
            return None
        soup = BeautifulSoup(response.text, self.parser)

        # Extract states options
        specialties_element = soup.find("select", attrs={
            "id": "usLicenseSearch_ddlField4"})
        specialties = [option["value"] for option in specialties_element.find_all("option") if
                       option["value"].strip() and option["value"].strip() != "0"]

        return specialties

    def fetch_licensee_page(self, specialty):
        """Submit the form for a given specialty and fetch the results page."""
        params = {
            'AGY': '5',
            'FLD1': '',
            'FLD2': '',
            'FLD3': "0",
            'FLD4': specialty,
            'TYPE': '',
        }
        retries = 3
        while retries:
            retries -= 1
        try:
            response = requests.get(self.list_url, params=params,
                                    verify=False)

            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            logger.error(f"Retrying __ Failed to fetch data for Specialty {specialty}: {e}")
            time.sleep(1)
        return None

    def replace_multiple_whitespace(self, text):
        """ Replace consecutive spaces in full name if any"""
        # Replace multiple whitespaces with a single space
        return re.sub(r'\s+', ' ', text).strip()

    def parse_licensee_data(self, html_content):
        """Parse the licensee data from the HTML content."""
        if not html_content:
            return None
        soup = BeautifulSoup(html_content, 'html.parser')
        # Extract Registration Information

        extracted_data = {
            "Full_Name": None,
            "License_Type": None,
            "License_Number": None,
            "Status": None,
            "Professional": None,
            "Issued": None,
            "Expired": None,
        }
        active_rows = 0
        data_rows = []
        main_content = soup.find("div", class_="ky-content-main min")
        # Parse the fields
        if main_content:
            data_rows = main_content.find_all("div", class_="row")

        record_flag = "Board Action:"
        for index, data_row in enumerate(data_rows, start=1):
            cells = data_row.find_all("div")
            if len(cells) >= 2:
                key_1 = cells[0].get_text(strip=True)
                value_1 = cells[1].get_text(strip=True)

                # # Map keys to the extracted_data fields
                if "Name:" == key_1:
                    full_name = self.replace_multiple_whitespace(value_1)
                    license_type = None
                    # Update the License_Type based on the Full_Name suffix
                    if full_name.endswith(" D.O."):
                        license_type = "DO"
                    elif full_name.endswith(" M.D."):
                        license_type = "MD"
                    if license_type:
                        full_name = full_name[:-5].strip()  # Remove ", MD" and any trailing spaces
                    extracted_data["Full_Name"] = full_name
                    extracted_data["License_Type"] = license_type

                elif "License:" == key_1:
                    extracted_data["License_Number"] = value_1
                elif "Status:" == key_1:
                    extracted_data["Status"] = value_1
                elif "Year Licensed in KY:" == key_1:
                    extracted_data["Issued"] = value_1
                elif "Expiration:" == key_1:
                    extracted_data["Expired"] = value_1
                elif "*Area of Practice:" == key_1:
                    extracted_data["Professional"] = value_1

                if record_flag == key_1:
                    if "Active" in extracted_data["Status"]:
                        self.save_to_csv([extracted_data])
                        active_rows += 1
                    extracted_data = {
                        "Full_Name": None,
                        "License_Type": None,
                        "License_Number": None,
                        "Status": None,
                        "Professional": None,
                        "Issued": None,
                        "Expired": None,
                    }

        return active_rows

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

    def save_to_csv(self, results):
        """Save results to CSV in a thread-safe manner."""
        with self.csv_lock:
            with open(self.output_file, 'a', newline='', encoding='utf-8') as csvfile:
                fieldnames = ["Full_Name", "License_Type", "License_Number", "Status", "Professional", "Issued",
                              "Expired"]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                for result in results:
                    writer.writerow(result)

    def crawl_specialty(self, index, specialty, total):
        """Crawl and process licensee data for a given specialty."""
        logger.info(f"Crawling data for Specialty: {specialty} #({index})/{total}")

        page_content = self.fetch_licensee_page(specialty)

        if page_content:
            result_rows = self.parse_licensee_data(page_content)
            logger.info(f"Active Rows found: {result_rows} for Specialty: {specialty}")

        else:
            logger.info(f"Page content not found for Specialty: {specialty}")

        return True

    def run(self):
        """Run the crawler concurrently for all specialties."""

        specialties = self.fetch_specialties()
        if not specialties:
            logger.error(f"Could not establish a scraper.")
            return

        total_specialties = len(specialties)
        logger.info(f"Found specialties: {total_specialties}")

        # Write CSV headers
        with open(self.output_file, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ["Full_Name", "License_Type", "License_Number", "Status", "Professional", "Issued", "Expired"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

        # Process each specialty concurrently
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = {
                executor.submit(self.crawl_specialty, index, specialty, total_specialties): (
                    index, specialty) for index, specialty in enumerate(specialties, start=1)
            }

            # Wait for all futures to complete
            for future in as_completed(futures):

                try:
                    # Optionally, you can get the result if needed
                    _ = future.result()  # This will raise an exception if the task failed
                except Exception as e:
                    index, specialty = futures[future]
                    logger.error(f"Task generated an exception: {e} | {specialty} #{index}")

                logger.info(f"Crawled: {specialty} #{index}")
            logger.info(f"Completed")
            logger.info("Removing duplicates if any.")
            self.remove_duplicates_in_csv()


if __name__ == '__main__':
    crawler = LicenseCrawler()
    crawler.run()
