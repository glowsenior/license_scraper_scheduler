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
        self.base_url = 'https://ws.lasbme.org/api/TypeValues/RefTablesGetAll/null?tableKey=licensetype'
        self.detail_url = 'https://ws.lasbme.org/api/Individual/IndividualVerifyLicenseLAMED/'
        self.output_file = 'results/results.csv'
        self.csv_lock = Lock()

        self.headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'en-US,en;q=0.9',
            'content-type': 'application/json;charset=UTF-8',
            'datatype': 'json',
            'origin': 'https://online.lasbme.org',
            'priority': 'u=1, i',
            'referer': 'https://online.lasbme.org/',
            'sec-ch-ua': '"Brave";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-site',
            'sec-gpc': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        }

        self.page_size = 1000
        self.target_types = ["PHYSICIAN & SURGEON - MD", "PHYSICIAN & SURGEON - DO"]
        os.makedirs('results', exist_ok=True)

    def fetch_specialities(self):
        """Fetch initial page data including cookies"""
        response = requests.get(self.base_url, headers=self.headers)
        if response.status_code != 200:
            logger.error(f"Failed to fetch specialities: {response.status_code}")
            return None

        output_data = response.json()

        # Extract specialty options
        specialty_element = output_data["ReferenceEntities"]
        specialties = {specialty_element_dict["LicenseTypeId"]: specialty_element_dict["LicenseTypeName"]
                       for specialty_element_dict in specialty_element
                       if specialty_element_dict["LicenseTypeName"] in self.target_types}
        return specialties

    def fetch_licensee_data(self, specialty_value, specialty_name):
        """Submit the form for a given specialty and fetch the results page."""

        json_data = {
            'SortType': 'LicenseNumber',
            'SortOrder': 'asc',
            'CurrentPage': 1,
            'TotalRecords': 0,
            'PageSize': self.page_size,
            'maxSize': 5,
            'From': 0,
            'To': 0,
            'Data': {
                'LicenseNumber': '',
                'LicenseTypeId': specialty_value,
                'LicenseStatusTypeId': '',
                'LicenseSpecialityTypeId': '',
                'FirstName': '',
                'LastName': '',
                'City': '',
                'StateCd': '',
                'Zip': '',
                'CountyId': '',
            },
        }

        logger.info(
            f"Fetching listing page=1 for speciality {specialty_name}")

        try:
            response = requests.post(self.detail_url, headers=self.headers, json=json_data)

            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch data for specialty {specialty_value}: {e}")
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

    def extract_license_type(self, str_license_type):
        return str_license_type.split(" - ")[-1]

    def parse_listing_page(self, json_content, current_page_num=1):
        total_pages = None
        data = json_content.get("PagerVM", {})
        if current_page_num == 1:
            total_records = int(data.get("TotalRecords", 0))
            logger.info(f"Found Records: {total_records}")
            total_pages = total_records // self.page_size + 1

        records = data.get("Records", {})
        results = []

        for record in records:
            # Loop through all <a> tags within <td> elements and filter by the href pattern
            extracted_data = {
                "Full_Name": f"{record.get('FirstName', '')} {record.get('MiddleName', '')} {record.get('LastName', '')}".strip(),
                "License_Type": self.extract_license_type(record.get("LicenseTypeName", "")),
                "License_Number": record.get("LicenseNumber", ""),
                "Status": record.get("LicenseStatusTypeName", ""),
                "Professional": record.get("LicenseTypeName", ""),
                "Issued": record.get("LicenseEffectiveDate", ""),
                "Expired": record.get("LicenseExpirationDate", "")
            }
            results.append(extracted_data)
        return results, total_pages

    def fetch_next_page(self, specialty_value, specialty_name,
                        page_num, retries=5):

        """Submit the form for a given specialty and fetch the results page."""

        logger.info(
            f"Fetching listing page={page_num} for speciality  {specialty_name}")
        json_data = {
            'SortType': 'LicenseNumber',
            'SortOrder': 'asc',
            'CurrentPage': page_num,
            'TotalRecords': 0,
            'PageSize': self.page_size,
            'maxSize': 5,
            'From': 0,
            'To': 0,
            'Data': {
                'LicenseNumber': '',
                'LicenseTypeId': specialty_value,
                'LicenseStatusTypeId': '',
                'LicenseSpecialityTypeId': '',
                'FirstName': '',
                'LastName': '',
                'City': '',
                'StateCd': '',
                'Zip': '',
                'CountyId': '',
            },
        }
        while retries:
            retries -= 1
            try:
                response = requests.post(self.detail_url, headers=self.headers, json=json_data)

                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as e:
                logger.error(f"Retrying __ Failed to fetch data for specialty {specialty_value}: {e}")
                time.sleep(1)
        return None

    def crawl_specialty(self, specialty_name, specialty_value):
        """Crawl and process licensee data for a given specialty."""
        logger.info(f"Crawling data for specialty: {specialty_name}")
        page_content = self.fetch_licensee_data(specialty_value, specialty_name)
        rows_count = 0
        if page_content:

            results, pages_counts = self.parse_listing_page(page_content)
            logger.info(f"Found Rows: {len(results)}")

            if results:
                self.save_to_csv(results)

            # Covering Pagination
            for next_page_num in range(2, pages_counts + 1):
                page_content = self.fetch_next_page(specialty_value, specialty_name, next_page_num)
                results, _ = self.parse_listing_page(page_content, next_page_num)

                logger.info(f"Found More Rows: {len(results)}")
                if results:
                    self.save_to_csv(results)
                rows_count += len(results)


        else:
            logger.info(f"Page content not found: {specialty_name}")

    def run(self):
        """Run the crawler concurrently for all specialties."""
        logger.info("Fetching specialities")
        specialties = self.fetch_specialities()
        if not specialties:
            logger.error(f"Could not establish a scraper.")
            return

        # Write CSV headers
        with open(self.output_file, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ["Full_Name", "License_Type", "License_Number", "Status", "Professional", "Issued", "Expired"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

        # Process each specialty concurrently
        with ThreadPoolExecutor(max_workers=1) as executor:
            futures = {
                executor.submit(self.crawl_specialty, specialty_name, specialty_value): (
                    specialty_value, specialty_name) for specialty_value, specialty_name in specialties.items()
            }

            # Wait for all futures to complete
            for future in as_completed(futures):
                specialty_value, specialty = futures[future]
                try:
                    # Optionally, you can get the result if needed
                    result = future.result()  # This will raise an exception if the task failed
                except Exception as e:
                    print(f"Task generated an exception: {e} | {specialty}")


if __name__ == '__main__':
    crawler = LicenseCrawler()
    crawler.run()
