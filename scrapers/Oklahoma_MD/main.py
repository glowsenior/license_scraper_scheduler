import csv
import logging
import os
import re
import time
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

import urllib3
from twocaptcha import TwoCaptcha

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
        self.base_url = 'https://www.okmedicalboard.org/search'

        self.output_file = 'results/results.csv'
        self.csv_lock = Lock()
        self.proxy_url = "http://ptm24webscraping:ptmxx248Dime_country-us@us.proxy.iproyal.com:12323"
        self.proxy = {"http": self.proxy_url,
                      "https": self.proxy_url}
        self.headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'accept-language': 'en-US,en;q=0.6',
            'cache-control': 'max-age=0',
            'content-type': 'application/x-www-form-urlencoded',
            'origin': 'https://www.okmedicalboard.org',
            'priority': 'u=0, i',
            'referer': 'https://www.okmedicalboard.org/search',
            'sec-ch-ua': '"Brave";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-user': '?1',
            'sec-gpc': '1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        }

        self.max_record_count = 5
        self.target_types = ["MD"]

        os.makedirs('results', exist_ok=True)



    def fetch_licensee_data(self, specialty_index, specialty_name):
        """Submit the form for a given specialty and fetch the results page."""
        data = {
            'licensenbr': '',
            'lictype': specialty_name,
            'lname': '',
            'fname': '',
            'practcounty': '',
            'status': 'ACTIVE',
            'discipline': '',
            'hosp_county': '',
            'hosp_code': '',
            'accepting_patients': '',
            'accepting_medicaid': '',
            'accepting_medicare': '',
            'language': '',
            'licensedat_range': '',
            'order': 'lname',
            'show_details': 'Show Detailed list',
        }

        retries = 3
        while retries:
            retries -=1
            logger.info(
                f"Fetching listing page=0 for speciality  {specialty_name}")

            try:
                response = requests.post(self.base_url, headers=self.headers, data=data,proxies=self.proxy,
                                         verify=False)

                response.raise_for_status()
                return response.text
            except requests.exceptions.RequestException as e:
                logger.error(f"Retrying __ Failed to fetch data for specialty {specialty_name}: {e}")
                time.sleep(1)
            return None

    def fetch_next_page_licensee_data(self, specialty_index, specialty_name,current_page):
        """Submit the form for a given specialty and fetch the results page."""
        data = {
            'lictype': specialty_name,
            'status': 'ACTIVE',
            'show_details': '1',
            'current_page': f'{current_page}',
            'order': 'lname',
            'next_page': 'Next >>',
        }
        retries=3
        while retries:
            retries -= 1
            logger.info(
                f"Fetching listing page={current_page} for speciality  {specialty_name}")

            try:
                response = requests.post(self.base_url, headers=self.headers, data=data,proxies=self.proxy,
                                         verify=False)

                response.raise_for_status()
                return response.text
            except requests.exceptions.RequestException as e:
                logger.error(f"Retrying __ Failed to fetch data for specialty {specialty_name}: {e}")
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

    def parse_listing_page(self, html_content, first_page=False):
        """Extracting details from the html page"""
        total_records_count = 0
        soup = BeautifulSoup(html_content, "html.parser")
        # Extract tables with the class "licensee-info"
        tables = soup.find_all("table", class_="licensee-info")
        if not tables:
            return [], total_records_count

        if first_page:
            # Extract the total record count
            p_tag = soup.find("p", string=lambda text: text and text.startswith("Displaying"))
            if p_tag:
                record_count_text = p_tag.text
                # Use regex to extract the total records count (e.g., 14834 from "Displaying 1 to 5 out of 14834 results.")
                match = re.search(r"out of (\d+) results", record_count_text)
                if match:
                    total_records_count = int(match.group(1))
                    logger.info(f"Total Records: {total_records_count}")
                else:
                    logger.info("Total record count not found in the text.")
            else:
                logger.info("Record count not found.")



        # Parse each table and extract the required fields
        results = []
        for table in tables:
            # Extract full name
            try:
                full_name = next(table.find("th", colspan="4").stripped_strings, "").split("\n")[0]

                # Extract details from the inner tables
                rows = table.find_all("tr")
                license_type = license_number = issued = expired = status = professional = None

                for row in rows:
                    th = row.find("th")
                    td = row.find("td")
                    if not th or not td:
                        continue

                    header = th.get_text(strip=True)
                    value = td

                    if header == "License:":
                        license_number = value.get_text(strip=True)
                    elif header == "Dated:":
                        issued = value.get_text(strip=True)
                    elif header == "Expires:":
                        expired = value.get_text(strip=True)
                    elif header == "Status:":
                        status = value.get_text(strip=True)
                    elif header == "License Type:":
                        license_type = value.get_text(strip=True)
                    elif header == "Specialty:":
                        professional = value.get_text(strip=True, separator="\n")

                # Append the extracted data to the results list
                results.append({
                    "Full_Name": full_name,
                    "License_Type": license_type,
                    "License_Number": license_number,
                    "Issued": issued,
                    "Expired": expired,
                    "Status": status,
                    "Professional": professional
                })
            except Exception as parsing_exp:
                logger.error(f"Parsing Exception: {parsing_exp}")


        return results, total_records_count



    def crawl_specialty(self, specialty_index, specialty_name):
        """Crawl and process licensee data for a given specialty."""
        logger.info(f"Crawling data for specialty: {specialty_name}")

        page_content = self.fetch_licensee_data(specialty_index, specialty_name)
        if page_content:

            results, total_records_count = self.parse_listing_page(page_content,True)
            logger.info(f"Found records: {len(results)}")
            if results:
                self.save_to_csv(results)

            if total_records_count:
                total_pages = total_records_count//self.max_record_count
                logger.info(f"Pages to crawl: {total_pages+1}")
                for page_num in range(1,total_pages+1):
                    page_content = self.fetch_next_page_licensee_data(specialty_index,specialty_name,page_num)
                    results, _ = self.parse_listing_page(page_content)
                    logger.info(f"Found More Records: {len(results)}")
                    if results:
                        self.save_to_csv(results)

                    # for testing
                    # if page_num >3:
                    #     break

        else:
            logger.info(f"Page content not found: {specialty_name}")

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
                executor.submit(self.crawl_specialty, specialty_index, specialty_name): (
                    specialty_index, specialty_name) for specialty_index, specialty_name in enumerate(self.target_types,start=1)
            }

            # Wait for all futures to complete
            for future in as_completed(futures):

                try:
                    # Optionally, you can get the result if needed
                    _ = future.result()  # This will raise an exception if the task failed
                except Exception as e:
                    _, specialty_name = futures[future]
                    logger.error(f"Task generated an exception: {e} | {specialty_name}")
            logger.info("Completed")


if __name__ == '__main__':
    crawler = LicenseCrawler()
    crawler.run()
