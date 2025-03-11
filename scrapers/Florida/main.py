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
        self.base_url = self.detail_url = 'https://mqa-internet.doh.state.fl.us/MQASearchServices/HealthCareProviders'
        self.export_url = 'https://mqa-internet.doh.state.fl.us/MQASearchServices/HealthCareProviders/ExportToCsvLVP'
        self.output_file = 'results/results.csv'
        self.csv_lock = Lock()

        self.max_workers = 4
        self.headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive',
            'Referer': 'https://mqa-internet.doh.state.fl.us/MQASearchServices/HealthCareProviders',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
        }

        self.target_types = ["Osteopathic Physician", "Medical Doctor"]
        os.makedirs('results', exist_ok=True)

    def fetch_specialities(self):
        """Fetch initial page data"""
        response = requests.get(self.base_url, headers=self.headers)
        if response.status_code != 200:
            logger.error(f"Failed to fetch specialities: {response.status_code}")
            return None
        soup = BeautifulSoup(response.text, "html.parser")

        # Extract specialty options
        speciality_element = soup.find("select", attrs={"id": "ProfessionDD"})
        specialties = [(x.text.strip(), x["value"],) for x in speciality_element.find_all("option") if
                       x["value"].strip() and x.text.strip() in self.target_types]

        specialties.sort(key=lambda x: x[1], reverse=True)

        return specialties

    def fetch_licensee_data(self, specialty_value, specialty_name):
        """Submit the form for a given specialty and fetch the results page."""
        params = {
            'jsonModel': (
                             '{"Id":0,"Board":null,"Profession":"%s","SpecialtyOrCertification":null,'
                             '"OtherSpecialtyOrCertification":null,"LicenseNumber":null,"FirstName":null,'
                             '"LastName":null,"BusinessName":null,"City":null,"County":"","ZipCode":null,'
                             '"LicenseStatus":"ALL","IsAuthorizedToOrderCannabis":null}'
                         ) % specialty_value
        }

        logger.info(f"Fetching listings for speciality  {specialty_name}")

        try:
            response = requests.get(self.export_url
                                    , headers=self.headers, params=params,

                                    )

            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch data for specialty {specialty_value}: {e}")
            return None

    def get_dd_by_dt_text(self, soup, dt_text):

        dt = soup.find("dt", string=lambda text: text and dt_text == text.strip())

        if dt:
            if dt_text == "License":
                dd = dt.find_next("dd")
                next_dt = dt.find_next("dt")
                next_dd = next_dt.find_next("dd")
                if dd and next_dd:
                    return dd.text.strip(), next_dd.text.strip()

            dd = dt.find_next("dd")

            if dd:
                return dd.text.strip()
        return None

    def parse_licensee_data(self, html_content):
        """Parse the licensee data from the HTML content."""
        with open("a.html", "w", encoding="utf-8") as f:
            f.write(html_content)
        detail_response_soup = BeautifulSoup(html_content, 'html.parser')
        # Extract fields
        fields = {
            "Full_Name": None,
            "Professional": None,
            "License_Type": None,
            "License_Number": None,
            "Status": None,
            "Issued": None,
            "Expired": None,
        }

        # Extract data
        fields["Full_Name"] = detail_response_soup.find("h3").text.strip()
        fields["Professional"] = self.get_dd_by_dt_text(detail_response_soup, "Profession")
        fields["License_Number"], fields["Status"] = self.get_dd_by_dt_text(detail_response_soup, "License")
        fields["Issued"] = self.get_dd_by_dt_text(detail_response_soup, "License Original Issue Date")
        fields["Expired"] = self.get_dd_by_dt_text(detail_response_soup, "License Expiration Date")

        # Assuming "License_Type" is inferred from the "Profession"
        fields["License_Type"] = "MD" if fields["Professional"] == "Medical Doctor" else "DO"

        return fields

    def save_to_csv(self, results):
        """Save results to CSV in a thread-safe manner."""
        with self.csv_lock:
            with open(self.output_file, 'a', newline='', encoding='utf-8') as csvfile:
                fieldnames = ["Full_Name", "License_Type", "License_Number", "Status", "Professional", "Issued",
                              "Expired"]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                for result in results:
                    writer.writerow(result)

    def parse_listing_page(self, content):
        data = pd.read_csv(io.StringIO(content), encoding="utf-8")
        return list(list(data["License"]))

    def fetch_detail_page(self, detail_page_id, specialty_name, specialty_value, index, retries=5):
        """Submit the form for a given details page."""

        data = {
            # '__RequestVerificationToken': 'XfBfE0jFX24FR6svytzvqfu-xdZPAKfhEFwZ-EdplmvjLfNScNAYkF-i2WFc1aJfsRKbIlQs1PcO15YX-_GRCll9PkbiXuvg61_gF-IWtxc1',
            'SearchDto.Board': '',
            'SearchDto.Profession': '',
            'SearchDto.LicenseNumber': detail_page_id,
            'SearchDto.BusinessName': '',
            'SearchDto.LastName': '',
            'SearchDto.FirstName': '',
            'SearchDto.City': '',
            'SearchDto.County': '',
            'SearchDto.ZipCode': '',
            'SearchDto.LicenseStatus': 'ALL',
        }
        while retries:
            retries -= 1

            if index % 500 == 0:
                logger.info(f"Fetching details {index} for speciality {specialty_name} detail: {detail_page_id}")
            try:

                response = requests.post(self.base_url, headers=self.headers, data=data

                                         )
                response.raise_for_status()
                return response.text
            except requests.exceptions.RequestException as e:
                logger.error(
                    f"Retrying __ Failed to fetch data for detail page {index} for speciality {specialty_name} detail: {detail_page_id} : {e}")
                time.sleep(1)
        return None

    # A function for processing each results_id.
    def process_id(self, index, results_id, specialty_name, specialty_value):
        return self.parse_licensee_data(
            self.fetch_detail_page(results_id, specialty_name, specialty_value, index)
        )

    def extract_data(self, results_ids, specialty_name, specialty_value, start):

        # Start threading with ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks and store the futures.
            futures = {
                executor.submit(self.process_id, start + i, results_id, specialty_name, specialty_value): results_id
                for i, results_id in enumerate(results_ids)}

            # As each task completes, collect its result.
            for future in as_completed(futures):
                row = future.result()
                if row:
                    self.save_to_csv([row])

    def crawl_specialty(self, specialty, index):
        specialty_name, specialty_value = specialty
        """Crawl and process licensee data for a given specialty."""
        logger.info(f"Crawling data for specialty#{index}: {specialty_name}")

        page_content = self.fetch_licensee_data(specialty_value, specialty_name)
        if page_content:
            results = self.parse_listing_page(page_content)
            # In threading
            self.extract_data(results, specialty_name, specialty_value, 0)

        else:
            logger.info(f"Page content not found: {specialty_name}")

    def run(self):
        """Run the crawler concurrently for all specialties."""

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
                executor.submit(self.crawl_specialty, specialty, index): specialty for index, specialty in
                enumerate(specialties, start=1)
            }

            # Wait for all futures to complete
            for future in as_completed(futures):
                specialty = futures[future]
                try:
                    # Optionally, you can get the result if needed
                    result = future.result()  # This will raise an exception if the task failed
                except Exception as e:
                    print(f"Task generated an exception: {e} | {specialty}")


if __name__ == '__main__':
    crawler = LicenseCrawler()
    crawler.run()
