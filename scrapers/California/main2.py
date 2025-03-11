import csv
import logging
import os
import re
import time
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

import requests
import urllib3
from bs4 import BeautifulSoup

# Suppress specific warnings
warnings.filterwarnings('ignore', category=urllib3.exceptions.InsecureRequestWarning)
# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class LicenseCrawler:

    def __init__(self):

        """Initialize the crawler."""
        self.base_url = self.detail_url = 'https://search.dca.ca.gov/results'
        self.domain = 'https://search.dca.ca.gov'
        self.advance_search_url = "https://search.dca.ca.gov/advanced"
        self.output_file = 'results/results2.csv'
        self.csv_lock = Lock()

        self.max_workers = 2
        self.proxy_url = "http://cruexuku-US-rotate:c3h2jphwjv7y@p.webshare.io:80"
        self.proxy = {"http": self.proxy_url,
                      "https": self.proxy_url}

        self.headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.7',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Origin': 'https://search.dca.ca.gov',
            'Referer': 'https://search.dca.ca.gov/results',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
            'Sec-GPC': '1',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'sec-ch-ua': '"Brave";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
        }

        self.target_type = "296"  # Osteopathic Physicians and Surgeons
        self.payload = {
            'boardCode': '0',
            'licenseType': self.target_type,
            'licenseNumber': '',
            'busName': '',
            'firstName': '',
            'lastName': '',
            f'chk_licTyp_{self.target_type}': self.target_type,
            'chk_statusCode_1': '1',
        }
        os.makedirs('results', exist_ok=True)

    def fetch_cities(self):
        """Fetch listed cities"""

        response = requests.get(self.advance_search_url, headers=self.headers, proxies=self.proxy)
        if response.status_code != 200:
            logger.error(f"Failed to fetch cities: {response.status_code}")
            return None
        soup = BeautifulSoup(response.text, "html.parser")

        # Extract city options
        city_element = soup.find("select", attrs={"name": "advCity"})
        cities = [x.text.strip().upper() for x in city_element.find_all("option") if
                  x.text.strip()]
        cities = list(set(cities))
        cities = [("chk_city_" + x.replace(" ", "_"),x,) for x in cities]
        return cities

    def fetch_counties(self, city_name, city_value, retries=3):
        """Fetch listed counties against the city"""
        total_records = 0
        data = dict(self.payload.copy())
        data[city_name] = city_value
        status = None
        while retries:
            retries -= 1
            try:
                response = requests.post(self.base_url, headers=self.headers, data=data, proxies=self.proxy,
                                        timeout=30)
                status = response.status_code
                response.raise_for_status()

                soup = BeautifulSoup(response.text, "html.parser")

                # Extract county options
                county_element = soup.find("ul", attrs={"id": "countyFL"})
                counties = [(x["name"], x["value"]) for x in county_element.find_all("input") if
                            x["value"].strip()]

                h4_element = soup.find('h4')
                h4_text = h4_element.text.strip() if h4_element else ""
                # Use regex to extract the number from the text
                match = re.search(r'\b\d+\b', h4_text)  # Match any standalone number
                if match:
                    total_records = int(match.group())

                logger.info(f"{city_name}, {city_value}| {h4_text}")

                return counties, total_records

            except Exception as exp:
                logger.error(f"Retrying __ Failed to fetch counties: {status} : {exp}")
                time.sleep(2)
        logger.error(f"Failed to fetch counties: {status}")
        return None

    def fetch_licensee_data(self, index, city, county, retries=3):
        """Submit the form for a given city and county and fetch the results page."""

        data = dict(self.payload.copy())
        data[city[0]] = city[1]
        data[county[0]] = county[1]
        logger.info(f"Fetching listings for County#{index}: {city[1]} | County: {county[1]}")

        while retries:
            retries -= 1
            try:
                response = requests.post(self.base_url, headers=self.headers, data=data, proxies=self.proxy)
                response.raise_for_status()
                return response.text
            except requests.exceptions.RequestException as e:
                logger.error(f"Retrying __ Failed to fetch data for City: {city[1]} | County: {county[1]}: {e}")
                time.sleep(2)
            return None

    def replace_multiple_whitespace(self, text):
        # Replace multiple whitespaces with a single space
        return re.sub(r'\s+', ' ', text).strip()

    def parse_licensee_data(self, html_content):
        """Parse the licensee data from the HTML content."""

        soup = BeautifulSoup(html_content, 'html.parser')
        # initialize fields
        fields = {
            "Full_Name": None,
            "Professional": None,
            "License_Type": None,
            "License_Number": None,
            "Status": None,
            "Issued": None,
            "Expired": None,
        }

        # Extract target data

        detail_div = soup.find("div", class_="detailContainer")
        if not detail_div:
            return fields
        full_name = detail_div.find('p', id='name')
        fields['Full_Name'] = self.replace_multiple_whitespace(
            full_name.get_text().replace("Name:", "").strip()) if full_name else ''

        license_type = detail_div.find('p', id='licType')
        license_type = license_type.get_text().replace("License Type:", "").strip() if license_type else ''
        fields['License_Type'] = license_type
        fields['Professional'] = license_type

        license_status = detail_div.find('p', id='primaryStatus')
        fields['Status'] = license_status.get_text().replace("Primary Status:", "").strip() if license_status else ''

        license_number = soup.find('h2', id='licDetail')
        fields["License_Number"] = license_number.get_text().replace("Licensing details for:",
                                                                     "").strip() if license_type else ''

        issued = soup.find('p', id='issueDate')
        fields['Issued'] = issued.get_text().strip('"').strip() if issued else ''

        expired = soup.find('p', id='expDate')
        fields['Expired'] = expired.get_text().strip('"').strip() if expired else ''

        if fields:
            self.save_to_csv([fields])

    def save_to_csv(self, results):
        """Save results to CSV in a thread-safe manner."""
        with self.csv_lock:
            with open(self.output_file, 'a', newline='', encoding='utf-8') as csvfile:
                fieldnames = ["Full_Name", "License_Type", "License_Number", "Status", "Professional", "Issued",
                              "Expired"]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                for result in results:
                    writer.writerow(result)

    def parse_listing_page(self, html_content):
        """Extract detail page urls"""
        home_page_soup = BeautifulSoup(html_content, "html.parser")
        results_found = home_page_soup.find('header', class_="resultsHeader")
        if results_found:
            logger.info(results_found.text.strip().split("\n")[0])
        detail_page_urls = [self.domain + x["href"] for x in home_page_soup.find_all("a", class_="button newTab") if
                            x["href"]]

        return list(set(detail_page_urls))

    def fetch_detail_page(self, detail_page_url, city, county, index, retries=5):
        """Submit the form for a given details page."""

        while retries:
            retries -= 1
            logger.info(f"Fetching details {index} For City: {city[1]} | County: {county[1]}")
            try:

                response = requests.get(detail_page_url, headers=self.headers, proxies=self.proxy)
                response.raise_for_status()
                return response.text
            except requests.exceptions.RequestException as e:
                logger.error(
                    f"Retrying __ Failed to fetch data for detail page {index} for City: {city[1]} | County: {county[1]} : {e}")
                time.sleep(2)
        return None

    # A function for processing each results_id.
    def process_url(self, index, results_id, city, county):
        return self.parse_licensee_data(
            self.fetch_detail_page(results_id, city, county, index)
        )

    def extract_data(self, results_ids, city, county):

        # Start threading with ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks and store the futures.
            futures = {
                executor.submit(self.process_url, i, results_id, city, county): results_id
                for i, results_id in enumerate(results_ids, start=1)}

            # Wait for all tasks to complete
            for x in as_completed(futures):
                pass

    def crawl_city(self, city, index, total_cities):
        """Crawl and process licensee data for a given city."""

        city_name, city_value = city
        logger.info(f"Crawling data for city#{index}/({total_cities}): {city_value}")

        counties, total_records = self.fetch_counties(city_name, city_value)
        if total_records:
            for index, county in enumerate(counties, start=1):
                page_content = self.fetch_licensee_data(index, city, county)
                if page_content:
                    results = self.parse_listing_page(page_content)

                    logger.info(f"Found Urls: {len(results)}")
                    # In threading
                    self.extract_data(results, city, county)
                else:
                    logger.info(f"Page content not found: {city_value}")

    def run(self):
        """Run the crawler concurrently for all specialties."""
        logger.info("Fetching cities...")

        cities = self.fetch_cities()
        if not cities:
            logger.error(f"Could not establish a scraper.")
            return

        total_cities = len(cities)

        # Write CSV headers
        with open(self.output_file, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ["Full_Name", "License_Type", "License_Number", "Status", "Professional", "Issued", "Expired"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

        # Process each specialty concurrently
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = {
                executor.submit(self.crawl_city, city, index, total_cities): city for index, city in
                enumerate(cities, start=1)
            }

            # Wait for all futures to complete
            for future in as_completed(futures):
                city = futures[future]
                try:
                    # Optionally, you can get the result if needed
                    result = future.result()  # This will raise an exception if the task failed
                except Exception as e:
                    logger.info(f"Task generated an exception: {e} | {city[1]}")

            logger.info("Completed")


if __name__ == '__main__':
    crawler = LicenseCrawler()
    crawler.run()
