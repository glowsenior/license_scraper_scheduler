import csv
import json
import logging
import os
import string
import time
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

import urllib3

# Suppress specific warnings
warnings.filterwarnings('ignore', category=urllib3.exceptions.InsecureRequestWarning)
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class LicenseCrawler:

    def __init__(self):

        """Initialize the crawler."""
        self.base_url = 'https://delpros.delaware.gov/OH_VerifyLicense'
        self.detail_url = 'https://delpros.delaware.gov/apexremote'
        self.output_file = 'results/results.csv'
        self.csv_lock = Lock()
        proxy_url = "http://cruexuku-US-rotate:c3h2jphwjv7y@p.webshare.io:80"
        self.proxies = {
            "http": proxy_url,
            "https": proxy_url
        }
        self.headers = {
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.8',
            'Connection': 'keep-alive',
            'Content-Type': 'application/json', 'Origin': 'https://delpros.delaware.gov',
            'Referer': 'https://delpros.delaware.gov/OH_VerifyLicense',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-GPC': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'X-Requested-With': 'XMLHttpRequest',
            'X-User-Agent': 'Visualforce-Remoting',
            'sec-ch-ua': '"Brave";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
        }
        self.target_profession = "Medical Practice"
        self.target_types = ["Physician D.O.", "Physician M.D."]
        self.cookie_pattern = "Visualforce.remoting.Manager.add(new $VFRM.RemotingProviderImpl"

        os.makedirs('results', exist_ok=True)

    def fetch_specialities(self):
        """Fetch initial page data including cookies"""
        response = requests.get(self.base_url, headers=self.headers, verify=False,proxies=self.proxies)
        if response.status_code != 200:
            logger.error(f"Failed to fetch specialities and states")
            return None
        soup = BeautifulSoup(response.text, "html.parser")

        # Extract specialty options
        state_element = soup.find("select", attrs={"id": "j_id0:j_id111:state"})
        states = [x["value"] for x in state_element.find_all("option") if
                  x["value"].strip() and x["value"].strip() != "none"]
        specialities = []
        for letter in string.ascii_uppercase:
            for state in states:
                for type in self.target_types:
                    specialities.append({'license_type': type, "state": state,"last_name":letter})

        return specialities

    def fetch_initial_data(self,retries=3):
        """Fetch initial page data including cookies and hidden form fields."""
        session = vid = auth_token = csrf_token = ns = ver = None
        session = requests.session()
        session.headers.update(self.headers)
        while retries:
            retries -=1
            try:
                response = session.get(self.base_url, verify=False,proxies=self.proxies)
                response.raise_for_status()
                break
            except Exception as exp:
                logger.error("Retrying __ Error Fetching session.")
                time.sleep(1)


        if response.status_code != 200 or not retries:
            logger.error(f"Failed to fetch initial cookies")
            return None

        soup = BeautifulSoup(response.text, "html.parser")

        # Extract viewstate tokens and session cookie
        # Use regular expression to match the JSON structure within the <script> tag
        script_tags = soup.find_all('script', type='text/javascript')
        script_tag = None
        for script_t in script_tags:
            if self.cookie_pattern in script_t.text:
                script_tag = script_t
                break
        if script_tag:
            # Extract the content of the <script> tag (the JavaScript code)
            script_content = script_tag.string.strip()

            # Extract the JSON part (in this case, between the parentheses)
            start_index = script_content.find('({')
            end_index = script_content.rfind('})') + 1

            json_str = script_content[start_index + 1:end_index]

            # Parse the extracted JSON string into a Python dictionary
            data = json.loads(json_str)
            vid = data.get("vf", {}).get("vid", {})
            actions = data.get("actions", {}).get("OH_VerifyLicenseCtlr", {}).get("ms", [])
            for action in actions:
                if action.get("name", "") == "findLicensesForOwner":
                    auth_token = action.get("authorization", "")
                    csrf_token = action.get("csrf", "")
                    ns = action.get("ns", "")
                    ver = action.get("ver", 46)

        else:
            logger.error("initial cookies does not found")
            return None

        return session, vid, auth_token, csrf_token, ns, ver

    def extract_license_type(self, license_type_str):
        license_type = None
        if "M.D." in license_type_str:
            license_type = "MD"
        if "D.O." in license_type_str:
            license_type = "DO"
        return license_type

    def convert_timestamp_to_date(self, timestamp_ms):

        if not timestamp_ms:
            return ""
            # Convert milliseconds to seconds
        timestamp_sec = timestamp_ms / 1000

        # Known Unix epoch start date (January 1, 1970)
        epoch_start = datetime(1970, 1, 1)

        # Subtract the timestamp in seconds from the epoch start date to get the correct date
        date_obj = epoch_start + timedelta(seconds=timestamp_sec)

        # Format the datetime object as MM/DD/YYYY
        formatted_date = date_obj.strftime("%m/%d/%Y")

        return formatted_date

    def fetch_licensee_data(self, session, vid, auth_token, csrf_token, ns, ver, specialty_name, state,last_name,retries=3):
        """Submit the form for a given specialty and fetch the results page."""

        json_data = {
            'action': 'OH_VerifyLicenseCtlr',
            'method': 'findLicensesForOwner',
            'data': [
                {
                    'firstName': '',
                    'lastName': last_name,
                    'board': self.target_profession,
                    'licenseType': specialty_name,
                    'licenseNumber': '',
                    'city': '',
                    'state': state,
                    'businessBoard': '',
                    'businessLicenseType': '',
                    'businessLicenseNumber': '',
                    'businessCity': '',
                    'businessState': 'none',
                    'businessName': '',
                    'dbafileld': '',
                    'searchType': 'individual',
                },
            ],
            'type': 'rpc',
            'tid': 2,
            'ctx': {
                'csrf': csrf_token,
                'vid': vid,
                'ns': ns,
                'ver': ver,
                'authorization': auth_token
                ,
            },
        }

        # logger.info(f"Fetching listing page=1 for speciality  {specialty_name} state {state}")

        while retries:
            retries -=1
            try:
                response = session.post(self.detail_url, headers=self.headers, json=json_data, verify=False,proxies=self.proxies)

                response.raise_for_status()
                return response.json()
            except Exception as e:
                logger.error(f"Retrying __ Failed to fetch data for specialty {specialty_name}: {e}")
                time.sleep(1)
        return None

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

    def save_to_csv(self, results):
        """Save results to CSV in a thread-safe manner."""
        with self.csv_lock:
            with open(self.output_file, 'a', newline='', encoding='utf-8') as csvfile:
                fieldnames = ["Full_Name", "License_Type", "License_Number", "Status", "Professional", "Issued",
                              "Expired"]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                for result in results:
                    writer.writerow(result)

    def crawl_specialty(self, specialty_dict, index, total, init_data):
        """Crawl and process licensee data for a given specialty."""
        specialty_name = specialty_dict['license_type']
        state = specialty_dict['state']
        last_name = specialty_dict['last_name']
        logger.info(f"Crawling data for specialty({index}/{total}): {specialty_name} state: {state} last name: {last_name}")

        session, vid, auth_token, csrf_token, ns, ver = init_data
        page_content = self.fetch_licensee_data(session, vid, auth_token, csrf_token, ns, ver, specialty_name, state,last_name)
        if page_content:
            licensees = []
            results = page_content[0].get("result")
            if isinstance(results, list):
                licensees = results
            if isinstance(results, dict):
                licensees = results.get("v", [])

            results_rows = []
            for licensee in licensees:

                licensee_dict = licensee.get("license", "{}")
                if "v" and "s" in licensee_dict.keys():
                    licensee_dict = licensee_dict.get("v", {})
                row = {
                    "Full_Name": licensee.get("Name", ""),
                    "License_Type": self.extract_license_type(licensee.get("Type", "")),
                    "License_Number": licensee.get("RecNumber", ""),
                    "Issued": self.convert_timestamp_to_date(licensee_dict.get("MUSW__Issue_Date__c", "")),
                    "Expired": self.convert_timestamp_to_date(licensee_dict.get("MUSW__Expiration_Date__c", "")),
                    "Status": licensee.get("Status", ""),
                    "Professional": licensee_dict.get("Board__c", "")
                }
                if row:
                    results_rows.append(row)
            logger.info(f"Rows found: {len(results_rows)} | specialty: {specialty_name} last name: {last_name}")
            if results_rows:
                self.save_to_csv(results_rows)
        else:
            logger.info(f"Page content not found. Specialty: {specialty_name} state: {state} last name: {last_name}")

    def run(self):
        """Run the crawler concurrently for all specialties."""

        specialties = self.fetch_specialities()
        if not specialties:
            logger.error(f"Could not establish a scraper.")
            return
        init_data = self.fetch_initial_data()
        if not init_data:
            logger.error(f"Could not extract data")
            return

        # Write CSV headers
        with open(self.output_file, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ["Full_Name", "License_Type", "License_Number", "Status", "Professional", "Issued", "Expired"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

        total_specialities = len(specialties)
        logger.info(f"Total specialities found: {total_specialities}")

        # Process each specialty concurrently
        with ThreadPoolExecutor(max_workers=
                                5) as executor:
            futures = {
                executor.submit(self.crawl_specialty, specialty, index, total_specialities,init_data): specialty for
                index, specialty in enumerate(specialties, start=1)
            }

            # Wait for all futures to complete
            for future in as_completed(futures):
                specialty_dict = futures[future]
                try:
                    # Optionally, you can get the result if needed
                    result = future.result()  # This will raise an exception if the task failed
                except Exception as e:
                    logger.error(
                        f"Task generated an exception: {e} | speciality: {specialty_dict['license_type']}, state: {specialty_dict['state']} last name: {specialty_dict['last_name']}")
        # check duplicates on the bases of License_Number
        logger.info("Removing duplicates if any.")
        self.remove_duplicates_in_csv("License_Number")


if __name__ == '__main__':
    crawler = LicenseCrawler()
    crawler.run()
