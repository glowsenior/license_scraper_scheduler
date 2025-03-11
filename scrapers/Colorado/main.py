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
        self.base_url = 'https://apps2.colorado.gov/dora/licensing/lookup/licenselookup.aspx'
        self.detail_url = 'https://apps2.colorado.gov/dora/licensing/Lookup/licensedetail.aspx'
        self.output_file = 'results/results.csv'
        self.csv_lock = Lock()
        self.headers = {
            'Accept': 'text/html, */*; q=0.01',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive',
            'Referer': 'https://apps2.colorado.gov/dora/licensing/lookup/licenselookup.aspx',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-GPC': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'X-Requested-With': 'XMLHttpRequest',
            'sec-ch-ua': '"Brave";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
        }
        self.listing_header = {
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Origin': 'https://apps2.colorado.gov',
            'Referer': 'https://apps2.colorado.gov/dora/licensing/lookup/licenselookup.aspx',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-GPC': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'X-MicrosoftAjax': 'Delta=true',
            'X-Requested-With': 'XMLHttpRequest',
            'sec-ch-ua': '"Brave";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
        }
        self.max_pages_count = 9
        self.target_types = ["Medical"]
        self.data = {
            'ctl00$MainContentPlaceHolder$ucLicenseLookup$ctl03$ddCredPrefix': '',
            'ctl00$MainContentPlaceHolder$ucLicenseLookup$ctl03$tbLicenseNumber': '',
            'ctl00$MainContentPlaceHolder$ucLicenseLookup$ctl03$ddSubCategory': '',
            'ctl00$MainContentPlaceHolder$ucLicenseLookup$ctl03$tbDBA_Contact': '',
            'ctl00$MainContentPlaceHolder$ucLicenseLookup$ctl03$tbMaidenName_Contact': '',
            'ctl00$MainContentPlaceHolder$ucLicenseLookup$ctl03$tbCity_ContactAddress': '',

            'ctl00$MainContentPlaceHolder$ucLicenseLookup$ctl03$tbZipCode_ContactAddress': '',
            'ctl00$MainContentPlaceHolder$ucLicenseLookup$ResizeLicDetailPopupID_ClientState': '0,0',
            'ctl00$OutsidePlaceHolder$ucLicenseDetailPopup$ResizeLicDetailPopupID_ClientState': '0,0',
            '__ASYNCPOST': 'true'}

        self.last_name_values = [''.join(comb) for comb in itertools.product('ABCDEFGHIJKLMNOPQRSTUVWXYZ', repeat=2)]
        self.first_name_values = [x for x in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ']
        self.TWO_CAPTCHA_API_KEY = "b22b185ce18a4b6d1a7d26cb97889c96"


        os.makedirs('results', exist_ok=True)

    def fetch_specialities(self):
        """Fetch initial page data including cookies"""
        response = requests.get(self.base_url, headers=self.headers, verify=False)
        if response.status_code != 200:
            logger.error(f"Failed to fetch specialities")
            return None
        soup = BeautifulSoup(response.text, "html.parser")

        # Extract specialty options
        specialty_element = soup.find("select", attrs={
            "name": "ctl00$MainContentPlaceHolder$ucLicenseLookup$ctl03$lbMultipleCredentialTypePrefix"})
        specialties = {option["value"]: option["title"].strip() for option in specialty_element.find_all("option") if
                       option["value"].strip() and option["title"].strip() in self.target_types}

        state_element = soup.find("select", attrs={
            "name": "ctl00$MainContentPlaceHolder$ucLicenseLookup$ctl03$ddStates"})
        states = [option["value"] for option in state_element.find_all("option") if
                  option["value"].strip()]


        return specialties, states

    def solve_captcha(self, image_url,retries=3):
        """Solving image captcha using two captcha api"""

        captcha_solution = None

        while retries:
            retries -= 1

            if not image_url:
                logger.error(f"Invalid Image for solving captcha")
                return captcha_solution

            logger.info("Solving captcha")

            try:
                # Initialize the 2Captcha solver
                solver = TwoCaptcha(self.TWO_CAPTCHA_API_KEY)

                # Solve the CAPTCHA
                img_url = "https://apps2.colorado.gov/dora/licensing/" + image_url.replace("../", "")
                result = solver.normal(img_url)
                captcha_solution = result.get("code", None)

            except Exception as e:
                logger.error(f"An error occurred while solving captcha: {e}")
                time.sleep(5)

        return captcha_solution

    def fetch_initial_data(self):
        """Fetch initial page data including cookies and hidden form fields."""
        response = requests.get(self.base_url, headers=self.headers, verify=False)
        if response.status_code != 200:
            logger.error(f"Failed to fetch initial cookies")
            return None
        soup = BeautifulSoup(response.text, "html.parser")

        # Extract viewstate tokens and session cookie
        viewstate = soup.find("input", attrs={"id": "__VIEWSTATE"})["value"]
        viewstate_generator = soup.find("input", attrs={"id": "__VIEWSTATEGENERATOR"})["value"]

        session_cookie = {}
        for response_cookie in response.cookies:
            session_cookie[response_cookie.name] = response_cookie.value
        image_url = soup.find("img", id="FormShield1_Image")["src"] if soup.find("img",
                                                                                 id="FormShield1_Image") else None
        captcha_solution = self.solve_captcha(image_url)

        if not captcha_solution:
            logger.error(f"Failed to fetch initial cookies")
            return None

        return viewstate, viewstate_generator, session_cookie, captcha_solution

    def fetch_licensee_data(self, captcha_solution, viewstate, viewstate_generator, session_cookie, specialty_value,
                            specialty_name,
                            state_initial, last_name_value, first_name_value=""):
        """Submit the form for a given specialty and fetch the results page."""
        data = {
            'ctl00$ScriptManager1': 'ctl00$MainContentPlaceHolder$ucLicenseLookup$UpdtPanelGridLookup|ctl00$MainContentPlaceHolder$ucLicenseLookup$UpdtPanelGridLookup',
            '__EVENTTARGET': 'ctl00$MainContentPlaceHolder$ucLicenseLookup$UpdtPanelGridLookup',
            '__EVENTARGUMENT': "11",
            '__VIEWSTATE': viewstate,
            '__VIEWSTATEGENERATOR': viewstate_generator,
            'ctl00$MainContentPlaceHolder$ucLicenseLookup$ctl03$lbMultipleCredentialTypePrefix': specialty_value,
            'ctl00$MainContentPlaceHolder$ucLicenseLookup$ctl03$ddStates': state_initial,
            "ctl00$MainContentPlaceHolder$ucLicenseLookup$ctl03$tbLastName_Contact": last_name_value,
            "ctl00$MainContentPlaceHolder$ucLicenseLookup$ctl03$tbFirstName_Contact": first_name_value,
            "ctl00$MainContentPlaceHolder$ucLicenseLookup$CaptchaSecurity1$txtCAPTCHA": captcha_solution,
        }

        data.update(self.data)
        logger.info(
            f"Fetching listing page=1 for speciality  {specialty_name} state {state_initial} Last name: {last_name_value} First Name: {first_name_value}")

        try:
            response = requests.post(self.base_url, headers=self.listing_header, cookies=session_cookie, data=data,
                                     verify=False)


            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch data for specialty {specialty_value}: {e}")
            return None

    def parse_licensee_data(self, html_content):
        """Parse the licensee data from the HTML content."""
        if not html_content:
            return None
        soup = BeautifulSoup(html_content, 'html.parser')
        # Extract Registration Information

        name_info = soup.find('table', {'id': 'Grid0'}).find_all('td')
        full_name = name_info[0].text.strip().replace("\t", "") if name_info else None

        # Extract Registration Information
        registration_info = soup.find('table', {'id': 'Grid1'}).find_all('td')
        license_number = registration_info[0].text.strip() if registration_info else None
        professional = registration_info[2].text.strip() if registration_info else None
        status = registration_info[3].text.strip() if registration_info else None
        issued = registration_info[4].text.strip() if registration_info else None
        expired = registration_info[6].text.strip() if registration_info else None

        return {
            "Full_Name": full_name,
            "License_Type": professional,
            "License_Number": license_number,
            "Issued": issued,
            "Expired": expired,
            "Status": status,
            "Professional": professional
        }

    def save_to_csv(self, results):
        """Save results to CSV in a thread-safe manner."""
        with self.csv_lock:
            with open(self.output_file, 'a', newline='', encoding='utf-8') as csvfile:
                fieldnames = ["Full_Name", "License_Type", "License_Number", "Status", "Professional", "Issued",
                              "Expired"]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                for result in results:
                    writer.writerow(result)

    def parse_listing_page(self, html_content, current_page_num=1):
        next_page_num = None
        total_pages_count = 0
        soup = BeautifulSoup(html_content, "html.parser")
        table = soup.find("table", attrs={"class": "table table-responsive table-hover table-striped"})
        if not table:
            return [], next_page_num, total_pages_count
        table_tbody = table.tbody
        if not table_tbody:
            return [], next_page_num, total_pages_count
        table_rows = table_tbody.find_all("tr")
        results = []

        for row in table_rows:
            if row:
                # Loop through all <a> tags within <td> elements and filter by the href pattern
                row_data = row.find_all('td')
                if row_data:
                    for td in row_data:
                        # whose id starts with: ctl00_MainContentPlaceHolder_ucLicenseLookup_gvSearchResults_ct
                        a_tag = td.find('a', href=True)  # Find <a> tag with an href attribute
                        if a_tag and a_tag['href']:  # Check if the href matches the pattern
                            if a_tag["id"].startswith(
                                    'ctl00_MainContentPlaceHolder_ucLicenseLookup_gvSearchResults_ct'):
                                # Use regular expression to capture the parameters inside the DisplayLicenceDetail() function
                                href_value = a_tag['href']
                                match = re.search(r"DisplayLicenceDetail\('([^']+)'\)", href_value)

                                if match:
                                    # Extracted parameters inside the function
                                    params = match.group(1)
                                    # check if records is active or not
                                    if row_data[3].text.strip() == "Active":
                                        results.append(params)

        page_nums = set()
        pagination_row = table.find('tr', class_="CavuGridPager")  # Find the link with title "Next page"
        if pagination_row:
            pagination_a_tags = pagination_row.find_all('a', href=True)  # Find <a> tag with an href attribute
            for pagination_a_tag in pagination_a_tags:
                if pagination_a_tag and pagination_a_tag['href']:  # Check if the href matches the pattern
                    # Extract the href attribute
                    href_value = pagination_a_tag['href']

                    # Use regular expression to capture the page number after 'Page$'
                    match = re.search(r"Page\$(\d+)", href_value)

                    if match:
                        # Extracted page number
                        p = match.group(1)
                        page_nums.add(int(p))

            next_page_num = next((num for num in page_nums if num > current_page_num), None)
            total_pages_count = max(page_nums)

        return results, next_page_num, total_pages_count

    def fetch_next_page(self, captcha_solution, viewstate, viewstate_generator, session_cookie, specialty_value,
                        specialty_name,
                        state_initial,
                        page_num, last_name_value, first_name_value):
        """Submit the form for a given specialty and fetch the results page."""
        data = {
            'ctl00$ScriptManager1': 'ctl00$MainContentPlaceHolder$ucLicenseLookup$UpdtPanelGridLookup|ctl00$MainContentPlaceHolder$ucLicenseLookup$gvSearchResults',
            '__EVENTTARGET': 'ctl00$MainContentPlaceHolder$ucLicenseLookup$gvSearchResults',
            '__EVENTARGUMENT': f'Page${page_num}',
            '__VIEWSTATE': viewstate,
            '__VIEWSTATEGENERATOR': viewstate_generator,
            'ctl00$MainContentPlaceHolder$ucLicenseLookup$ctl03$lbMultipleCredentialTypePrefix': specialty_value,
            'ctl00$MainContentPlaceHolder$ucLicenseLookup$ctl03$ddStates': state_initial,
            "ctl00$MainContentPlaceHolder$ucLicenseLookup$ctl03$tbLastName_Contact": last_name_value,
            "ctl00$MainContentPlaceHolder$ucLicenseLookup$ctl03$tbFirstName_Contact": first_name_value,
            "ctl00$MainContentPlaceHolder$ucLicenseLookup$CaptchaSecurity1$txtCAPTCHA": captcha_solution,

        }
        data.update(self.data)
        logger.info(
            f"Fetching listing page={page_num} for speciality  {specialty_name} state {state_initial} Last name: {last_name_value} First Name: {first_name_value}")

        try:
            response = requests.post(self.base_url, headers=self.listing_header, cookies=session_cookie, data=data,
                                     verify=False)

            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch data for specialty {specialty_value}: {e}")
            return None

    def fetch_detail_page(self, detail_page_id, session_cookie, specialty_name, index, retries=3):
        """Submit the form for a given details page."""

        params = {
            'id': detail_page_id,
        }
        while retries:
            retries -= 1

            logger.info(f"Fetching details {index} for speciality {specialty_name} ID: {detail_page_id}")
            try:

                response = requests.get(self.detail_url, headers=self.headers, cookies=session_cookie, params=params,
                                        verify=False)
                response.raise_for_status()
                return response.text
            except requests.exceptions.RequestException as e:
                logger.error(f"Retrying __ Failed to fetch data for detail page {index}: {e}")
        return None

    def process_records(self, page_content, captcha_solution, viewstate, viewstate_generator, session_cookie,
                        specialty_value, specialty_name, state_initial,
                        last_name_value, first_name_value):
        """Process the listing of records"""
        results_ids = []
        results, next_page_num, total_pages_count = self.parse_listing_page(page_content)
        logger.info(f"Found Active rows: {len(results)}")
        results_ids.extend(results)

        # Covering Pagination
        while next_page_num:
            page_content = self.fetch_next_page(captcha_solution, viewstate, viewstate_generator, session_cookie,
                                                specialty_value, specialty_name, state_initial,
                                                next_page_num, last_name_value, first_name_value)

            results, next_page_num, _ = self.parse_listing_page(page_content, next_page_num)

            logger.info(f"Found more Active rows: {len(results)}")
            results_ids.extend(results)


        if results_ids:
            logger.info(f"Rows Expected: {len(results_ids)} | {specialty_name}")
            logger.info("Extracting details...")
            result_rows = []

            for index, results_id in enumerate(results_ids, start=1):
                row = self.parse_licensee_data(
                    self.fetch_detail_page(results_id, session_cookie, specialty_name, index))
                if row:
                    result_rows.append(row)

            if result_rows:
                logger.info(f"Rows found: {len(result_rows)} | {specialty_name}")
                self.save_to_csv(result_rows)

    def crawl_specialty(self, specialty_name, specialty_value, states):
        """Crawl and process licensee data for a given specialty."""
        logger.info(f"Crawling data for specialty: {specialty_name}")
        init_data = self.fetch_initial_data()
        if not init_data:
            logger.error(f"Could not extract: {specialty_name}")
            return

        viewstate, viewstate_generator, session_cookie, captcha_solution = init_data
        if not captcha_solution:
            logger.error(f"Captcha Solution not found: {captcha_solution}")
            return
        for index, state_initial in enumerate(states, start=1):

            for last_name_index, last_name_value in enumerate(self.last_name_values, start=1):
                page_content = self.fetch_licensee_data(captcha_solution, viewstate, viewstate_generator,
                                                        session_cookie,
                                                        specialty_value, specialty_name, state_initial, last_name_value)
                if page_content:

                    results, next_page_num, total_pages_count = self.parse_listing_page(page_content)
                    if total_pages_count == self.max_pages_count:
                        # Loop through First Names
                        for first_name_index, first_name_value in enumerate(self.first_name_values, start=1):
                            page_content = self.fetch_licensee_data(captcha_solution, viewstate, viewstate_generator,
                                                                    session_cookie,
                                                                    specialty_value, specialty_name, state_initial,
                                                                    last_name_value, first_name_value)
                            if page_content:
                                self.process_records(page_content, captcha_solution, viewstate,
                                                     viewstate_generator, session_cookie,
                                                     specialty_value, specialty_name, state_initial,
                                                     last_name_value, first_name_value)

                    else:
                        # Only use last name combinations
                        self.process_records(page_content, captcha_solution, viewstate,
                                             viewstate_generator, session_cookie,
                                             specialty_value, specialty_name, state_initial,
                                             last_name_value, first_name_value="")

                else:
                    logger.info(f"Page content not found: {specialty_name}")

    def run(self):
        """Run the crawler concurrently for all specialties."""

        specialties, states = self.fetch_specialities()
        if not specialties or not states:
            logger.error(f"Could not establish a scraper.")
            return

        # Write CSV headers
        with open(self.output_file, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ["Full_Name", "License_Type", "License_Number", "Status", "Professional", "Issued", "Expired"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

        # Process each specialty concurrently
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = {
                executor.submit(self.crawl_specialty, specialty_name, specialty_value, states): (
                    specialty_value, specialty_name) for specialty_value, specialty_name in specialties.items()
            }

            # Wait for all futures to complete
            for future in as_completed(futures):
                specialty_value, specialty = futures[future]
                try:
                # Optionally, you can get the result if needed
                    result = future.result()  # This will raise an exception if the task failed
                except Exception as e:
                    logger.error(f"Task generated an exception: {e} | {specialty}")
            logger.info("Completed")


if __name__ == '__main__':
    crawler = LicenseCrawler()
    crawler.run()
