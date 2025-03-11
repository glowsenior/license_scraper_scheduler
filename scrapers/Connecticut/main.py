import csv
import logging
import os
import re
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
        self.base_url = 'https://elicense.ct.gov/Lookup/LicenseLookup.aspx'
        self.detail_url = 'https://elicense.ct.gov/Lookup/licensedetail.aspx'
        self.output_file = 'results/results.csv'
        self.csv_lock = Lock()
        self.headers = {
            'accept': 'text/html, */*; q=0.01',
            'accept-language': 'en-US,en;q=0.6',
            'priority': 'u=1, i',
            'referer': 'https://elicense.ct.gov/Lookup/LicenseLookup.aspx',
            'sec-ch-ua': '"Chromium";v="130", "Brave";v="130", "Not?A_Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'sec-gpc': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
            'x-requested-with': 'XMLHttpRequest',
        }
        self.target_types = ["Physician / Surgeon", "Physician Assistant"]
        self.data = {'__VIEWSTATEENCRYPTED': '',
                     'ctl00$MainContentPlaceHolder$ucLicenseLookup$ctl03$ddCredPrefix': '',
                     'ctl00$MainContentPlaceHolder$ucLicenseLookup$ctl03$tbLicenseNumber': '',
                     'ctl00$MainContentPlaceHolder$ucLicenseLookup$ctl03$ddSubCategory': '',
                     'ctl00$MainContentPlaceHolder$ucLicenseLookup$ctl03$tbDBA_Contact': '',
                     'ctl00$MainContentPlaceHolder$ucLicenseLookup$ctl03$tbMaidenName_Contact': '',
                     'ctl00$MainContentPlaceHolder$ucLicenseLookup$ctl03$tbFirstName_Contact': '',
                     'ctl00$MainContentPlaceHolder$ucLicenseLookup$ctl03$tbLastName_Contact': '',
                     'ctl00$MainContentPlaceHolder$ucLicenseLookup$ctl03$tbAddress2_ContactAddress': '',
                     'ctl00$MainContentPlaceHolder$ucLicenseLookup$ctl03$tbCity_ContactAddress': '',
                     'ctl00$MainContentPlaceHolder$ucLicenseLookup$ctl03$ddStates': '',
                     'ctl00$MainContentPlaceHolder$ucLicenseLookup$ctl03$tbZipCode_ContactAddress': '',
                     'ctl00$MainContentPlaceHolder$ucLicenseLookup$ctl03$ddCountry': '221',
                     'ctl00$MainContentPlaceHolder$ucLicenseLookup$ResizeLicDetailPopupID_ClientState': '0,0',
                     'ctl00$OutsidePlaceHolder$ucLicenseDetailPopup$ResizeLicDetailPopupID_ClientState': '0,0',
                     '__ASYNCPOST': 'true'}

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

        return specialties

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
        session_cookie = response.cookies.get("ASP.NET_SessionId", "")

        return viewstate, viewstate_generator, session_cookie

    def fetch_licensee_data(self, viewstate, viewstate_generator, session_cookie, specialty_value, specialty_name):
        """Submit the form for a given specialty and fetch the results page."""
        data = {
            'ctl00$ScriptManager1': 'ctl00$MainContentPlaceHolder$ucLicenseLookup$UpdtPanelGridLookup|ctl00$MainContentPlaceHolder$ucLicenseLookup$UpdtPanelGridLookup',
            '__EVENTTARGET': 'ctl00$MainContentPlaceHolder$ucLicenseLookup$UpdtPanelGridLookup',
            '__EVENTARGUMENT': '11',
            '__VIEWSTATE': viewstate,
            '__VIEWSTATEGENERATOR': viewstate_generator,
            '__VIEWSTATEENCRYPTED': '',
            'ctl00$MainContentPlaceHolder$ucLicenseLookup$ctl03$lbMultipleCredentialTypePrefix': specialty_value,
        }

        data.update(self.data)
        logger.info(
            f"Fetching listing page=1 for speciality  {specialty_name}")
        cookies = {'ASP.NET_SessionId': session_cookie}

        try:
            response = requests.post(self.base_url, headers=self.headers, cookies=cookies, data=data, verify=False)

            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch data for specialty {specialty_value}: {e}")
            return None

    def parse_licensee_data(self, html_content):
        """Parse the licensee data from the HTML content."""
        soup = BeautifulSoup(html_content, 'html.parser')
        # Extract Registration Information
        registration_info = soup.find('table', {'id': 'Grid1'}).find_all('td')
        professional = registration_info[0].text.strip() if registration_info else None
        license_number = registration_info[1].text.strip() if registration_info else None
        status = registration_info[6].text.strip() if registration_info else None
        issued = registration_info[3].text.strip() if registration_info else None
        full_name = registration_info[4].text.strip().replace("\t", "") if registration_info else None
        expired = registration_info[2].text.strip() if registration_info else None

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
        soup = BeautifulSoup(html_content, "html.parser")
        table = soup.find("table", attrs={"class": "table table-responsive table-hover table-striped"})
        if not table:
            return [], next_page_num
        table_rows = table.tbody.find_all("tr")
        results = []

        for row in table_rows:
            # Loop through all <a> tags within <td> elements and filter by the href pattern
            for td in row.find_all('td'):
                # whose id starts with: ctl00_MainContentPlaceHolder_ucLicenseLookup_gvSearchResults_ct
                a_tag = td.find('a', href=True)  # Find <a> tag with an href attribute
                if a_tag and a_tag['href']:  # Check if the href matches the pattern
                    if a_tag["id"].startswith('ctl00_MainContentPlaceHolder_ucLicenseLookup_gvSearchResults_ct'):
                        # Use regular expression to capture the parameters inside the DisplayLicenceDetail() function
                        href_value = a_tag['href']
                        match = re.search(r"DisplayLicenceDetail\('([^']+)'\)", href_value)

                        if match:
                            # Extracted parameters inside the function
                            params = match.group(1)
                            results.append(params)

        page_nums = set()
        pagination_row = table.find('tr', class_="CavuGridPager")  # Find the link with title "Next page"
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

        return results, next_page_num

    def fetch_next_page(self, viewstate, viewstate_generator, session_cookie, specialty_value, specialty_name,
                        page_num):
        """Submit the form for a given specialty and fetch the results page."""
        data = {
            'ctl00$ScriptManager1': 'ctl00$MainContentPlaceHolder$ucLicenseLookup$UpdtPanelGridLookup|ctl00$MainContentPlaceHolder$ucLicenseLookup$gvSearchResults',
            '__EVENTTARGET': 'ctl00$MainContentPlaceHolder$ucLicenseLookup$gvSearchResults',
            '__EVENTARGUMENT': f'Page${page_num}',
            '__VIEWSTATE': viewstate,
            '__VIEWSTATEGENERATOR': viewstate_generator,
            'ctl00$MainContentPlaceHolder$ucLicenseLookup$ctl03$lbMultipleCredentialTypePrefix': specialty_value,

        }
        data.update(self.data)
        logger.info(
            f"Fetching listing page={page_num} for speciality  {specialty_name}")
        cookies = {'ASP.NET_SessionId': session_cookie}

        try:
            response = requests.post(self.base_url, headers=self.headers, cookies=cookies, data=data, verify=False)

            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch data for specialty {specialty_value}: {e}")
            return None

    def fetch_detail_page(self, detail_page_id, session_cookie, specialty_name, index, retries=3):
        """Submit the form for a given details page."""

        cookies = {'ASP.NET_SessionId': session_cookie}

        params = {
            'id': detail_page_id,
        }
        while retries:
            retries -= 1

            logger.info(f"Fetching details {index} for speciality {specialty_name}")
            try:

                response = requests.get(self.detail_url, headers=self.headers, cookies=cookies, params=params,
                                        verify=False)
                response.raise_for_status()
                return response.text
            except requests.exceptions.RequestException as e:
                logger.error(f"Retrying __ Failed to fetch data for detail page {index}: {e}")
        return None

    def crawl_specialty(self, specialty_name, specialty_value):
        """Crawl and process licensee data for a given specialty."""
        logger.info(f"Crawling data for specialty: {specialty_name}")
        init_data = self.fetch_initial_data()
        if not init_data:
            logger.error(f"Could not extract: {specialty_name}")
            return
        viewstate, viewstate_generator, session_cookie = init_data
        page_content = self.fetch_licensee_data(viewstate, viewstate_generator, session_cookie,
                                                specialty_value, specialty_name)
        if page_content:
            results_ids = []
            results, next_page_num = self.parse_listing_page(page_content)
            results_ids.extend(results)

            # Covering Pagination
            while next_page_num:
                page_content = self.fetch_next_page(viewstate, viewstate_generator, session_cookie,
                                                    specialty_value, specialty_name, next_page_num)

                results, next_page_num = self.parse_listing_page(page_content, next_page_num)

                logger.info(f"Found more rows: {len(results)}")
                results_ids.extend(results)

            if results_ids:
                logger.info(f"Rows Expected: {len(results_ids)} | {specialty_name}")
                logger.info("Extracting details...")
                result_rows = []

                for index, results_id in enumerate(results_ids, start=1):
                    row = self.parse_licensee_data(
                        self.fetch_detail_page(results_id, session_cookie, specialty_name, index))
                    if row and row["Full_Name"]:
                        result_rows.append(row)
                    else:
                        print(row)

                if result_rows:
                    logger.info(f"Rows found: {len(result_rows)} | {specialty_name}")
                    self.save_to_csv(result_rows)

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
        with ThreadPoolExecutor(max_workers=2) as executor:
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
