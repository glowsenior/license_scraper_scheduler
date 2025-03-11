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
        self.base_url = 'https://aca-prod.accela.com/MILARA/GeneralProperty/PropertyLookUp.aspx'
        self.output_file = 'results/results.csv'
        self.csv_lock = Lock()

        self.headers = {
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.7',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',

            'Origin': 'https://aca-prod.accela.com',
            'Referer': 'https://aca-prod.accela.com/MILARA/GeneralProperty/PropertyLookUp.aspx?isLicensee=Y&TabName=APO',
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


        self.target_types = ['Medical Doctor', 'Osteopathic Physician']

        self.data = {"__VIEWSTATEENCRYPTED": "",
                     "ctl00$HeaderNavigation$hdnShoppingCartItemNumber": "",
                     "ctl00$HeaderNavigation$hdnShowReportLink": "N",
                     "ctl00$PlaceHolderMain$refLicenseeSearchForm$txtLicenseNumber": "",
                     "ctl00$PlaceHolderMain$refLicenseeSearchForm$txtBusiLicense": "",
                     "ctl00$PlaceHolderMain$refLicenseeSearchForm$txtFirstName": "",
                     "ctl00$PlaceHolderMain$refLicenseeSearchForm$txtMiddleInitial": "",
                     "ctl00$PlaceHolderMain$refLicenseeSearchForm$txtLastName": "",
                     "ctl00$PlaceHolderMain$refLicenseeSearchForm$txtBusiName": "",
                     "ctl00$PlaceHolderMain$refLicenseeSearchForm$txtTitle": "",
                     "ctl00$PlaceHolderMain$refLicenseeSearchForm$txtInsuranceCompany": "",
                     "ctl00$HDExpressionParam": "",
                     "Submit": "Submit",
                     "__ASYNCPOST": "true",
                     }
        self.params = {
            'isLicensee': 'Y',
            'TabName': 'APO',
        }

        self.meta_data_pattern = r'\|hiddenField\|(?P<key>[^|]+)\|(?P<value>[^|]*)'
        os.makedirs('results', exist_ok=True)

    def fetch_specialities(self):
        """Fetch initial page data including cookies"""
        response = requests.get(self.base_url, headers=self.headers)
        if response.status_code != 200:
            logger.error(f"Failed to fetch specialities: {response.status_code}")
            return None
        soup = BeautifulSoup(response.text, "html.parser")

        # Extract specialty options
        speciality_element = soup.find("select",
                                       attrs={"id": "ctl00_PlaceHolderMain_refLicenseeSearchForm_ddlLicenseType"})

        specialties = {option["value"]: option.text.strip() for option in speciality_element.find_all("option") if
                       option["value"].strip() and option["value"].strip() in self.target_types}


        return specialties

    def fetch_initial_data(self):
        """Fetch initial page data including cookies and hidden form fields."""
        session = requests.Session()
        session.headers.update(self.headers)
        response = session.get(self.base_url)
        if response.status_code != 200:
            logger.error(f"Failed to fetch initial cookies")
            return None
        soup = BeautifulSoup(response.text, "html.parser")

        # Extract viewstate tokens and session cookie
        viewstate = soup.find("input", attrs={"id": "__VIEWSTATE"})["value"]
        viewstate_generator = soup.find("input", attrs={"id": "__VIEWSTATEGENERATOR"})["value"]
        aca_cs_field = soup.find("input", attrs={"id": "ACA_CS_FIELD"})["value"]
        session_cookies = {}
        for cookie in response.cookies:
            session_cookies[cookie.name] = cookie.value
        session.cookies.update(session_cookies)
        return viewstate, viewstate_generator, aca_cs_field , session

    def fetch_licensee_data(self, viewstate, viewstate_generator, aca_cs_field , session, specialty_value, specialty_name):
        """Submit the form for a given specialty and fetch the results page."""
        payload = {
            "ctl00$ScriptManager1": "ctl00$PlaceHolderMain$updatePanel|ctl00$PlaceHolderMain$btnNewSearch",
            "__EVENTTARGET": "ctl00$PlaceHolderMain$btnNewSearch",
            "ACA_CS_FIELD": aca_cs_field,
            '__EVENTARGUMENT': '',
            '__VIEWSTATE': viewstate,
            '__VIEWSTATEGENERATOR': viewstate_generator,
            "ctl00$PlaceHolderMain$refLicenseeSearchForm$ddlLicenseType": specialty_value,
        }

        payload.update(self.data)
        logger.info(
            f"Fetching listing page=1 for speciality {specialty_name}")
        try:

            response = session.post(self.base_url,params=self.params,data=payload)

            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch data for specialty {specialty_value}: {e}")
            return None

    def parse_licensee_data(self, html_content):
        """Parse the licensee data from the HTML content."""
        soup = BeautifulSoup(html_content, 'html.parser')
        name_and_address = soup.find('table', {'id': 'Grid0'}).find_all('td')
        full_name = name_and_address[0].text.strip() if name_and_address else None

        # Extract Registration Information
        registration_info = soup.find('table', {'id': 'Grid1'}).find_all('td')
        professional = registration_info[0].text.strip() if registration_info else None
        license_number = registration_info[1].text.strip() if registration_info else None
        status = registration_info[2].text.strip() if registration_info else None
        issued = registration_info[4].text.strip() if registration_info else None
        expired = registration_info[5].text.strip() if registration_info else None

        return {
            "Full_Name": full_name,
            "License_Type": "MD",
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

    def extract_license_type(self, str_license_type):
        return "MD" if str_license_type == "Medical Doctor" else "DO"

    def parse_listing_page_for_next_window(self, html_content):
        event_target = event_argument = None
        soup = BeautifulSoup(html_content, "html.parser")
        page_text = soup.text.strip()

        a_tag = soup.find('a', class_='aca_simple_text font11px', string="Next >")
        if a_tag:
            show_more_pages_element = a_tag.find_parent("td").find_previous_sibling("td")
            if show_more_pages_element:
                if show_more_pages_element.a:
                    if show_more_pages_element.a.text.strip() == "...":
                        href_value = show_more_pages_element.a.get('href')

                        # Parse the value inside __doPostBack
                        match = re.search(r"__doPostBack\('(.*?)','(.*?)'\)", href_value)
                        if match:
                            event_target = match.group(1)
                            event_argument = match.group(2)
                    else:
                        # we have reached last window and need to go to the last page
                        href_value = a_tag.get('href')

                        # Parse the value inside __doPostBack
                        match = re.search(r"__doPostBack\('(.*?)','(.*?)'\)", href_value)
                        if match:
                            event_target = match.group(1)
                            event_argument = match.group(2)


        return event_target, event_argument, page_text

    def parse_listing_page(self, html_content):
        event_target = event_argument = None
        more_active_records = True
        soup = BeautifulSoup(html_content, "html.parser")
        page_text = soup.text.strip()
        # Extract Name and Address
        table_rows = soup.find('table', {'class': 'ACA_GridView ACA_Grid_Caption'}).find_all('tr', attrs={
            "class": ["ACA_TabRow_Odd", "ACA_TabRow_Even"]})
        if not table_rows:
            return [],event_target, event_argument ,None, more_active_records

        results = []
        for index, row in enumerate(table_rows, start=1):
            row_data = row.find_all("td")
            if len(row_data) >= 9:
                status = row_data[7].text.strip() if row_data[7] else None
                if status and not status.startswith("A"):
                    more_active_records = False
                    break
                first_name = row_data[2].text.strip() if row_data[2] else None
                second_name = row_data[3].text.strip() if row_data[3] else None
                last_name = row_data[4].text.strip() if row_data[4] else None
                professional = row_data[0].text.strip() if row_data[0] else None
                license_number = row_data[1].text.strip() if row_data[1] else None
                issue_date = ""

                expiry_date = row_data[8].text.strip() if row_data[8] else None
                license_type = self.extract_license_type(professional)
                # Only include non-empty strings
                full_name = " ".join(filter(None, [first_name, second_name, last_name]))

                # Construct the output dictionary
                fields = {
                    "Full_Name": full_name,
                    "License_Type": license_type,
                    "License_Number": license_number,
                    "Issued": issue_date,
                    "Expired": expiry_date,
                    "Status": status,
                    "Professional": professional
                }
                results.append(fields)

        a_tag = soup.find('a', class_='aca_simple_text font11px', string="Next >")
        if a_tag:

            # Extract the href attribute
            href_value = a_tag.get('href')

            # Parse the value inside __doPostBack
            match = re.search(r"__doPostBack\('(.*?)','(.*?)'\)", href_value)
            if match:
                event_target = match.group(1)
                event_argument = match.group(2)

        return results, event_target, event_argument ,page_text,more_active_records

    def fetch_sorted_page(self, viewstate, viewstate_generator, aca_cs_field , session, specialty_value, specialty_name, event_target, event_argument,retries=5):

        """Submit the form for a given specialty and fetch the results page."""
        data = {
            "ctl00$ScriptManager1": f'''ctl00$PlaceHolderMain$refLicenseeList$updatePanel|{event_target}''',
            "ACA_CS_FIELD": aca_cs_field,
            "__EVENTTARGET": event_target,
            '__VIEWSTATE': viewstate,
            '__VIEWSTATEGENERATOR': viewstate_generator,
            "__EVENTARGUMENT": event_argument,


        }
        data.update(self.data)
        logger.info(
            f"Sorting listing pages for speciality  {specialty_name}")

        while retries:
            retries -= 1
            try:
                response = session.post(self.base_url, data=data, params=self.params)
                response.raise_for_status()
                return response.text
            except requests.exceptions.RequestException as e:
                logger.error(f"Retrying __ Failed to fetch data for specialty {specialty_value}: {e}")
                time.sleep(1)
        return None


    def fetch_next_page(self, viewstate, viewstate_generator, aca_cs_field , session, specialty_value, specialty_name, event_target, event_argument,page_num,retries=5):

        """Submit the form for a given specialty and fetch the results page."""
        data = {
            "ctl00$ScriptManager1": f'''ctl00$PlaceHolderMain$refLicenseeList$updatePanel|{event_target}''',
            "ACA_CS_FIELD": aca_cs_field,
            "__EVENTTARGET": event_target,
            '__VIEWSTATE': viewstate,
            '__VIEWSTATEGENERATOR': viewstate_generator,
            "__EVENTARGUMENT": event_argument,


        }
        data.update(self.data)
        logger.info(
            f"Fetching listing page={page_num} for speciality  {specialty_name}")

        while retries:
            retries -= 1
            try:
                response = session.post(self.base_url, data=data, params=self.params)
                response.raise_for_status()
                return response.text
            except requests.exceptions.RequestException as e:
                logger.error(f"Retrying __ Failed to fetch data for specialty {specialty_value}: {e}")
                time.sleep(1)
        return None



    def extract_meta_data(self,text_data):
        # Find all matches
        matches = re.findall(self.meta_data_pattern, text_data)

        # Convert matches to a dictionary
        fields = {key: value for key, value in matches}

        # Extract specific fields
        required_fields = [
            "ACA_CS_FIELD",
            "__VIEWSTATEGENERATOR",
            "__VIEWSTATE",
        ]

        parsed_fields = {field: fields.get(field, None) for field in required_fields}

        return parsed_fields.get("__VIEWSTATE","") ,  parsed_fields.get("__VIEWSTATEGENERATOR","") ,  parsed_fields.get("ACA_CS_FIELD","")

    def crawl_specialty(self, specialty_name, specialty_value):
        """Crawl and process licensee data for a given specialty."""
        logger.info(f"Crawling data for specialty: {specialty_name}")
        init_data = self.fetch_initial_data()
        if not init_data:
            logger.error(f"Could not extract: {specialty_name}")
            return
        viewstate, viewstate_generator, aca_cs_field , session = init_data
        page_content = self.fetch_licensee_data(viewstate, viewstate_generator, aca_cs_field , session,
                                                specialty_value, specialty_name)
        rows_count = 0
        if page_content:
            # use show more pagination to go till the last page.
            event_target, event_argument, page_text = self.parse_listing_page_for_next_window(page_content)
            page_num = 1
            # Covering Pagination
            while event_target:
                page_num += 10
                viewstate, viewstate_generator, aca_cs_field = self.extract_meta_data(page_text)
                page_content = self.fetch_next_page(viewstate, viewstate_generator, aca_cs_field, session,
                                                    specialty_value, specialty_name, event_target, event_argument,
                                                    page_num)
                event_target, event_argument, page_text = self.parse_listing_page_for_next_window(page_content)

            logger.info("Reached to the end")
            # Make a sort call

            page_content = self.fetch_sorted_page(viewstate, viewstate_generator, aca_cs_field, session,
                                                specialty_value, specialty_name, "ctl00$PlaceHolderMain$refLicenseeList$gdvRefLicenseeList$ctl01$lnkBusiName2Header", "",
                                                )
            results, event_target, event_argument, page_text ,more_active_records  = self.parse_listing_page(page_content)

            if results:
                logger.info(f"Found Rows: {len(results)}")
                self.save_to_csv(results)
                rows_count += len(results)
            if not more_active_records:
                event_target = True

            page_num = 1
            # Covering Pagination
            while event_target:
                if not more_active_records:
                    logger.info("No more active records")
                    break
                page_num += 1
                viewstate, viewstate_generator, aca_cs_field = self.extract_meta_data(page_text)
                page_content = self.fetch_next_page(viewstate, viewstate_generator, aca_cs_field , session,
                                                    specialty_value, specialty_name, event_target, event_argument,page_num)

                results, event_target, event_argument , page_text,more_active_records  = self.parse_listing_page(page_content)
                if results:
                    logger.info(f"Found more rows: {len(results)}")
                    self.save_to_csv(results)
                    rows_count += len(results)


        else:
            logger.info(f"Page content not found: {specialty_name}")

        logger.info(f"Rows found for: {specialty_name} | {rows_count}")
        if session:
            session.close()


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
