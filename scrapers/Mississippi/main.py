import csv
import logging
import os
import re
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from datetime import datetime, date
import urllib3

# Suppress specific warnings
warnings.filterwarnings('ignore', category=urllib3.exceptions.InsecureRequestWarning)
from anticaptchaofficial.recaptchav2proxyless import *
from bs4 import BeautifulSoup

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class LicenseCrawler:

    def __init__(self):

        """Initialize the crawler."""
        self.base_url = 'https://gateway.msbml.ms.gov/verification/search.aspx'

        self.output_file = 'results/results.csv'
        self.csv_lock = Lock()
        proxy_url = "http://cruexuku-US-rotate:c3h2jphwjv7y@p.webshare.io:80"
        self.proxy = {
            "http": proxy_url,
            "https": proxy_url
        }

        self.headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Origin': 'https://gateway.msbml.ms.gov',
            'Referer': 'https://gateway.msbml.ms.gov/verification/search.aspx',
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

        # ['Medical Doctor', 'Osteopathic Physician']
        self.target_types = ['DOP', "MDP"]
        self.ANTI_CAPTCHA_API_KEY = 'fe348e4a8a96a206a483b6ea98ee3751'
        self.search_filter_list = []
        # for testing
        self.last_names_initials = list('ABCDEFGHIJKLMNOPQRSTUVWXYZ')
        self.records_cut_off = 250

        # Around 52
        for target_type in self.target_types:
            for last_name in self.last_names_initials:
                self.search_filter_list.append(
                    (last_name, target_type,))

        os.makedirs('results', exist_ok=True)

    def solve_captcha(self, site_key):
        """Solve captcha using site Key"""
        solver = recaptchaV2Proxyless()
        solver.set_verbose(1)
        solver.set_key(self.ANTI_CAPTCHA_API_KEY)
        solver.set_website_url(self.base_url)
        solver.set_website_key(site_key)
        # Specify softId to earn 10% commission with your app.
        # Get your softId here: https://anti-captcha.com/clients/tools/devcenter
        solver.set_soft_id(0)
        g_response = solver.solve_and_return_solution()
        if g_response != 0:
            pass
        else:
            logger.error("task finished with error " + solver.error_code)

            g_response = None
        return g_response

    def calculate_expiration_date(self):
        today = date.today()
        current_year = today.year
        cut_off_date_current_year = date(current_year, 6, 30)

        # Check if today is before or after June 30th
        if today <= cut_off_date_current_year:
            # Expiration date is June 30th of the current year
            expiration_date = cut_off_date_current_year
        else:
            # Expiration date is June 30th of the next year
            expiration_date = date(current_year + 1, 6, 30)

        return expiration_date.strftime("%m/%d/%Y")

    def fetch_initial_data(self, retries=3):
        """Fetch initial page data including cookies and hidden form fields."""

        session = payload = None

        while retries:
            retries -= 1
            try:
                response = requests.get(self.base_url, headers=self.headers, proxies=self.proxy)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, "html.parser")

                session_cookie = dict()
                for key, value in response.cookies.items():
                    session_cookie[key] = value

                script_tags = soup.find_all("script")
                # Extract the sitekey using regex
                data_sitekey = None
                for script in script_tags:
                    if script.string:  # Ensure the script has content
                        match = re.search(r'"sitekey":\s*\'(.*?)\'', script.string)
                        if match:
                            data_sitekey = match.group(1)
                            break

                __VIEWSTATE = soup.find("input", attrs={"id": "__VIEWSTATE"})["value"]
                __RequestVerificationToken = soup.find("input", attrs={"name": "__RequestVerificationToken"})["value"]
                __VIEWSTATEGENERATOR = soup.find("input", attrs={"id": "__VIEWSTATEGENERATOR"})["value"]
                __EVENTVALIDATION = soup.find("input", attrs={"id": "__EVENTVALIDATION"})["value"]

                if data_sitekey:
                    g_response = self.solve_captcha(data_sitekey)
                    if g_response:
                        session = requests.session()
                        session.headers.update(self.headers)
                        session.cookies.update(session_cookie)

                        payload = {
                            '__EVENTTARGET': '',
                            '__EVENTARGUMENT': '',
                            '__VIEWSTATE': __VIEWSTATE,
                            '__VIEWSTATEGENERATOR': __VIEWSTATEGENERATOR,
                            '__VIEWSTATEENCRYPTED': '',
                            '__EVENTVALIDATION': __EVENTVALIDATION,
                            '__RequestVerificationToken': __RequestVerificationToken,
                            'ctl00$Content$txtFirst': '',
                            'ctl00$Content$txtMiddle': '',

                            'ctl00$Content$txtCity': '',
                            'ctl00$Content$txtLicNum': '',

                            'ctl00$Content$ddLicStatus': 'A',
                            'g-recaptcha-response': g_response,
                            'ctl00$Content$btnSubmit': 'Search',
                        }

                        return session, payload
                    else:
                        raise Exception("Captcha Response not found")
                else:
                    raise Exception("data site key not found")


            except Exception as exp:
                logger.error(f"Retrying __ Error Fetching session: {exp}")
                time.sleep(1)

        return None

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

            # logger.info(f"Duplicates found: {duplicate_count}")

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

    def fetch_licensee_data(self, session, payload, last_name_initial, specialty_name, retries=3):
        """Submit the form for a given specialty and fetch the results page."""

        payload['ctl00$Content$ddLicType'] = specialty_name
        payload['ctl00$Content$txtLast'] = last_name_initial

        logger.info(f"Fetching listing page for filter {last_name_initial} {specialty_name}")

        while retries:
            retries -= 1
            try:

                response = session.post(self.base_url, data=payload, proxies=self.proxy)
                response.raise_for_status()
                return response.text
            except Exception as e:
                logger.error(f"Retrying __ Failed to fetch data for filter {last_name_initial} {specialty_name}: {e}")
                time.sleep(1)
        return None

    def parse_listing_page(self, html_content):
        """Extract active licence information and details"""
        result_set = []
        has_data = None
        soup = BeautifulSoup(html_content, "html.parser")
        has_data = True if soup.find("div", class_="panel-heading") and "Search Results" in soup.find("div",
                                                                                                      class_="panel-heading").text else False
        if not has_data:
            has_data = True if soup.find("div",
                                         class_="alert alert-danger") and " No match was found for the criteria you entered, please try again." in soup.find(
                "div",
                class_="alert alert-danger").text else False

        table_element = soup.find("table", class_="table table-hover")
        if table_element:
            table_rows = table_element.tbody.find_all("tr") if table_element.tbody else []

            for row in table_rows:
                row_tds = row.find_all("td")
                # print(len(row_tds))
                if len(row_tds) >= 7:
                    status = row_tds[3].text.strip()
                    # print(status)
                    if "Active" in status:
                        licence_number = row_tds[1].text.strip()
                        full_name = row_tds[0].text.strip()
                        license_type = row_tds[2].text.strip()
                        professional = license_type
                        # Construct the output dictionary
                        record = {
                            "Full_Name": full_name,
                            "License_Type": license_type,
                            "License_Number": licence_number,
                            "Issued": "",
                            "Expired": self.calculate_expiration_date(),
                            "Status": status,
                            "Professional": professional
                        }
                        result_set.append(record)
        else:
            logger.info("Table not found")

        return result_set, has_data

    def crawl_search_filter(self, search_filter_index, search_filter):
        """Crawl and process licensee data for a given search filter."""
        last_name_initial, speciality = search_filter
        logger.info(f"Crawling data for search filter#{search_filter_index} | {last_name_initial} {speciality}")
        session = payload = None
        results = {}
        retries = 5
        while retries:
            retries -= 1
            init_data = self.fetch_initial_data()
            if not init_data:
                logger.error(
                    f"Could not extract data to for search filter#{search_filter_index} | {last_name_initial} {speciality}")
                return

            session, payload = init_data

            page_content = self.fetch_licensee_data(session, payload, last_name_initial, speciality)

            if page_content:
                extracted_results, has_data = self.parse_listing_page(page_content)
                if not has_data:
                    logger.error(
                        f"Invalid Captcha, could not extract data to for search filter#{search_filter_index} | {last_name_initial} {speciality}")
                    time.sleep(20)
                    continue

                results[last_name_initial] = extracted_results
                result_count = len(extracted_results)
                logger.info(
                    f"Active licence numbers found: {result_count} for search filter#{search_filter_index} | {last_name_initial} {speciality}")

                # Criteria
                if result_count >= self.records_cut_off:
                    if len(last_name_initial) < 3:
                        next_prefixes = [last_name_initial + chr(ord('A') + i) for i in
                                         range(26)]  # Generating AA, AB, ..., AZ
                        for next_prefix in next_prefixes:
                            results.update(self.crawl_search_filter(
                                search_filter_index, (next_prefix, speciality)
                            ))

                if results:
                    for k, v in results.items():
                        self.save_to_csv(v)
                return results

            else:
                logger.info(
                    f"Page content not found. search filter#{search_filter_index} | {last_name_initial} {speciality}")

            if not retries:
                logger.error(
                    f"Could not solve captcha for search filter#{search_filter_index} | {last_name_initial} {speciality}")
                return
            else:
                time.sleep(5)

    def run(self):
        """Run the crawler concurrently for all search items."""

        # Write CSV headers
        with open(self.output_file, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ["Full_Name", "License_Type", "License_Number", "Status", "Professional", "Issued", "Expired"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

        # Process each specialty concurrently
        with ThreadPoolExecutor(max_workers=1) as executor:
            futures = {
                executor.submit(self.crawl_search_filter, search_filter_index, search_filter):
                    search_filter_index for search_filter_index, search_filter in
                enumerate(self.search_filter_list, start=1)
            }

            # Wait for all futures to complete
            for future in as_completed(futures):
                try:
                    #     # Optionally, you can get the result if needed
                    _ = future.result()  # This will raise an exception if the task failed
                except Exception as e:
                    search_filter_index = futures[future]
                    logger.error(f"Task generated an exception: {e} | {search_filter_index}")

            logger.info("Completed")
            logger.info("Removing duplicates if any.")
            self.remove_duplicates_in_csv()


if __name__ == '__main__':
    crawler = LicenseCrawler()
    crawler.run()
