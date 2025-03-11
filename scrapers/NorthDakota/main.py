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
from anticaptchaofficial.recaptchav2proxyless import *
from bs4 import BeautifulSoup

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class LicenseCrawler:

    def __init__(self):

        """Initialize the crawler."""
        self.base_url = 'https://www.ndbom.org/public/find_verify/verify.asp'
        self.pagination_url = "https://www.ndbom.org/public/find_verify/verifyResults.asp"
        self.detail_url = "https://www.ndbom.org/public/find_verify/verifyDetails.asp"

        self.output_file = 'results/results.csv'
        self.csv_lock = Lock()

        self.headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Origin': 'https://www.ndbom.org',
            'Referer': 'https://www.ndbom.org/public/find_verify/verify.asp',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0',
            'sec-ch-ua': '"Microsoft Edge";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
        }
        self.target_types = ['Physician']
        self.ANTI_CAPTCHA_API_KEY = 'fe348e4a8a96a206a483b6ea98ee3751'
        self.search_filter_list = []

        for last_name in list('ABCDEFGHIJKLMNOPQRSTUVWXYZ'):
            self.search_filter_list.append(
                (last_name, 'Physician', 'MEMBER',))

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

    def fetch_initial_data(self, retries=3):
        """Fetch initial page data including cookies and hidden form fields."""

        __ncforminfo = session = data_sitekey = g_response = None

        while retries:
            retries -= 1
            try:
                response = requests.get(self.base_url, headers=self.headers, verify=False)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, "html.parser")

                session_cookie = dict()
                for key, value in response.cookies.items():
                    if "ASPSESSIONID" or "sessionID" in key:
                        session_cookie[key] = value

                data_site_key_div = soup.find("div", "g-recaptcha")
                if data_site_key_div:
                    data_sitekey = data_site_key_div["data-sitekey"]
                    __ncforminfo = soup.find("input", attrs={"name": "__ncforminfo"})["value"]

                    g_response = self.solve_captcha(data_sitekey)
                    if g_response:
                        session = requests.session()
                        session.headers.update(self.headers)
                        session.cookies.update(session_cookie)

                        return __ncforminfo, session, data_sitekey, g_response
                    else:
                        raise Exception("Captcha Response not found")
                else:
                    raise Exception("data site key not found")


            except Exception as exp:
                logger.error("Retrying __ Error Fetching session.")
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

    # Function to parse the detail page
    def parse_detail_page(self, html_content, professional):
        """Parse detail page information"""
        soup = BeautifulSoup(html_content, "html.parser")

        fields = {
            "Full_Name": None,
            "License_Type": None,
            "License_Number": None,
            "Issued": None,
            "Expired": None,
            "Status": None,
            "Professional": None,
        }
        verify_table = None
        # Extract from the first table
        try:
            verify_table = soup.find("table", {"id": "verifyDetails"})
            if verify_table:
                rows = verify_table.find_all("tr")
                if len(rows) > 1:  # Ensure there is at least one data row
                    first_row_data = rows[1].find_all("td")
                    fields["Full_Name"] = first_row_data[0].get_text(strip=True).replace("\xa0", " ") if len(
                        first_row_data) > 0 else None
                    fields["License_Type"] = first_row_data[1].get_text(strip=True) if len(first_row_data) > 1 else None
                    fields["License_Number"] = first_row_data[2].get_text(strip=True) if len(
                        first_row_data) > 2 else None
                    fields["Status"] = first_row_data[3].get_text(strip=True) if len(first_row_data) > 3 else None
                    fields["Professional"] = professional
        except Exception as e:
            logger.error(f"Error processing first table: {e}")

        # Extract from the second table
        try:
            if verify_table:
                license_table = verify_table.find_next("table", {"id": "verifyDetails"})
                rows = license_table.find_all("tr")
                if len(rows) > 1:  # Ensure there is at least one data row
                    first_row_data = rows[1].find_all("td")
                    fields["Issued"] = first_row_data[1].get_text(strip=True) if len(first_row_data) > 1 else None
                    fields["Expired"] = first_row_data[2].get_text(strip=True) if len(first_row_data) > 2 else None
        except Exception as e:
            logger.error(f"Error processing second table: {e}")

        return fields

    def fetch_licensee_data(self, __ncforminfo, session, _, g_response, last_name, search_type, retries=3):
        """Submit the form for a given specialty and fetch the results page."""

        data = {
            'action': 'verify',
            'licenseTyp': search_type,
            'g-recaptcha-response': g_response,
            'licenseSpec': '',
            'licenseNo': '',
            'lastName': last_name,
            'firstName': '',
            'city': '',
            '__ncforminfo': __ncforminfo,
        }
        logger.info(f"Fetching listing page=1 for filter {last_name} {search_type}")

        while retries:
            retries -= 1
            try:

                response = session.post(self.base_url, data=data)
                response.raise_for_status()
                return response.text
            except Exception as e:
                logger.error(f"Retrying __ Failed to fetch data for filter  {last_name} {search_type}: {e}")
                time.sleep(1)
        return None

    def fetch_next_licensee_data(self, next_page, next_page_index, session, last_name, search_type, retries=3):
        """Submit the form for a given specialty and fetch the results page."""

        data = {
            "start": f"{next_page}"
        }
        logger.info(f"Fetching listing page={next_page_index} for filter {last_name} {search_type}")

        while retries:
            retries -= 1
            try:

                response = session.post(self.pagination_url, data=data)
                response.raise_for_status()
                return response.text
            except Exception as e:
                logger.error(f"Retrying __ Failed to fetch data for filter  {last_name} {search_type}: {e}")
                time.sleep(1)
        return None

    def extract_page_number(self, soup):
        """ Extract total pages by parsing the HTML using BeautifulSoup"""
        # Find the <anchor> tag with text "Next"
        a_tag = soup.find('a', string="Next")
        if a_tag:
            # Use regex to extract the index from the href attribute
            match = re.search(r"goPage\('(\d+)'\)", a_tag['href'])
            if match:
                return int(match.group(1))  # Extract and convert to an integer
        return None  # Return None if not found

    def extract_index_from_html(self, soup):
        """Extract index for the detail page"""
        # Find the <td> tag with class "name"
        td = soup.find('td', class_='name')
        if td:
            # Find the <a> tag inside the <td>
            a_tag = td.find('a', href=True)
            if a_tag:
                # Use regex to extract the index from the href attribute
                match = re.search(r"details\('(\d+)'\)", a_tag['href'])
                if match:
                    return int(match.group(1))  # Extract and convert to an integer
        return None  # Return None if not found

    def parse_listing_page(self, html_content, is_initial_page=False):
        """Extract active licence information and total pages"""
        result_indexes = []
        is_result_page = False
        soup = BeautifulSoup(html_content, "html.parser")

        if is_initial_page:
            is_result_page = True if soup.find("h2", string="Search Results") else False
        next_page = self.extract_page_number(soup)

        table_element = soup.find("table", id="results")
        if table_element:
            rows = table_element.find("tbody").find_all("tr", class_=False) if table_element.find("tbody") else []
            for row in rows:
                row_data = row.find_all("td")
                status = row_data[-4].text.strip()
                if "Active" in status:
                    result_index = self.extract_index_from_html(row)
                    if result_index:
                        result_indexes.append(result_index)

        return result_indexes, next_page, is_result_page

    def fetch_the_target_licence_index(self, target_licence_index, row_index, last_name, search_type, session,
                                       retries=3):
        """Fetch detail page"""

        data = {
            "t": f"{target_licence_index}"
        }
        # logger.info(f"Fetching detail page={row_index} for filter {last_name} {search_type}")

        while retries:
            retries -= 1
            try:

                response = session.post(self.detail_url, data=data)
                response.raise_for_status()
                return response.text
            except Exception as e:
                logger.error(
                    f"Retrying __ Failed to fetch data for detail page={row_index} {last_name} {search_type}: {e}")
                time.sleep(1)
        return None

    def crawl_search_filter(self, search_filter_index, search_filter):
        """Crawl and process licensee data for a given search filter."""
        last_name, professional, search_type = search_filter
        logger.info(f"Crawling data for search filter#{search_filter_index} | {search_filter}")
        result_indexes = []
        results_ = []
        next_page = __ncforminfo = session = data_sitekey = g_response = None

        retries = 3
        while retries:
            retries -= 1
            init_data = self.fetch_initial_data()
            if not init_data:
                logger.error(f"Could not extract data to for search filter#{search_filter_index} | {search_filter}")
                return

            __ncforminfo, session, _, g_response = init_data

            page_content = self.fetch_licensee_data(__ncforminfo, session, _, g_response, last_name, search_type)

            if page_content:
                results_, next_page, is_result_page = self.parse_listing_page(page_content, is_initial_page=True)
                if is_result_page:
                    break


            else:
                logger.info(f"Page content not found. search filter#{search_filter_index} | value: {search_filter}")

            if not retries:
                logger.error(f"Could not solve captcha for search filter#{search_filter_index} | {search_filter}")
                return
            else:
                time.sleep(5)

        result_indexes.extend(results_)
        logger.info(f"Active licence numbers found till now: {len(result_indexes)}")
        next_page_index = 2
        while next_page:
            page_content = self.fetch_next_licensee_data(next_page, next_page_index, session, last_name,
                                                         search_type)
            results_, next_page, _ = self.parse_listing_page(page_content)

            result_indexes.extend(results_)
            logger.info(f"Active licence numbers found till now: {len(result_indexes)}")
            next_page_index += 1

        total_records = len(result_indexes)

        row_index = 1
        while result_indexes:
            target_licence_index = result_indexes.pop()
            logger.info(
                f"Processing license #{row_index}/({total_records})| License Index: {target_licence_index} | filter#{search_filter_index} | {search_filter}")
            page_content = self.fetch_the_target_licence_index(target_licence_index, row_index, last_name, search_type,
                                                               session)
            if page_content:
                record = self.parse_detail_page(page_content, professional)
                if record:
                    self.save_to_csv([record])
            row_index += 1

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
                    # Optionally, you can get the result if needed
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

# test, then apply return close on timeout full exception.
