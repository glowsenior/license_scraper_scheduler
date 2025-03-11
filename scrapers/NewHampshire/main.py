import csv
import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
import js2py
import requests
from bs4 import BeautifulSoup


class LicenseCrawler:
    def __init__(self):
        self.processed_license_numbers = set()
        self.csv_lock = Lock()
        self.profession_name = 'Medicine'
        self.license_type_names = ['Physician', 'Compact Physician', 'Physician Assistant']
        self.output_file = 'results/results.csv'
        os.makedirs('results', exist_ok=True)

    def extract_and_compute_cookie(self, js_code):
        """ Extracting cookie from JS code"""
        context = js2py.EvalJs()
        modified_js_code = js_code.replace(
            '{ document.cookie="KEY=',
            'cookie = "KEY='
        ).replace(
            'document.location.reload(true); }',
            'return cookie;'
        )

        try:
            context.execute(modified_js_code)
            key_value = context.go()
            return key_value
        except Exception as e:
            logger.error("Error executing JavaScript code:", e)
            return None

    def visit_site(self, session):
        """Fetching home page in order to start a session and extract necessary meta data"""
        try:
            url = 'https://forms.nh.gov/licenseverification/Search.aspx'
            headers = {
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'accept-language': 'en-US,en;q=0.9,ko;q=0.8',
                'cache-control': 'max-age=0',
                'referer': 'https://forms.nh.gov/licenseverification/ErrorPage.html?aspxerrorpath=/licenseverification/Search.aspx',
                'sec-ch-ua': '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"',
                'sec-fetch-dest': 'document',
                'sec-fetch-mode': 'navigate',
                'sec-fetch-site': 'same-origin',
                'sec-fetch-user': '?1',
                'upgrade-insecure-requests': '1',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36'
            }

            response = session.get(url, headers=headers)
            response = self.check_redirect(session, url, headers, response)
            if response.status_code != 200:
                raise Exception(f"Initial request failed with status code {response.status_code}")

            viewstate, eventvalidation, view_state_generator = self.extract_form_data(response.text)
            return viewstate, eventvalidation, view_state_generator
        except Exception as e:
            print(f"Error in visit_site: {e}")
            raise

    def run_home_page_search(self, session, viewstate, eventvalidation, view_state_generator, license_type_name="",
                             license_no=""):
        """Fetching first page results using license type or and license number depending upon the call."""
        url = 'https://forms.nh.gov/licenseverification/Search.aspx'
        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-US,en;q=0.9,ko;q=0.8',
            'cache-control': 'max-age=0',
            'referer': 'https://forms.nh.gov/licenseverification/Search.aspx',
            'sec-ch-ua': '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36'
        }

        data = {
            '__EVENTTARGET': '',
            '__EVENTARGUMENT': '',
            '__LASTFOCUS': '',
            '__VIEWSTATE': viewstate,
            '__VIEWSTATEGENERATOR': view_state_generator,
            '__EVENTVALIDATION': eventvalidation,
            't_web_lookup__profession_name': self.profession_name,
            't_web_lookup__license_type_name': license_type_name,
            't_web_lookup__first_name': '',
            't_web_lookup__last_name': ' ',
            't_web_lookup__license_no': license_no,
            'sch_button': 'Search'
        }

        retries = 3
        while retries:
            retries -= 1
            try:
                response = session.post(url, headers=headers, data=data)
                if response.status_code != 200:
                    raise Exception(f"Search failed with status code {response.status_code}")

                viewstate, eventvalidation, view_state_generator = self.extract_form_data(response.text)
                return viewstate, eventvalidation, view_state_generator, response.text
            except Exception as e:
                print(f"Retrying __ Error during search operation: {e}")
                time.sleep(1)
        raise

    def navigate_to_listing_page(self, session, viewstate, eventvalidation, viewstate_generator, target, page_number):
        """Fetching next page in the pagination"""

        logger.info(f"Navigate to Listing Page: {page_number}")

        try:
            url = 'https://forms.nh.gov/licenseverification/SearchResults.aspx'
            headers = {
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'accept-language': 'en-US,en;q=0.9,ko;q=0.8',
                'cache-control': 'max-age=0',
                'content-type': 'application/x-www-form-urlencoded',
                'origin': 'https://forms.nh.gov',
                'referer': 'https://forms.nh.gov/licenseverification/SearchResults.aspx',
                'sec-ch-ua': '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"',
                'sec-fetch-dest': 'document',
                'sec-fetch-mode': 'navigate',
                'sec-fetch-site': 'same-origin',
                'upgrade-insecure-requests': '1',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
            }

            data = {
                'CurrentPageIndex': str(page_number - 1),
                '__EVENTTARGET': f'datagrid_results$_ctl44$_ctl{target}',
                '__EVENTARGUMENT': '',
                '__VIEWSTATE': viewstate,
                '__VIEWSTATEGENERATOR': viewstate_generator,
                '__EVENTVALIDATION': eventvalidation,
            }

            max_retries = 3
            for attempt in range(max_retries):
                try:
                    response = session.post(url, headers=headers, data=data)
                    if response.status_code == 200:
                        response = self.check_redirect(session, url, headers, response, data)
                        if response.status_code == 200:
                            viewstate, eventvalidation, view_state_generator = self.extract_form_data(response.text)
                            return viewstate, eventvalidation, view_state_generator, response.text
                        else:
                            print(f"Redirect check failed with status code {response.status_code}. Retrying...")
                    else:
                        print(f"POST request failed with status code {response.status_code}. Retrying...")

                except requests.exceptions.RequestException as e:
                    print(f"Network error on attempt {attempt + 1}: {e}")
                except Exception as e:
                    print(f"Unexpected error on attempt {attempt + 1}: {e}")

                if attempt == max_retries - 1:
                    print("Max retries reached. Exiting...")
                    return None
        except Exception as e:
            print(f"Error during search page navigation: {e}")
            return None

    def get_detail_page(self, session, url):
        """Fecthing detail page of a licensee using url"""

        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-US,en;q=0.9,ko;q=0.8',
            'cache-control': 'max-age=0',
            'priority': 'u=0, i',
            'referer': 'https://forms.nh.gov/licenseverification/SearchResults.aspx',
            'sec-ch-ua': '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36'
        }
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = session.post(url, headers=headers)
                if response.status_code == 200:
                    response = self.check_redirect(session, url, headers, response)
                    if response.status_code == 200:
                        return response.text
                    else:
                        print(f"Redirect check failed with status code {response.status_code}. Retrying...")
                else:
                    print(f"POST request failed with status code {response.status_code}. Retrying...")
            except requests.exceptions.RequestException as e:
                print(f"Network error on attempt {attempt + 1}: {e}")
            except Exception as e:
                print(f"Unexpected error on attempt {attempt + 1}: {e}")

            if attempt == max_retries - 1:
                print("Max retries reached. Exiting...")
                return None

    def check_redirect(self, session, url, headers, response, data=None):
        """ Check if a redirect is required by identifying javascript function call in exting html and redirecting to the original URL with newly found cookie"""
        try:
            soup = BeautifulSoup(response.text, 'html.parser')
            js_code = None
            for script_tag in soup.find_all('script'):
                if script_tag.string and 'function go()' in script_tag.string:
                    js_code = script_tag.string
                    break

            if js_code is None:
                logger.info("JavaScript code with the function 'go' was not found. --- Skipping redirection check")
                return response

            key_cookie = self.extract_and_compute_cookie(js_code)
            if key_cookie:
                session.cookies.set('KEY', key_cookie.split(';')[0].split('=')[1])
                if not data:
                    redirect_response = session.post(url, headers=headers)
                else:
                    redirect_response = session.post(url, headers=headers, data=data)
                return redirect_response
            return response
        except Exception as e:
            logger.error(f"Error during redirect check: {e}")
            return response

    def extract_form_data(self, html_content):
        """ Extracting form meta data essential for the session to continue proceedings"""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            viewstate = soup.find('input', {'id': '__VIEWSTATE'})['value']
            eventvalidation = soup.find('input', {'id': '__EVENTVALIDATION'})['value']
            view_state_generator = soup.find('input', {'id': '__VIEWSTATEGENERATOR'})['value']
            return viewstate, eventvalidation, view_state_generator
        except Exception as e:
            print(f"Error extracting form data: {e}")
            raise

    def parse_listing_page(self, html_content, expected_listing_page_num):
        """ Extracting Active license numbers from the listing page html and the next page target event value of pagination"""

        results = set()
        target_for_the_next_page = None

        soup = BeautifulSoup(html_content, 'html.parser')
        table_elements = soup.select("#datagrid_results > tr:not(:last-child, :first-child)")
        for row in table_elements:
            cells = row.find_all(['td'])
            if len(cells) >= 5:
                license_type = cells[2].get_text(strip=True)
                license_number = cells[3].get_text(strip=True)
                status = cells[4].get_text(strip=True)
                if status and "Active" in status:
                    results.add((license_number, license_type,))

        pagination_tr = soup.find("tr", attrs={"style": "color:Black;background-color:#F2F2F5;"})
        if pagination_tr:
            # check pagination
            # get the target , if page num matches, else, check if ... having text is in the last td, extract it's target.
            a_tags = pagination_tr.find_all('a', href=True)
            if a_tags:
                for a_tag in a_tags:
                    if a_tag.text.strip() == str(expected_listing_page_num):
                        href_value = a_tag.get('href')

                        # Parse the value inside __doPostBack
                        match = re.search(r"__doPostBack\('(.*?)','(.*?)'\)", href_value)
                        if match:
                            target_for_the_next_page = match.group(1)

                if not target_for_the_next_page:
                    # check for the next_window a tag with text == '...'
                    if a_tags[-1].text.strip() == "...":
                        href_value = a_tags[-1].get('href')

                        # Parse the value inside __doPostBack
                        match = re.search(r"__doPostBack\('(.*?)','(.*?)'\)", href_value)
                        if match:
                            target_for_the_next_page = match.group(1)

        return list(results), target_for_the_next_page

    def replace_multiple_whitespace(self, text):
        """ Replace consecutive spaces in full name if any"""

        # Replace multiple whitespaces with a single space
        return re.sub(r'\s+', ' ', text).strip()

    def parse_detail_page(self, html_content, license_number):
        """ Extract details of each lincense row from the html page"""

        soup = BeautifulSoup(html_content, "html.parser")
        row_data = {}

        full_name = soup.find('span', id='_ctl31__ctl1_full_name')
        row_data['Full_Name'] = self.replace_multiple_whitespace(full_name.get_text().strip()) if full_name else ''

        license_type = soup.find('span', id='_ctl43__ctl1_license_type')
        row_data['License_Type'] = license_type.get_text().strip() if license_type else ''

        row_data["License_Number"] = license_number

        issued = soup.find('span', id='_ctl43__ctl1_issue_date')
        row_data['Issued'] = issued.get_text().strip('"').strip() if issued else ''

        expired = soup.find('span', id='_ctl43__ctl1_expiration_date')
        row_data['Expired'] = expired.get_text().strip('"').strip() if expired else ''
        license_status = soup.find('span', id='_ctl43__ctl1_sec_lic_status')
        row_data['Status'] = license_status.get_text().strip() if license_status else ''

        professional = soup.find('span', id='_ctl43__ctl1_profession_id')
        row_data['Professional'] = professional.get_text().strip() if professional else ''

        return row_data

    def save_to_csv(self, results):
        """Save results to CSV in a thread-safe manner."""
        with self.csv_lock:
            with open(self.output_file, 'a', newline='', encoding='utf-8') as csvfile:
                fieldnames = ["Full_Name", "License_Type", "License_Number", "Status", "Professional", "Issued",
                              "Expired"]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                for result in results:
                    writer.writerow(result)

    def process_license_number(self, index, l_num, total_count):
        """Process license number by searching the website with the correspodning license type and extracting the detail page url"""

        license_number, license_type = l_num
        logger.info(f"Processing license #{index}/({total_count}) | {license_number}")
        session = requests.Session()
        try:
            viewstate, event_validation, view_state_generator = self.visit_site(session)
            _, __, ___, page_content = self.run_home_page_search(session, viewstate, event_validation,
                                                                 view_state_generator, license_type, license_number)

            detail_page_url = ""
            soup = BeautifulSoup(page_content, "html.parser")
            table_elements = soup.select("#datagrid_results > tr:not(:last-child, :first-child)")

            base_url = "https://forms.nh.gov/licenseverification/"

            for row in table_elements:
                cells = row.find_all(['td'])
                if len(cells) >= 5:
                    name_cell = cells[0].find('a')
                    detail_page_url = base_url + name_cell['href'] if name_cell else None
                    break

            if not detail_page_url:
                logger.error(f"License Number {license_number} not found.")
                session.close()
                return None
            detail_page_content = self.get_detail_page(session, detail_page_url)
            if detail_page_content:
                res = self.parse_detail_page(detail_page_content, license_number)
                if res:
                    self.save_to_csv([res])

        except Exception as e:
            print(f"Error in process_license_number({license_number}): {e}")
            session.close()
            raise

        session.close()

    def run(self):
        """Run the crawler over all the pages of license type names."""
        license_nums = []
        # make a session which do search

        for index, license_type_name in enumerate(self.license_type_names, start=1):
            logger.info(f"Processing license type: #{index} | {license_type_name}")

            page_num = 1
            session = requests.Session()
            logger.info("Started Scraping")
            viewstate, eventvalidation, view_state_generator = self.visit_site(session)
            logger.info("Running Home Page Search")
            viewstate, eventvalidation, view_state_generator, page_content = self.run_home_page_search(session,
                                                                                                       viewstate,
                                                                                                       eventvalidation,
                                                                                                       view_state_generator,
                                                                                                       license_type_name)
            results, target = self.parse_listing_page(page_content, page_num + 1)
            logger.info(f"Found Active licenses: {len(results)}")
            license_nums.extend(results)

            while target:
                page_num += 1

                viewstate, eventvalidation, view_state_generator, page_content = self.navigate_to_listing_page(session,
                                                                                                               viewstate,
                                                                                                               eventvalidation,
                                                                                                               view_state_generator,
                                                                                                               target,
                                                                                                               page_num)
                results, target = self.parse_listing_page(page_content, page_num + 1)
                logger.info(f"Found More Active licenses: {len(results)}")
                license_nums.extend(results)

            session.close()

            license_nums = list(set(license_nums))
            logger.info(f"Total Active Licenses Found Are: {len(license_nums)}")

        total_count = len(license_nums)

        # Apply threading, max_workers = 2
        # Write CSV headers
        with open(self.output_file, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ["Full_Name", "License_Type", "License_Number", "Status", "Professional", "Issued", "Expired"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

        license_nums_left_for_rerun = []
        # Process each specialty concurrently
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = {
                executor.submit(self.process_license_number, index, l_num, total_count): (
                    index, l_num) for index, l_num in enumerate(license_nums, start=1)
            }

            # Wait for all futures to complete
            for future in as_completed(futures):
                index, l_num = futures[future]
                try:
                    # Optionally, you can get the result if needed
                    result = future.result()  # This will raise an exception if the task failed
                except Exception as e:
                    print(f"Recorded for Retry __ Task generated an exception: {e} | {l_num}")
                    license_nums_left_for_rerun.append(l_num)

        total_count = len(license_nums_left_for_rerun)
        logger.info(f"Retrying Remaining License nums. Total are: {total_count}")

        # Process each specialty concurrently
        with ThreadPoolExecutor(max_workers=1) as executor:
            futures = {
                executor.submit(self.process_license_number, index, l_num, total_count): (
                    index, l_num) for index, l_num in enumerate(license_nums_left_for_rerun, start=1)
            }

            # Wait for all futures to complete
            for future in as_completed(futures):
                index, l_num = futures[future]
                try:
                    # Optionally, you can get the result if needed
                    result = future.result()  # This will raise an exception if the task failed
                except Exception as e:
                    print(f"Task generated an exception: {e} | {l_num}")


if __name__ == "__main__":
    crawler = LicenseCrawler()
    crawler.run()
