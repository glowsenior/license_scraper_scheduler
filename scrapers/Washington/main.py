import requests
from bs4 import BeautifulSoup
import time
import os
from urllib.parse import urljoin
import csv
import string
import logging
from requests.exceptions import RequestException
from concurrent.futures import ThreadPoolExecutor
import threading


class LicenseCrawler:
    def __init__(self, api_key="bb743d81179f6439cb71e645192ad2cd", max_retries=3):
        self.base_url = "https://fortress.wa.gov/doh/providercredentialsearch/Default.aspx"
        self.api_key = api_key
        self.session = requests.Session()
        self.existing_data = set()
        self.max_retries = max_retries
        self.license_types = {
            "161": {"type": "DO", "professional": "Osteopathic Physician and Surgeon"},
            "167": {"type": "MD", "professional": "Physician and Surgeon"},
            "424": {"type": "MD", "professional": "Physician and Surgeon License Interstate Medical Licensure Compact"},
        }

        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        self.initialize_csv()

    def initialize_csv(self):
        """Create the result directory and initialize the CSV file with headers."""
        os.makedirs('result', exist_ok=True)
        if not os.path.exists('result/results.csv'):
            with open('result/results.csv', 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['Full_Name', 'License_Type', 'License_Number', 'Status', 'Professional', 'Issued',
                              'Expired']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()

    def solve_captcha(self, image_path):
        """Solve the CAPTCHA using the 2captcha service."""
        with open(image_path, 'rb') as captcha_file:
            files = {'file': captcha_file}
            response = requests.post(
                "https://2captcha.com/in.php",
                data={'key': self.api_key, 'method': 'post'},
                files=files
            )
            result = response.text
            if "OK" not in result:
                raise Exception(f"Error submitting CAPTCHA: {result}")
            captcha_id = result.split('|')[1]

            for _ in range(10):
                time.sleep(2)
                answer_response = requests.get(
                    f"https://2captcha.com/res.php?key={self.api_key}&action=get&id={captcha_id}"
                )
                answer_result = answer_response.text
                if "OK" in answer_result:
                    return answer_result.split('|')[1]
                elif "CAPCHA_NOT_READY" not in answer_result:
                    raise Exception(f"Error getting CAPTCHA answer: {answer_result}")
            raise Exception("Failed to solve CAPTCHA after multiple attempts")

    def submit_form(self, first_name, last_name, credential_type):
        """Submit the search form with the given parameters."""
        self.logger.info(
            f"\nGetting data for Last Name: {last_name}, First Name: {first_name}, Type: {credential_type}")

        for attempt in range(self.max_retries):
            try:
                response = self.session.get(self.base_url)
                response.raise_for_status()
                soup = BeautifulSoup(response.content, 'html.parser')

                captcha_img_rel_url = soup.find(id="c_default_ctl00_contentplaceholder_samplecaptcha_CaptchaImage")[
                    'src']
                captcha_img_url = urljoin(self.base_url, captcha_img_rel_url)
                captcha_img_response = self.session.get(captcha_img_url, stream=True)
                captcha_img_response.raise_for_status()

                with open('current_captcha.png', 'wb') as f:
                    f.write(captcha_img_response.content)

                captcha_solution = self.solve_captcha('current_captcha.png')

                data = {
                    "__EVENTTARGET": "",
                    "__EVENTARGUMENT": "",
                    "__VIEWSTATE": soup.find(id="__VIEWSTATE")['value'],
                    "__VIEWSTATEGENERATOR": soup.find(id="__VIEWSTATEGENERATOR")['value'],
                    "__SCROLLPOSITIONX": "0",
                    "__SCROLLPOSITIONY": "0",
                    "__VIEWSTATEENCRYPTED": "",
                    "__EVENTVALIDATION": soup.find(id="__EVENTVALIDATION")['value'],
                    "ctl00$ContentPlaceholder$CredentialAliasDropDownList": credential_type,
                    "ctl00$ContentPlaceholder$LastNameTextBox": last_name,
                    "ctl00$ContentPlaceholder$FirstNameTextBox": first_name,
                    "ctl00$ContentPlaceholder$ExactNameRadioButtonList": "False",
                    "BDC_VCID_c_default_ctl00_contentplaceholder_samplecaptcha": soup.find(
                        id="BDC_VCID_c_default_ctl00_contentplaceholder_samplecaptcha")['value'],
                    "BDC_BackWorkaround_c_default_ctl00_contentplaceholder_samplecaptcha": "1",
                    "ctl00$ContentPlaceholder$CaptchaCodeTextBox": captcha_solution,
                    "ctl00$ContentPlaceholder$SearchButton": "Search"
                }

                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Cache-Control": "max-age=0",
                    "Connection": "keep-alive",
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Origin": "https://fortress.wa.gov",
                    "Referer": self.base_url,
                }

                result_response = self.session.post(self.base_url, data=data, headers=headers)
                result_response.raise_for_status()
                soup = BeautifulSoup(result_response.content, 'html.parser')

                show_all_link = soup.find(id="ctl00_ContentPlaceholder_ResultsGridView_ctl01_ShowAllLinkButton")
                if show_all_link:
                    show_all_data = {
                        "__EVENTTARGET": "ctl00$ContentPlaceholder$ResultsGridView$ctl01$ShowAllLinkButton",
                        "__EVENTARGUMENT": "",
                        "__VIEWSTATE": soup.find(id="__VIEWSTATE")['value'],
                        "__VIEWSTATEGENERATOR": soup.find(id="__VIEWSTATEGENERATOR")['value'],
                        "__SCROLLPOSITIONX": "0",
                        "__SCROLLPOSITIONY": soup.find(id="__SCROLLPOSITIONY")['value'] if soup.find(
                            id="__SCROLLPOSITIONY") else "0",
                        "__VIEWSTATEENCRYPTED": "",
                        "__EVENTVALIDATION": soup.find(id="__EVENTVALIDATION")['value'],
                    }

                    show_all_response = self.session.post(self.base_url, data=show_all_data, headers=headers)
                    show_all_response.raise_for_status()
                    soup = BeautifulSoup(show_all_response.content, 'html.parser')

                return self.extract_table_data(soup, credential_type)

            except RequestException as e:
                self.logger.error(f"Request failed (attempt {attempt + 1}/{self.max_retries}): {str(e)}")
                if attempt == self.max_retries - 1:
                    self.logger.error(f"Max retries reached. Moving to next name combination.")
                    return None
                time.sleep(5)
            except Exception as e:
                self.logger.error(f"An error occurred: {str(e)}")
                return None

    def extract_table_data(self, soup, credential_type):
        """Extract data from the result table."""
        table = soup.find(id="ctl00_ContentPlaceholder_ResultsGridView")
        if table:
            headers = [th.text for th in table.find_all('th')]
            data = []
            credentials_to_fetch = []

            rows = table.find_all('tr')[2:]  # Skip the first two rows
            total_records = len(rows)
            self.logger.info(f"Total records found: {total_records}")

            for row in rows:
                cells = row.find_all('td')
                if len(cells) == len(headers):
                    row_data = {}
                    for header, cell in zip(headers, cells):
                        if header == "Credential":
                            credential_link = cell.find('a')
                            if credential_link:
                                row_data[header] = credential_link.text
                                credentials_to_fetch.append((credential_link['id'], row_data))
                        else:
                            row_data[header] = cell.text.strip()

                    unique_id = f"{row_data['Credential']}_{row_data['Last Name']}_{row_data['First Name']}"
                    if unique_id not in self.existing_data:
                        self.existing_data.add(unique_id)
                        row_data['Credential_Type'] = credential_type
                        data.append(row_data)

            if credentials_to_fetch:
                self.fetch_first_issue_dates_concurrent(soup, credentials_to_fetch, total_records)

            return data
        return None

    def fetch_first_issue_dates_concurrent(self, soup, credentials_to_fetch, total_records, max_workers=10):
        """Fetch first issue dates for multiple credentials concurrently."""
        processed_count = 0
        lock = threading.Lock()

        def worker(credential_info):
            nonlocal processed_count
            control_id, row_data = credential_info
            try:
                first_issue_date = self.get_details_page_data(soup, control_id)
                if first_issue_date:
                    row_data['Issued'] = first_issue_date

                with lock:
                    nonlocal processed_count
                    processed_count += 1
                    self.logger.info(f"Scrapped data: {processed_count}/{total_records}")

            except Exception as e:
                self.logger.error(f"Error fetching details for {row_data['Credential']}: {str(e)}")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            executor.map(worker, credentials_to_fetch)

    def get_details_page_data(self, soup, control_id):
        """Fetch first issue date from the details page."""
        try:
            control_num = control_id.split('_')[3]
            event_target = f"ctl00$ContentPlaceholder$ResultsGridView${control_num}$CredentialNumber"

            data = {
                "__EVENTTARGET": event_target,
                "__EVENTARGUMENT": "",
                "__VIEWSTATE": soup.find(id="__VIEWSTATE")['value'],
                "__VIEWSTATEGENERATOR": soup.find(id="__VIEWSTATEGENERATOR")['value'],
                "__SCROLLPOSITIONX": "0",
                "__SCROLLPOSITIONY": soup.find(id="__SCROLLPOSITIONY")['value'] if soup.find(
                    id="__SCROLLPOSITIONY") else "0",
                "__VIEWSTATEENCRYPTED": "",
                "__EVENTVALIDATION": soup.find(id="__EVENTVALIDATION")['value']
            }

            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                "Accept-Language": "en-US,en;q=0.9",
                "Content-Type": "application/x-www-form-urlencoded",
                "Origin": "https://fortress.wa.gov",
                "Referer": self.base_url
            }

            response = self.session.post(self.base_url, data=data, headers=headers)
            response.raise_for_status()

            details_soup = BeautifulSoup(response.content, 'html.parser')
            details_table = details_soup.find(id="ctl00_ContentPlaceholder_DetailsGridView")

            if details_table:
                data_row = details_table.find_all('tr')[1]
                if data_row:
                    first_issue_date = data_row.find_all('td')[2].text.strip()
                    return first_issue_date
            return None
        except Exception as e:
            self.logger.error(f"Error fetching details page data: {str(e)}")
            return None

    def append_results_to_csv(self, results, credential_type):
        """Append results to CSV file."""
        with open('result/results.csv', 'a', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['Full_Name', 'License_Type', 'License_Number', 'Status', 'Professional', 'Issued', 'Expired']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            for row in results:
                full_name = f"{row['First Name']} {row['Middle Name or Initial']} {row['Last Name']}".strip()
                writer.writerow({
                    'Full_Name': full_name,
                    'License_Type': self.license_types[credential_type]['type'],
                    'License_Number': row['Credential'],
                    'Status': row['Credential Status'],
                    'Professional': self.license_types[credential_type]['professional'],
                    'Issued': row.get('Issued', ''),
                    'Expired': row['CE Due Date']
                })

    def run(self):
        """Main method to run the crawler."""
        for last_name in string.ascii_uppercase:
            for first_name in string.ascii_uppercase:
                for credential_type in self.license_types.keys():
                    results = self.submit_form(first_name, last_name, credential_type)
                    if results:
                        self.append_results_to_csv(results, credential_type)
                    time.sleep(1)


if __name__ == "__main__":
    crawler = LicenseCrawler()
    crawler.run()