## For 3 type options

import requests
from bs4 import BeautifulSoup
import re
import csv
import time
import logging
import os
from datetime import datetime
import base64
import json


class LicenseCrawler:
    def __init__(self):
        self.base_url = "https://ebizws.mt.gov/PUBLICPORTAL/"
        self.output_file = 'result/result.csv'
        self.headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"'
        }
        # Save csv headers
        self.csv_headers = ["Full_Name", "License_Type", "License_Number", "Status", "Professional", "Issued",
                            "Expired"]
        # License Types
        self.license_types = [
            "Medical%20Examiners%20Medical%20Doctor%20Compact%20License",
            "Medical%20Examiners%20Medical%20Doctor%20License",
            "Medical%20Examiners%20Telemed%20License"
        ]
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        os.makedirs(os.path.dirname(self.output_file), exist_ok=True)

    # function that downloads captcha image
    def get_and_solve_captcha(self, session):
        verify_url = f"{self.base_url}verif"
        response = session.get(verify_url, headers=self.headers)
        soup = BeautifulSoup(response.text, 'html.parser')
        img_tag = soup.find('img', src=re.compile(r'^data:image'))

        if img_tag:
            image_data = img_tag['src']
            base64_data = image_data.split(',')[1]
            with open('captcha.png', 'wb') as f:
                f.write(base64.b64decode(base64_data))

            return self.solve_captcha('captcha.png')
        else:
            raise Exception("Captcha image not found")

    # function that use 2captcha api to solve downloaded captcha image
    def solve_captcha(self, image_path):
        api_key = "639d775e312601bdd68a44de0c235ac8"

        with open(image_path, 'rb') as captcha_file:
            files = {'file': captcha_file}
            response = requests.post(
                "https://2captcha.com/in.php",
                data={'key': api_key, 'method': 'post'},
                files=files
            )
            result = response.text
            if "OK" not in result:
                raise Exception(f"Error submitting CAPTCHA: {result}")
            captcha_id = result.split('|')[1]

        for _ in range(10):
            time.sleep(5)
            answer_response = requests.get(
                f"https://2captcha.com/res.php?key={api_key}&action=get&id={captcha_id}"
            )
            answer_result = answer_response.text
            if "OK" in answer_result:
                return answer_result.split('|')[1]
            elif "CAPCHA_NOT_READY" not in answer_result:
                raise Exception(f"Error getting CAPTCHA answer: {answer_result}")

        raise Exception("Failed to solve CAPTCHA after multiple attempts")

    # Function to send solved captcha request
    def post_solved_captcha(self, session, solution):
        verify_url = f"{self.base_url}verif"
        payload = f"userVerif={solution}"
        headers = self.headers.copy()
        headers.update({
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'X-Requested-With': 'XMLHttpRequest'
        })
        response = session.post(verify_url, headers=headers, data=payload)
        return response.text == "success"

    # Function that gets license data as json.
    def get_license_data(self, session, license_type):
        search_url = f"{self.base_url}searchform"
        payload = f'licboard=Medical%20Examiners&lictype={license_type}&licnumber=&firstname=&lastname=&businessname=&city=&state=&zip=&country=&mylist=license'
        headers = self.headers.copy()
        headers.update({
            'Content-Type': 'application/x-www-form-urlencoded'
        })
        response = session.post(search_url, headers=headers, data=payload)
        soup = BeautifulSoup(response.text, 'html.parser')
        script_tag = soup.find('script', text=re.compile(r'var data ='))

        if script_tag:
            json_data = re.search(r'var data = (\[.*?\]);', script_tag.string, re.DOTALL)
            if json_data:
                return json.loads(json_data.group(1))

        raise Exception(f"License data not found for license type: {license_type}")

    # Function that saves json data with respective headers.
    def get_license_details(self, session, url):
        response = session.get(url, headers=self.headers)
        soup = BeautifulSoup(response.text, 'html.parser')

        license_info = soup.find('fieldset').find_all('span')
        license_holder = soup.find_all('fieldset')[1].find_all('span')

        record = {
            'Full_Name': license_holder[1].text.split('Name: ')[1].strip(),
            'License_Type': license_info[1].text.split('License Type: ')[1].strip(),
            'License_Number': license_info[2].text.split('License Number: ')[1].strip(),
            'Status': license_info[3].text.split('License Status: ')[1].strip(),
            'Professional': license_info[1].text.split('License Type: ')[1].strip(),
            'Issued': license_info[5].text.split('License Issued Date: ')[1].strip(),
            'Expired': license_info[4].text.split('License Expiration Date: ')[1].strip()
        }
        print(record)
        return record

    def save_to_csv(self, data):
        """
        Save a single record to the CSV file. Avoids writing duplicate rows.
        """
        try:
            # Check if the file exists
            file_exists = os.path.isfile(self.output_file)

            # Read existing rows into a set of unique identifiers (e.g., License_Number)
            existing_rows = set()
            if file_exists:
                with open(self.output_file, 'r', encoding='utf-8') as csvfile:
                    reader = csv.DictReader(csvfile)
                    for row in reader:
                        # Assuming 'License_Number' is a unique identifier
                        existing_rows.add(row['License_Number'])

            # Only write if the record is new (not in existing_rows)
            if data['License_Number'] not in existing_rows:
                with open(self.output_file, 'a', newline='', encoding='utf-8') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=self.csv_headers)
                    # Write header if file is empty or newly created
                    if not file_exists or os.stat(self.output_file).st_size == 0:
                        writer.writeheader()
                    writer.writerow(data)
        except PermissionError:
            logging.error(f"File {self.output_file} is open. Could not write data.")

    def run(self):
        session = requests.Session()

        # try solving captcha
        try:
            # call captcha function
            captcha_solution = self.get_and_solve_captcha(session)
            if self.post_solved_captcha(session, captcha_solution):
                # Loop through different types of licenses.
                for license_type in self.license_types:
                    logging.info(f"Fetching data for license type: {license_type}")
                    try:
                        license_data = self.get_license_data(session, license_type)
                        print(f"Total licenses for {license_type}: {len(license_data)}")

                        for license_info in license_data:
                            record = {
                                'Full_Name': f"{license_info.get('firstName', '')} {license_info.get('lastName', '')}".strip(),
                                'License_Type': license_info.get('recordAlias', ''),
                                'License_Number': re.search(r">([^<]+)</a>",
                                                            license_info.get('licenseNumber', '')).group(
                                    1) if license_info.get('licenseNumber') else '',
                                'Status': license_info.get('licenseStatus', ''),
                                'Professional': license_info.get('recordAlias', ''),
                                'Issued': '',  # This information might not be available in the initial data
                                'Expired': license_info.get('expDate', '')
                            }

                            self.save_to_csv(record)

                    except Exception as e:
                        logging.error(f"Error fetching data for {license_type}: {str(e)}")

                logging.info("Crawling completed successfully.")
            else:
                logging.error("Captcha verification failed.")
        except Exception as e:
            logging.error(f"An error occurred: {str(e)}")


if __name__ == "__main__":
    crawler = LicenseCrawler()
    crawler.run()