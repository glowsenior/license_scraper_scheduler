#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import requests
import json
import itertools
import csv
import os
from time import sleep
from urllib.parse import urljoin
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class LicenseCrawler:
    def __init__(self, api_key="639d775e312601bdd68a44de0c235ac8"):
        """Initialize the crawler with configuration and session setup."""
        self.api_key = api_key
        self.base_url = "https://abme.igovsolution.net/online/Lookups/Individual_Lookup.aspx"
        self.verify_url = "https://abme.igovsolution.net/online/JS_grd/Grid.svc/Verifycaptcha"
        self.data_url = "https://abme.igovsolution.net/online/JS_grd/Grid.svc/GetIndv_license"
        self.session = self._initialize_session()
        self.processed_licenses = set()
        self.output_file = 'result/results.csv'
        os.makedirs('result', exist_ok=True)

    def _initialize_session(self):
        """Initialize and return a session with proper headers."""
        session = requests.Session()
        headers = {
            'accept': 'application/json, text/javascript, */*; q=0.01',
            'accept-language': 'en-US,en;q=0.9',
            'content-type': 'application/json; charset=UTF-8',
            'origin': 'https://abme.igovsolution.net',
            'referer': 'https://abme.igovsolution.net/online/Lookups/Individual_Lookup.aspx',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
            'x-requested-with': 'XMLHttpRequest'
        }
        session.headers.update(headers)
        return session

    def verify_captcha(self, captcha_solution):
        """Verify captcha solution with the server."""
        try:
            payload = {"resp": captcha_solution, "uip": ""}
            response = self.session.post(self.verify_url, json=payload)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Captcha verification failed: {e}")
            return None

    def solve_captcha(self, image_path):
        """Solve captcha using 2captcha service."""
        try:
            with open(image_path, 'rb') as captcha_file:
                response = requests.post(
                    "https://2captcha.com/in.php",
                    data={'key': self.api_key, 'method': 'post'},
                    files={'file': captcha_file}
                )
                if "OK" not in response.text:
                    raise Exception(f"Captcha submission failed: {response.text}")
                
                captcha_id = response.text.split('|')[1]
                
                for _ in range(10):
                    sleep(2)
                    answer = requests.get(
                        f"https://2captcha.com/res.php?key={self.api_key}&action=get&id={captcha_id}"
                    ).text
                    if "OK" in answer:
                        return answer.split('|')[1]
                    if "CAPCHA_NOT_READY" not in answer:
                        raise Exception(f"Captcha solution error: {answer}")
                
                raise Exception("Captcha solving timeout")
        except Exception as e:
            logger.error(f"Captcha solving failed: {e}")
            return None

    def get_verification_id(self):
        """Get verification ID after solving captcha."""
        try:
            self.session.get(self.base_url)
            captcha_img_url = urljoin(self.base_url, "../Captcha.aspx")
            captcha_response = self.session.get(captcha_img_url, stream=True)
            captcha_response.raise_for_status()

            with open('current_captcha.png', 'wb') as f:
                f.write(captcha_response.content)

            solution = self.solve_captcha('current_captcha.png')
            if not solution:
                return None

            verification_result = self.verify_captcha(solution)
            return verification_result['d'] if verification_result else None
        except Exception as e:
            logger.error(f"Verification ID retrieval failed: {e}")
            return None

    def fetch_license_data(self, fname, lname, l_type, page, vid):
        """Fetch license data for given parameters."""
        try:
            payload = {
                "lnumber": "",
                "lname": lname,
                "fname": fname,
                "lictype": l_type,
                "county": "-1",
                "vid": vid,
                "pageSize": 100,
                "page": page,
                "sortby": "",
                "sortexp": "",
                "sdata": []
            }
            response = self.session.post(self.data_url, json=payload)
            response.raise_for_status()
            return response.text
        except Exception as e:
            logger.error(f"License data fetch failed: {e}")
            return None

    def process_license_data(self, data):
        """Process and format license data, checking for duplicates."""
        try:
            formatted_data = {
                'Full_Name': f"{data['LastName']}, {data['First_Name']} {data['Middle_Name']}".strip(),
                'License_Type': data['License_Type'],
                'License_Number': data['Lic_no'].strip(),
                'Status': data['License_Status'],
                'Professional': '',
                'Issued': data['Issue_date'],
                'Expired': data['Expire_date']
            }
            
            # Check for duplicates using license number
            if formatted_data['License_Number'] not in self.processed_licenses:
                self.processed_licenses.add(formatted_data['License_Number'])
                return formatted_data
            return None
        except Exception as e:
            logger.error(f"Data processing failed: {e}")
            return None

    def run(self):
        """Run the crawler with error handling and duplicate checking."""
        headers = ['Full_Name', 'License_Type', 'License_Number', 'Status', 'Professional', 'Issued', 'Expired']
        combinations = [(chr(i), ''.join(x)) for i in range(65, 91) 
                       for x in itertools.product('ABCDEFGHIJKLMNOPQRSTUVWXYZ', repeat=2)]

        with open(self.output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()

            # Iterate through license types
            for license_type in ["1","2"]:
                logger.info(f"Processing license type: {license_type}")

                for fname, lname in combinations:
                    try:
                        logger.info(f"Processing: {fname} {lname} for type {license_type}")
                        vid = self.get_verification_id()
                        if not vid:
                            continue
                        self._process_name_combination(fname, lname, vid, writer, license_type)
                        sleep(1)
                    except Exception as e:
                        logger.error(f"Error processing {fname} {lname}: {e}")
                        continue

    def _process_name_combination(self, fname, lname, vid, writer, license_type):
        """Process a single name combination."""
        response = self.fetch_license_data(fname, lname, license_type, "1", vid)
        if not response:
            return

        parsed_data = json.loads(response)
        second_parse = json.loads(parsed_data['d'])
        record_count = second_parse['reccount']
        data = json.loads(second_parse['Response'])

        if data:
            self._write_license_data(data, writer)

            if record_count > 100:
                self._process_additional_pages(fname, lname, vid, record_count, writer, license_type)

    def _write_license_data(self, data, writer):
        """Write license data to CSV file."""
        for item in data:
            formatted_data = self.process_license_data(item)
            if formatted_data:
                writer.writerow(formatted_data)

    def _process_additional_pages(self, fname, lname, vid, record_count, writer, license_type):
        """Process additional pages of results."""
        total_pages = (record_count + 99) // 100
        for page in range(2, total_pages + 1):
            response = self.fetch_license_data(fname, lname, license_type, str(page), vid)
            if response:
                parsed_data = json.loads(response)
                additional_data = json.loads(json.loads(parsed_data['d'])['Response'])
                self._write_license_data(additional_data, writer)

if __name__ == "__main__":
    crawler = LicenseCrawler("639d775e312601bdd68a44de0c235ac8")
    crawler.run()

