#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import requests
from bs4 import BeautifulSoup
import csv
import time
import itertools
import logging
import os
import re

class LicenseCrawler:
    def __init__(self):
        self.url = "http://cgi.docboard.org/cgi-shl/nhayer.exe"
        self.output_file = 'result/result.csv'
        self.headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Origin': 'http://docfinder.docboard.org',
            'Referer': 'http://docfinder.docboard.org/',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
        }
        self.existing_records = set()
        self.csv_headers = ["Full_Name", "License_Type", "License_Number", "Status", "Professional", "Issued", "Expired"]
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    def get_initial_options(self, payload):
        try:
            response = requests.post(self.url, headers=self.headers, data=payload, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            options = soup.find_all('option')
            return [(option['value'], option.text.strip()) for option in options]
        except requests.RequestException as e:
            logging.error(f"Error fetching initial options: {e}")
            return []

    def get_doctor_details(self, payload, option_value):
        try:
            payload_dict = dict(item.split('=') for item in payload.split('&'))
            payload_dict['mednumb'] = option_value
            response = requests.post(self.url, headers=self.headers, data=payload_dict, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            table = soup.find('table')
            if table:
                rows = table.find_all('tr')
                return self.extract_doctor_details(rows)
        except requests.RequestException as e:
            logging.error(f"Error fetching doctor details for {option_value}: {e}")
        return None

    def extract_doctor_details(self, rows):
        for row in rows:
            cells = row.find_all('td')
            if len(cells) > 1:
                data = self.extract_first_six_td_data(cells[1])
                return self.extract_license_info(data)
        return None

    def extract_first_six_td_data(self, td):
        return re.sub(r'\s+', ' ', td.get_text().strip())

    def extract_license_info(self, data_string):
        extracted_data = {
            'Full_Name': '', 'License_Number': '', 'License_Type': '',
            'Status': '', 'Professional': '', 'Issued': '', 'Expired': ''
        }
        data_patterns = {
            'Full_Name': r'^([\w\s]+)License Number',
            'License_Number': r'License Number(\d+)',
            'License_Type': r'License Type([\w\s]+)License Status',
            'Status': r'License Status([\w\s]+)Original License',
            'Issued': r'Original License(\d{2}/\d{2}/\d{4})',
            'Expired': r'License Expiration Date(\d{2}/\d{2}/\d{4})'
        }
        for key, pattern in data_patterns.items():
            match = re.search(pattern, data_string)
            if match:
                extracted_data[key] = match.group(1).strip()
        return extracted_data

    def process_last_name(self, last_name):
        logging.info(f"Processing last name: {last_name}")
        payload = f'form_id=medname&state=mn&lictype=PY&medlname={last_name}&medfname=&med_town=&medlicno='
        options = self.get_initial_options(payload)
        new_records = []

        for option_value, option_text in options:
            details = self.get_doctor_details(payload, option_value)
            if details:
                record_tuple = (details['License_Number'], details['Issued'])
                if record_tuple not in self.existing_records:
                    new_records.append(details)
                    self.existing_records.add(record_tuple)
                    self.append_to_csv(details)

        return new_records

    def append_to_csv(self, record):
        file_exists = os.path.isfile(self.output_file)
        with open(self.output_file, mode='a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=self.csv_headers)
            if not file_exists:
                writer.writeheader()
            writer.writerow(record)

    def run(self):
        last_names = [''.join(i) for i in itertools.product('ABCDEFGHIJKLMNOPQRSTUVWXYZ', repeat=2)]
        total_new_records = 0

        for last_name in last_names:
            new_records = self.process_last_name(last_name)
            total_new_records += len(new_records)
            time.sleep(1)  # Add a delay to avoid overwhelming the server

        logging.info(f"Total new records added: {total_new_records}")

if __name__ == "__main__":
    crawler = LicenseCrawler()
    crawler.run()

