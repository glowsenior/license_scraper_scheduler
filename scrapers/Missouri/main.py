import requests
from bs4 import BeautifulSoup
import csv
import time
import logging
import os
import re
from datetime import datetime


class LicenseCrawler:
    """
    A web crawler to scrape license details for professionals from the Missouri licensing website.
    It saves the extracted data to a CSV file and manages retries for robust operation.
    """

    def __init__(self):
        # Base URL for the Missouri license search results page
        self.base_url = "https://pr.mo.gov/licensee-search-results.asp"
        
        # Filepath to save the scraped results
        self.output_file = 'result/result.csv'
        
        # HTTP headers to mimic a browser and avoid being flagged as a bot
        self.headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Referer': 'https://pr.mo.gov/licensee-search-division.asp',
            'Origin': 'https://pr.mo.gov'
        }
        
        # Set to track already processed records to avoid duplication
        self.existing_records = set()
        
        # Headers for the output CSV file
        self.csv_headers = ["Full_Name", "License_Type", "License_Number", "Status", "Professional", "Issued", "Expired"]
        
        # List of professions to scrape data for
        self.professions = [
            'Osteopathy%20Phys%20%26%20Surgeon',
            'Medical%20Physician%20%26%20Surgeon'
        ]
        
        # Set up logging for tracking the script's operation
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        
        # Ensure the output directory exists
        os.makedirs(os.path.dirname(self.output_file), exist_ok=True)

    def get_total_records_and_pages(self, soup):
        """
        Extracts the total number of records and pages from the HTML soup object.
        """
        records_info = soup.find('td', colspan='2', align='center').text
        total_records = int(re.search(r'(\d+) records found', records_info).group(1))
        total_pages = int(re.search(r'of (\d+) total pages', records_info).group(1))
        return total_records, total_pages

    def get_passkeys(self, soup):
        """
        Extracts the passkeys for detail pages from the HTML soup object.
        """
        passkeys = []
        rows = soup.find_all('tr', bgcolor=['#cdcdcd', '#ececec'])
        for row in rows:
            detail_link = row.find('a', href=re.compile(r'licensee-search-detail\.asp\?passkey=\d+'))
            if detail_link:
                passkey = re.search(r'passkey=(\d+)', detail_link['href']).group(1)
                passkeys.append(passkey)
        return passkeys

    def get_detail_data(self, passkey, session):
        """
        Fetches the detailed license data using the provided passkey.
        Implements retries for handling transient errors.
        """
        url = f"https://pr.mo.gov/licensee-search-detail.asp?passkey={passkey}"
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                response = session.get(url, headers=self.headers, verify=False)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, 'html.parser')

                # Parse the details table
                data = {}
                table = soup.find('table', border="1")
                if table:
                    rows = table.find_all('tr')[:5]  # Only the first 5 rows contain relevant data
                    for row in rows:
                        key = row.find('strong').text.strip(':')
                        value = row.find_all('td')[1].text.strip()
                        data[key] = value

                    return data
                else:
                    logging.warning(f"No data table found for passkey {passkey}")
                    return None
            except requests.RequestException as e:
                logging.error(f"Error fetching detail data for passkey {passkey}, attempt {attempt + 1}: {e}")
                if attempt == max_retries - 1:
                    logging.error(f"Max retries reached for passkey {passkey}, moving to next request")
                    return None
                time.sleep(2 ** attempt)  # Exponential backoff for retries

    def scrape_all_pages(self):
        """
        Main scraping logic that iterates through all pages for each profession and extracts data.
        """
        session = requests.Session()
        for profession in self.professions:
            page = 1
            while True:
                # Prepare payload for POST request
                payload = f'message=Submited%20Succesfully&select_county=ALL&select_profession={profession}&select_search=None&select_criteria=&PageIndex={page}'
                url = f"{self.base_url}?passview=1"

                # Send POST request
                response = session.post(url, data=payload, headers=self.headers, verify=False)
                soup = BeautifulSoup(response.text, 'html.parser')

                if page == 1:
                    # Get total records and pages on the first page
                    total_records, total_pages = self.get_total_records_and_pages(soup)
                    logging.info(f"Scraping {profession}: Total records: {total_records}, Total pages: {total_pages}")
                # Get passkeys for the current page
                passkeys = self.get_passkeys(soup)
                # Process each passkey
                for passkey in passkeys:
                    detail_data = self.get_detail_data(passkey, session)
                    if detail_data:
                        self.save_to_csv(detail_data)

                logging.info(f"Scraped {profession} page {page} of {total_pages}")

                if page >= total_pages:
                    break

                page += 1
                time.sleep(1) # Sleep to avoid overwhelming the serve

    def save_to_csv(self, data):
        """
        Saves the extracted data into the output CSV file.
        Ensures no duplicate records and handles date parsing for expiration status.
        """
        try:
            file_exists = os.path.isfile(self.output_file)
            with open(self.output_file, 'a', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=self.csv_headers)
                if not file_exists:
                    writer.writeheader()

                # Parse the expiration date
                expiration_date = datetime.strptime(data.get('Expiration Date', ''), '%m/%d/%Y')

                # Determine status based on expiration date
                status = 'Expired' if expiration_date < datetime.now() else 'Active'

                writer.writerow({
                    'Full_Name': data.get('Licensee Name', ''),
                    'License_Type': data.get('Profession Name', ''),
                    'License_Number': data.get('Licensee Number', ''),
                    'Status': status,
                    'Professional': data.get('Profession Name', ''),
                    'Issued': data.get('Original Issue Date', ''),
                    'Expired': data.get('Expiration Date', '')
                })
        except PermissionError:
            logging.error(f"File {self.output_file} is open. Could not write data.")
        except ValueError:
            logging.error(f"Invalid date format for {data.get('Licensee Name', '')}")

    def run(self):
        """
        Entry point for the crawler. Initiates the scraping process.
        """
        self.scrape_all_pages()
        logging.info("Crawling completed.")


if __name__ == "__main__":
    # Instantiate and run the crawler
    crawler = LicenseCrawler()
    crawler.run()
