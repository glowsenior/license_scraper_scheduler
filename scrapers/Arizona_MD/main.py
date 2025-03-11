import os
import csv
import logging
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin
from threading import Lock

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class LicenseCrawler:
    def __init__(self):
        """Initialize the crawler and set up CSV writing with thread safety."""
        self.base_url = 'https://azbomv7prod.glsuite.us/glsuiteweb/clients/azbom/public/webverificationsearch.aspx?q=azmd&t=20240701054428'
        self.result_file = 'results.csv'
        self.csv_lock = Lock()
        self.result_file = os.path.join('result', 'results.csv')

        os.makedirs('result', exist_ok=True)


        if not os.path.isfile(self.result_file):
            with open(self.result_file, mode='w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ["Full_Name", "License_Type", "License_Number", "Professional", "Status", "Issued", "Expired"]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()

    def convert_to_dict(self, data_list):
        """Convert HTML details list into a dictionary."""
        temp_details = {}
        for item in data_list:
            cleaned_item = item.replace('<td>', '').replace('</td>', '').replace('\xa0', '').strip()
            if cleaned_item and ':' in cleaned_item:
                key, value = cleaned_item.split(':', 1)
                temp_details[key.strip()] = value.strip()
        return temp_details

    def init_reqs(self, speciality_value):
        """Initialize the request session and fetch the necessary tokens."""
        session = requests.Session()
        try:
            r1 = session.get(url=self.base_url, timeout=10)
            soup = BeautifulSoup(r1.content, 'html.parser')
            VIEWSTATE = soup.find('input', {'name': '__VIEWSTATE'}).get('value')
            VIEWSTATEGENERATOR = soup.find('input', {'name': '__VIEWSTATEGENERATOR'}).get('value')
            EVENTVALIDATION = soup.find('input', {'name': '__EVENTVALIDATION'}).get('value')

            headers = {
                "User-Agent": "Mozilla/5.0",
                "Content-Type": "application/x-www-form-urlencoded",
            }

            data = {
                "__VIEWSTATE": VIEWSTATE,
                "__VIEWSTATEGENERATOR": VIEWSTATEGENERATOR,
                "__EVENTVALIDATION": EVENTVALIDATION,
                "ctl00$ContentPlaceHolder1$Specialty": "rbSpecialty1",
                "ctl00$ContentPlaceHolder1$ddlSpecialty": speciality_value,
                "ctl00$ContentPlaceHolder1$ddlCounty": "15910",
                "__EVENTTARGET": "ctl00$ContentPlaceHolder1$btnSpecial",
            }

            main_req = session.post(url=self.base_url, headers=headers, data=data, timeout=10)
            return main_req, session
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to initialize requests: {e}")
            return None, None

    def scrape_profile(self, profile_link, profession_name):
        """Scrape details of an individual profile."""
        try:
            with requests.Session() as session:
                req_to_profile = session.get(url=profile_link, timeout=10)
                soup = BeautifulSoup(req_to_profile.content, 'html.parser')

                name_tag = soup.find('span', {"id": "ContentPlaceHolder1_dtgGeneral_lblLeftColumnEntName_0"})
                name = name_tag.text.replace(' MD', '') if name_tag else ''

                main_table = soup.find('table', {'id': "ContentPlaceHolder1_dtgGeneral"}).find_all('td')
                details_table = main_table[1] if len(main_table) > 1 else None
                details_table_list = str(details_table).split('<br/>') if details_table else []
                profile_details = self.convert_to_dict(details_table_list)

                details = {
                    "Full_Name": name,
                    "License_Type": "MD",
                    "License_Number": profile_details.get('License Number', ''),
                    "Professional": profession_name,
                    "Status": profile_details.get("License Status", ''),
                    "Issued": profile_details.get('Licensed Date', ''),
                    "Expired": profile_details.get('If not Renewed, LicenseExpires', '')
                }
                return details
        except Exception as e:
            logger.error(f"Error retrieving profile at {profile_link}: {e}")
            return None

    def fetch_profile_links(self, speciality_value):
        """Fetch profile links from the main search page."""
        main_req, session = self.init_reqs(speciality_value)
        if not main_req:
            return []

        soup = BeautifulSoup(main_req.content, 'html.parser')
        links_elem = soup.find_all('tr', {'class': 'headerBlue Verdana10Center'})
        profile_links = [urljoin("https://azbomv7prod.glsuite.us", i.find('td').find('a').get('href')) for i in links_elem]
        
        logger.info(f"Found {len(profile_links)} profile links.")
        return profile_links

    def append_to_csv(self, details_list):
        """Append the scraped details to the CSV in a thread-safe manner."""
        with self.csv_lock:
            with open(self.result_file, mode='a', newline='', encoding='utf-8') as csvfile:
                fieldnames = ["Full_Name", "License_Type", "License_Number", "Professional", "Status", "Issued", "Expired"]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                for details in details_list:
                    writer.writerow(details)
    def return_professionList(self) -> list:
        """Returns value for all selections"""
        url = "https://azbomv7prod.glsuite.us/glsuiteweb/clients/azbom/public/webverificationsearch.aspx?q=azmd&t=20240701054428"
        profession_req = requests.get(url=url)
        soup = BeautifulSoup(profession_req.content, 'html.parser')
        select_elem = soup.find('select', {'name':"ctl00$ContentPlaceHolder1$ddlSpecialty"})
        profession_values_elem = select_elem.find_all('option')
        profession_values_list = []
        profession_name_list = []
        for _ in profession_values_elem:
            profession_values_list.append(_.get('value'))
            profession_name_list.append(_.get('title'))
            # print(_.get('title'))
        return profession_values_list, profession_name_list

    def run(self):
        """Main function to run the crawler and write each profile directly to CSV."""
        specialityValue_list, profession_name_list = self.return_professionList()
        profession_name_list = profession_name_list[1:]
        # print(profession_name_list)
        for speciality_value, profession_name in zip(specialityValue_list, profession_name_list):
            profile_links = self.fetch_profile_links(speciality_value)
            if not profile_links:
                logger.error("No profile links found.")
                return

            with ThreadPoolExecutor(max_workers=4) as executor:
                future_to_profile = {executor.submit(self.scrape_profile, link, profession_name): link for link in profile_links}
                for future in as_completed(future_to_profile):
                    details = future.result()
                    if details:
                        self.append_to_csv([details])  

        logger.info("Crawling completed and data saved to CSV.")



if __name__ == '__main__':
    crawler = LicenseCrawler()
    crawler.run()
