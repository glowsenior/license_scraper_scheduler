import csv
import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

import requests
from bs4 import BeautifulSoup

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class LicenseCrawler:

    def __init__(self):

        """Initialize the crawler."""
        self.base_url = 'https://license.wi.gov/s/license-lookup#panel1'
        self.page_url = 'https://license.wi.gov/s/sfsites/aura'
        self.professions_allowed = {"Physician - MD":"MD","Physician - MD Compact":"MD" ,"Physician - DO":"DO","Physician - DO Compact":"DO"}


        self.target_category = "Health"

        self.output_file = 'results/results.csv'
        self.csv_lock = Lock()
        self.headers = {
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.7',
            'Connection': 'keep-alive',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Origin': 'https://license.wi.gov',
            'Referer': 'https://license.wi.gov/s/license-lookup',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-GPC': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
            'X-B3-Sampled': '0',
            'X-SFDC-LDS-Endpoints': 'ApexActionController.execute:DSPS_LicensesLookupController.searchLicense',
            'sec-ch-ua': '"Chromium";v="130", "Brave";v="130", "Not?A_Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
        }

        os.makedirs('results', exist_ok=True)

    def fetch_detail_page(self, licensee_id, index, specialty_name, app_id, fwuid,retries=4):
        """Submit the form for a given details page."""
        while retries:
            retries -= 1
            params = {
                'r': '8',
                'aura.ApexAction.execute': '1',
            }

            data = {
                'message': '{"actions":[{"id":"62;a","descriptor":"aura://ApexActionController/ACTION$execute",'
                           '"callingDescriptor":"UNKNOWN","params":{"namespace":"","classname":"DSPS_LLMS_LicenseLookupDetailsController",'
                           '"method":"searchLicense","params":{"recId":"' + licensee_id + '"},"cacheable":false,"isContinuation":false}}]}',
                'aura.context': '{"mode":"PROD","fwuid":"' + fwuid + '","app":"siteforce:communityApp","loaded":{"APPLICATION@markup://siteforce:communityApp":"' + app_id + '","COMPONENT@markup://instrumentation:o11ySecondaryLoader":"335_G1NlWPtUoLRA_nLC-0oFqg"},"dn":[],"globals":{},"uad":false}',
                'aura.pageURI': '/s/licenseView?id=' + licensee_id,
                'aura.token': 'null',
            }
            logger.info(f"Fetching details {index} for speciality {specialty_name}")
            try:

                response = requests.post(self.page_url, headers=self.headers, params=params, data=data)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as e:
                logger.error(f"Retrying __ Failed to fetch data for detail page {index}: {e}")
        return None

    def fetch_specialities(self, app_id, fwuid):
        """Fetch initial page data including cookies"""
        specialties = []
        params = {
            'r': '3',
            'aura.ApexAction.execute': '1',
        }

        data = {
            'message': '{"actions":[{"id":"63;a","descriptor":"aura://ApexActionController/ACTION$execute","callingDescriptor":"UNKNOWN","params":{"namespace":"","classname":"DSPS_LicensesLookupController","method":"fetchProfessions","cacheable":false,"isContinuation":false}}]}',
            'aura.context': f'{{"mode":"PROD","fwuid":"{fwuid}","app":"siteforce:communityApp","loaded":{{"APPLICATION@markup://{app_id}":"1176_gJXcTqd3KllqEBeApbDkWQ"}},"dn":[],"globals":{{}},"uad":false}}',
            'aura.pageURI': '/s/license-lookup#panel1',
            'aura.token': 'null',
        }
        response = requests.post(self.page_url, headers=self.headers, data=data, params=params)
        if response.status_code != 200:
            logger.error(f"Failed to fetch specialities")
            return None

        j_data = response.json()
        actions = j_data.get("actions", [])
        preferred_professions = self.professions_allowed.keys()
        if actions:
            action = actions[0]
            spec_data = action.get("returnValue", {}).get("returnValue", {}).get(self.target_category, [])
            for spec in spec_data:
                if spec["label"] in preferred_professions:
                    specialties.append({spec["value"]: spec["label"]})

        return specialties

    def fetch_initial_cookies(self):
        """Fetch initial cookies by visiting the base URL."""
        application_value = None
        fwuid = None
        try:

            response = requests.get(self.base_url, headers=self.headers, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            # Find the <script> tag containing the auraConfig object
            aurascript_tag = soup.find("script", string=re.compile(r"var auraConfig"))

            if aurascript_tag:
                # Extract the JavaScript content inside the <script> tag
                script_content = aurascript_tag.string

                # Regular expression to directly find the value of "APPLICATION@markup://siteforce:communityApp"
                match = re.search(r'"APPLICATION@markup://siteforce:communityApp"\s*:\s*"([^"]+)"', script_content)

                if match:
                    application_value = match.group(1)

                else:
                    logger.error("Key 'APPLICATION@markup://siteforce:communityApp' not found.")
            else:
                logger.error("Config object not found.")

            # Find the <script> tag with the src attribute containing the alphanumeric code
            auraFW_script_tag = soup.find("script", {"src": re.compile(r"/s/sfsites/auraFW/javascript/")})

            if auraFW_script_tag:
                # Extract the src attribute
                src_url = auraFW_script_tag['src']

                # Regular expression to match the alphanumeric code in the URL
                match = re.search(r'/javascript/([A-Za-z0-9._-]+)/aura_prod\.js', src_url)

                if match:
                    fwuid = match.group(1)

                else:
                    logger.error("fwuid not found.")
            else:
                logger.error("fwuid Script tag with the specific src attribute not found.")

            return application_value, fwuid
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch initial cookies: {e}")
            return application_value, fwuid

    def fetch_data(self, specialty, app_id, fwuid):
        """Submit the form for a given specialty and fetch the results page."""
        specialty_id = next(iter(specialty.keys()), '')
        logger.info(f"Fetching listing page for speciality {next(iter(specialty.values()), '')}")

        # for detail page
        params = {
            'r': '1',
            'aura.ApexAction.execute': '1',
        }

        data = {
            'message': f'{{"actions":[{{"descriptor":"aura://ApexActionController/ACTION$execute","callingDescriptor":"UNKNOWN","params":{{"namespace":"","classname":"DSPS_LicensesLookupController","method":"searchLicense","params":{{"searchType":"Profession","dataObj":"{{\\"selectedCategory\\":\\"{self.target_category}\\",\\"selectedProfession\\":\\"{specialty_id}\\"}}","recapToken":""}},"cacheable":false,"isContinuation":false}}}}]}}',
            'aura.context': f'{{"mode":"PROD","fwuid":"{fwuid}","app":"siteforce:communityApp","loaded":{{"APPLICATION@markup://{app_id}":"1176_gJXcTqd3KllqEBeApbDkWQ"}},"dn":[],"globals":{{}},"uad":false}}',
            'aura.pageURI': '/s/license-lookup#panel1',
            'aura.token': 'null',
        }

        try:
            response = requests.post(self.page_url, headers=self.headers, data=data, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch data for specialty {specialty}: {e}")
            return None



    def extract_license_type(self, profession):
        return self.professions_allowed.get(profession,"")


    def parse_data(self, j_data, specialty, app_id, fwuid):
        """Parse the licensee data from the JSON content."""
        speciality_name = next(iter(specialty.values()), '')
        results = []
        licensee_ids = []
        actions = j_data.get("actions", [])
        if actions:
            action = actions[0]
            licensee_list = action.get("returnValue", {}).get("returnValue", [])
            for licensee in licensee_list:
                licensee_ids.append(licensee["Id"])
        if licensee_ids:
            logger.info(f"Extracting ({len(licensee_ids)}) details pages for: {speciality_name}")

        for index, licensee_id in enumerate(licensee_ids,start=1):
            detail_page_json = self.fetch_detail_page(licensee_id, index, speciality_name, app_id, fwuid)
            if detail_page_json:
                actions = detail_page_json.get("actions", [])
                if actions:
                    action = actions[0]
                    licensee_details = action.get("returnValue", {}).get("returnValue", [])
                    if licensee_details:
                        licensee_details =licensee_details[0]
                        results.append({
                            "Full_Name": licensee_details.get("name", ""),
                            "License_Type": self.extract_license_type(licensee_details.get("profession", "")),
                            "License_Number": licensee_details.get("licenseNo", ""),
                            "Issued": licensee_details.get("granted", ""),
                            "Expired": licensee_details.get("expirationDate", ""),
                            "Status": licensee_details.get("status", ""),
                            "Professional": licensee_details.get("specialities", ""),
                        })

        return results

    def save_to_csv(self, results):
        """Save results to CSV in a thread-safe manner."""
        with self.csv_lock:
            with open(self.output_file, 'a', newline='', encoding='utf-8') as csvfile:
                fieldnames = ["Full_Name", "License_Type", "License_Number", "Professional", "Status", "Issued",
                              "Expired"]

                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                for result in results:
                    writer.writerow(result)

    def crawl_specialty(self, specialty):
        """Crawl and process licensee data for a given specialty."""
        specialty_name = next(iter(specialty.values()), '')
        logger.info(f"Crawling data for specialty: {specialty_name}")
        app_id, fwuid = self.fetch_initial_cookies()
        if not app_id or not fwuid:
            logger.error(f"Could not extract: {specialty_name}")
            return

        page_json = self.fetch_data(specialty, app_id, fwuid)

        results = self.parse_data(page_json, specialty, app_id, fwuid)
        if results:
            logger.info(f"Rows found: {len(results)} | {specialty_name}")
            self.save_to_csv(results)

    def run(self):
        """Run the crawler concurrently for all specialties."""

        app_id, fwuid = self.fetch_initial_cookies()
        if not app_id or not fwuid:
            logger.error(f"Could not establish cookies.")
            return

        specialties = self.fetch_specialities(app_id, fwuid)
        logger.info(f"specialities found: {len(specialties)}")
        if not specialties:
            logger.error(f"Could not establish a scraper.")
            return

        # Write CSV headers
        with open(self.output_file, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ["Full_Name", "License_Type", "License_Number", "Professional", "Status", "Issued", "Expired"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()


        # Process each specialty concurrently
        with ThreadPoolExecutor(max_workers=1) as executor:
            futures = {executor.submit(self.crawl_specialty, specialty): specialty for specialty in specialties}

            # Wait for all futures to complete
            for future in as_completed(futures):
                specialty = futures[future]
                try:
                    # Optionally, you can get the result if needed
                    result = future.result()  # This will raise an exception if the task failed
                except Exception as e:
                    logger.error(f"Task generated an exception: {e} | {specialty}")


if __name__ == '__main__':
    crawler = LicenseCrawler()
    crawler.run()
