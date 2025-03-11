import csv
import logging
import os
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import urllib3

# Suppress specific warnings
warnings.filterwarnings('ignore', category=urllib3.exceptions.InsecureRequestWarning)
from bs4 import BeautifulSoup
from anticaptchaofficial.recaptchav2proxyless import *

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class LicenseCrawler:

    def __init__(self):

        """Initialize the crawler."""
        self.base_url = 'https://www.wvbdosteo.org/verify/'
        self.captcha_url = 'https://www.wvbdosteo.org/www/verify/recaptcha'
        self.data_url = "https://www.wvbdosteo.org/ajax/"

        self.output_file = 'results/results.csv'
        self.csv_lock = Lock()

        self.ANTI_CAPTCHA_API_KEY = 'fe348e4a8a96a206a483b6ea98ee3751'
        self.headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Sec-GPC': '1',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'sec-ch-ua': '"Brave";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
        }

        self.target_types = ['Osteopathic Physician', 'Osteopathic Physician Assistant']


        os.makedirs('results', exist_ok=True)

    def fetch_specialities(self):
        """Fetch initial page data including cookies"""
        response = requests.get(self.base_url, headers=self.headers)
        if response.status_code != 200:
            logger.error(f"Failed to fetch specialities and states")
            return None
        soup = BeautifulSoup(response.text, "html.parser")

        # Extract specialty options
        speciality_element = soup.find("select", attrs={"id": "licType"})
        specialities_data = [(x.text.strip(), x["value"],) for x in speciality_element.find_all("option") if
                        x["value"].strip()]

        specialities = []

        for speciality_data in specialities_data:
            professional, search_type = speciality_data
            if professional in self.target_types:
                specialities.append({'license_type': "DO", "professional": professional,"search_type":search_type})

        session_cookie = dict()
        for key, value in response.cookies.items():

            if "ASPSESSIONID" or "sessionID" in key:
                session_cookie[key] = value

        return specialities ,session_cookie

    def solve_captcha(self,site_key):
        # The CAPTCHA site-key (you need to inspect the HTML to get this value)
        # site_key = "your_target_site_recaptcha_key"  # Replace with the actual site-key from the target page

        solver = recaptchaV2Proxyless()
        solver.set_verbose(1)
        solver.set_key(self.ANTI_CAPTCHA_API_KEY)
        solver.set_website_url(self.base_url)
        solver.set_website_key(site_key)
        # set optional custom parameter which Google made for their search page Recaptcha v2
        # solver.set_data_s('"data-s" token from Google Search results "protection"')

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

    def fetch_initial_data(self,session_cookie, retries=3):
        """Fetch initial page data including cookies and hidden form fields."""

        session = data_sitekey = g_response = None

        while retries:
            retries -=1
            try:
                response = requests.get(self.captcha_url, headers=self.headers, verify=False)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, "html.parser")
                data_site_key_div = soup.find("div", "g-recaptcha")
                if data_site_key_div:
                    data_sitekey = data_site_key_div["data-sitekey"]
                    g_response = self.solve_captcha(data_sitekey)
                    if g_response:
                        session = requests.session()
                        session.headers.update(self.headers)
                        session.cookies.update(session_cookie)
                        break
                    else:
                        raise Exception("Captcha Response not found")
                else:
                    raise Exception("data site key not found")


            except Exception as exp:
                logger.error("Retrying __ Error Fetching session.")
                time.sleep(1)


        if response.status_code != 200 or not retries:
            logger.error(f"Failed to fetch initial cookies")
            return None

        return session ,  g_response


    def fetch_licensee_data(self, session, g_response, specialty_name, search_type,retries=3):
        """Submit the form for a given specialty and fetch the results page."""

        json_data  = {
            'action': 'verifySearch',
            'g-recaptcha-response': g_response,
            'licType': search_type,
            'licNo': '',
            'city': '',
        }

        # logger.info(f"Fetching listing page=1 for speciality  {specialty_name} state {state}")

        while retries:
            retries -=1
            try:
                response = session.get(self.captcha_url, data=json_data)
                response.raise_for_status()

                response = session.post(self.data_url, data=json_data)
                response.raise_for_status()

                total_records = response.json()["records"][0]["count"]
                logger.info(f"Fetching Total records {total_records} for speciality  {specialty_name}")

                json_data = {
                    'action': 'verifyPage',
                    'pageNo': '1',
                     'recsPage': f"{total_records}",

                }


                response = session.post(self.data_url, data=json_data)
                response.raise_for_status()

                return response.json()
            except Exception as e:
                logger.error(f"Retrying __ Failed to fetch data for specialty {specialty_name}: {e}")
                time.sleep(1)
        return None


    def save_to_csv(self, results):
        """Save results to CSV in a thread-safe manner."""
        with self.csv_lock:
            with open(self.output_file, 'a', newline='', encoding='utf-8') as csvfile:
                fieldnames = ["Full_Name", "License_Type", "License_Number", "Status", "Professional", "Issued",
                              "Expired"]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                for result in results:
                    writer.writerow(result)

    def clean_name(self,name_str):
        return name_str.replace("&nbsp;"," ").strip()

    def crawl_specialty(self, specialty_dict, index, total, init_data):
        """Crawl and process licensee data for a given specialty."""
        specialty_name = specialty_dict['professional']
        search_type = specialty_dict['search_type']
        license_type = specialty_dict['license_type']
        logger.info(f"Crawling data for specialty({index}/{total}): {specialty_name} search_type: {search_type}")

        session ,  g_response = init_data
        page_content = self.fetch_licensee_data(session, g_response, specialty_name, search_type)
        if page_content:

            licensees = page_content.get("records",{})

            results_rows = []
            for licensee in licensees:
                row = {
                    "Full_Name":self.clean_name( licensee.get("name", "")),
                    "License_Type": license_type,
                    "License_Number": licensee.get("licNo", ""),
                    "Issued":licensee.get("iss", ""),
                    "Expired": licensee.get("exp", ""),
                    "Status": licensee.get("stat", ""),
                    "Professional": specialty_name
                }
                if row:
                    results_rows.append(row)
            logger.info(f"Rows found: {len(results_rows)} | specialty: {specialty_name} search_type: {search_type}")
            if results_rows:
                self.save_to_csv(results_rows)
        else:
            logger.info(f"Page content not found. Specialty: {specialty_name} search_type: {search_type}")

    def run(self):
        """Run the crawler concurrently for all specialties."""

        specialties, session_cookie= self.fetch_specialities()
        if not specialties:
            logger.error(f"Could not establish a scraper.")
            return
        logger.info("Extracted specialities")
        init_data = self.fetch_initial_data(session_cookie)
        if not init_data:
            logger.error(f"Could not extract data")
            return
        logger.info("Extracted cookies, session and captcha response")
        # Write CSV headers
        with open(self.output_file, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ["Full_Name", "License_Type", "License_Number", "Status", "Professional", "Issued", "Expired"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

        total_specialities = len(specialties)
        logger.info(f"Total specialities found: {total_specialities}")

        # Process each specialty concurrently
        with ThreadPoolExecutor(max_workers=
                                1) as executor:
            futures = {
                executor.submit(self.crawl_specialty, specialty, index, total_specialities,init_data): specialty for
                index, specialty in enumerate(specialties, start=1)
            }

            # Wait for all futures to complete
            for future in as_completed(futures,timeout=60):
                specialty_dict = futures[future]
                try:
                    # Optionally, you can get the result if needed
                    result = future.result()  # This will raise an exception if the task failed
                except Exception as e:
                    logger.error(
                        f"Task generated an exception: {e} | speciality: {specialty_dict['professional']}, search_type: {specialty_dict['search_type']}")



if __name__ == '__main__':
    crawler = LicenseCrawler()
    crawler.run()
