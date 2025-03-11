import csv
import logging
import os
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
        self.base_url = 'https://dhp.virginiainteractive.org/Lookup/Index'
        self.page_url = 'https://dhp.virginiainteractive.org/Lookup/Result'
        self.domain = "https://dhp.virginiainteractive.org"
        proxy_url = "http://cruexuku-US-rotate:c3h2jphwjv7y@p.webshare.io:80"
        self.proxies = {
            "http": proxy_url,
            "https": proxy_url
        }
        self.target_occupations = ["Osteopathic Medicine", "Medicine"]

        self.output_file = 'results/results.csv'
        self.csv_lock = Lock()
        self.headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.6',
            'Connection': 'keep-alive',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Sec-GPC': '1',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
            'sec-ch-ua': '"Chromium";v="130", "Brave";v="130", "Not?A_Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
        }

        os.makedirs('results', exist_ok=True)

    def fetch_detail_page(self, detail_page_url, specialty, index, retries=3):
        """Submit the form for a given details page."""
        while retries:
            retries -= 1

            logger.info(f"Fetching details {index} for speciality {specialty['Occupation']} | {specialty['State']}")
            try:

                response = requests.get(detail_page_url, headers=self.headers, proxies=self.proxies)
                response.raise_for_status()
                return response.text
            except requests.exceptions.RequestException as e:
                logger.error(f"Retrying __ Failed to fetch data for detail page {index}: {e}")
        return None

    def parse_detail_page(self, html_content):
        soup = BeautifulSoup(html_content, 'html.parser')
        table = soup.find("table", class_="table table-responsive borderless")

        data = {
            "Full_Name": None,
            "License_Type": None,
            "License_Number": None,
            "Issued": None,
            "Expired": None,
            "Status": None,
            "Professional": None
        }

        if table:
            rows = table.find_all('tr')
            for row in rows:
                th = row.find('th')
                td = row.find('td')
                if not th or not td:
                    continue

                # Map <th> text to the desired field in the dictionary
                th_text = th.get_text(strip=True)
                data["License_Type"] = "MD"
                if th_text == "License Number":
                    data["License_Number"] = td.get_text(strip=True)
                elif th_text == "Occupation":
                    data["Professional"] = td.get_text(strip=True)
                elif th_text == "Name":
                    data["Full_Name"] = td.get_text(strip=True)
                elif th_text == "Initial License Date":
                    data["Issued"] = td.get_text(strip=True)
                elif th_text == "Expire Date":
                    data["Expired"] = td.get_text(strip=True)
                elif th_text == "License Status":
                    data["Status"] = td.get_text(strip=True)
        return data

    def extract_specialities(self, specialties, states):
        """Fetch initial page data including cookies"""

        filtered_specialties = []
        for target_occupation in self.target_occupations:
            for state in states:
                filtered_specialties.append({
                    "Occupation": target_occupation,
                    "OccupationId": specialties[target_occupation],
                    "State": state})

        return filtered_specialties

    def fetch_initial_data(self):
        """Fetch initial cookies by visiting the base URL."""
        cookies = {}
        verification_token = None
        occupations = {}
        states = []
        try:

            response = requests.get(self.base_url, headers=self.headers, proxies=self.proxies, timeout=10)
            response.raise_for_status()

            for cookie in response.cookies:
                cookies[cookie.name] = cookie.value

            soup = BeautifulSoup(response.text, "html.parser")
            # Find the <script> tag containing the auraConfig object
            verification_token = soup.find("input", attrs={"name": "__RequestVerificationToken"})["value"]

            if verification_token:
                occupations_element = soup.find("select", attrs={"id": "OccupationId"})
                occupations = {x.text.strip(): x["value"] for x in occupations_element.find_all("option") if x["value"]}
                states_element = soup.find("select", attrs={"id": "State"})
                states = [x["value"] for x in states_element.find_all("option") if x["value"]]

            else:
                logger.error("Verification Token not found.")

            return cookies, verification_token, occupations, states
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch initial cookies: {e}")
            return cookies, verification_token, occupations, states

    def fetch_data(self, specialty, cookies, verification_token):
        """Submit the form for a given specialty and fetch the results page."""
        session_id = ""
        logger.info(f"Fetching listing page=1 for speciality  {specialty['Occupation']} | {specialty['State']}")
        data = {
            '__RequestVerificationToken': verification_token,
            'OccupationId': specialty['OccupationId'],
            'FName': '',
            'LName': '',
            'State': specialty['State'],
            'Zip': '',
            # for currenlt active licenses
            'LicStatus': '2',
            'submitBtn': 'Search',
            'SearchByOther': 'true',
        }

        try:
            cookie_response = requests.post(self.base_url, headers=self.headers, cookies=cookies, proxies=self.proxies,
                                            data=data, allow_redirects=False)
            cookie_response.raise_for_status()
            session_id = [x.value for x in cookie_response.cookies if x.name == "ASP.NET_SessionId"][0]
            if cookie_response.is_redirect:
                redirect_url = cookie_response.headers['Location']
                response = requests.get(self.domain + redirect_url, headers=self.headers, proxies=self.proxies,
                                        cookies=cookie_response.cookies)
                response.raise_for_status()
                return response.text, session_id
            else:
                logger.error(f"Failed to fetch necessary cookies data for specialty {specialty}")
                return None, session_id
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch data for specialty {specialty}: {e}")
            return None, session_id

    def fetch_next_page(self, specialty, cookies, page_num):
        """Submit the form for a given page and fetch the results page."""
        logger.info(
            f"Fetching listing page={page_num} for speciality  {specialty['Occupation']} | {specialty['State']}")
        params = {
            'Page': page_num,

        }
        try:
            response = requests.get(self.page_url, proxies=self.proxies, headers=self.headers, cookies=cookies,
                                    params=params)
            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch data for specialty {specialty}: {e}")
            return None

    def parse_data(self, html_content):
        """Parse the licensee data from the HTML content."""
        next_page = False
        soup = BeautifulSoup(html_content, "html.parser")
        table = soup.find("table", attrs={"class": "table table-responsive table-striped"})
        if not table:
            return [], next_page
        table_rows = table.tbody.find_all("tr")
        results = []

        for row in table_rows:
            # Find all <td> elements containing <a> tags with the specified URL pattern
            pattern = "/Lookup/Detail/"  # Define the pattern you are looking for

            # Loop through all <a> tags within <td> elements and filter by the href pattern
            for td in row.find_all('td'):
                a_tag = td.find('a', href=True)  # Find <a> tag with an href attribute
                if a_tag and pattern in a_tag['href']:  # Check if the href matches the pattern
                    results.append(self.domain + a_tag["href"])

        link = soup.find('a', string="Next >")  # Find the link with title "Next page"
        if link and link["href"].strip():
            next_page = True

        return results, next_page

    def save_to_csv(self, results):
        """Save results to CSV in a thread-safe manner."""
        with self.csv_lock:
            with open(self.output_file, 'a', newline='', encoding='utf-8') as csvfile:
                fieldnames = ["Full_Name", "License_Type", "License_Number", "Professional", "Status", "Issued",
                              "Expired"]

                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                for result in results:
                    writer.writerow(result)

    def crawl_specialty(self, specialty, s_index):
        """Crawl and process licensee data for a given specialty."""
        logger.info(f"Crawling data for specialty filter #{s_index}: {specialty['Occupation']} | {specialty['State']}")
        cookies, verification_token, _, _ = self.fetch_initial_data()
        if not cookies or not verification_token:
            logger.error(f"Could not extract: {specialty['Occupation']} | {specialty['State']}")
            return

        page_content, session_id = self.fetch_data(specialty, cookies, verification_token)
        if not session_id:
            logger.error("Session Not created for pagination extraction")
        else:
            cookies["ASP.NET_SessionId"] = session_id

        results_urls = []
        results, next_page = self.parse_data(page_content)
        results_urls.extend(results)

        # Covering Pagination
        page_num = 2
        while next_page:
            page_content = self.fetch_next_page(specialty, cookies, str(page_num))
            page_num += 1
            results, next_page = self.parse_data(page_content)
            # logger.info(f"Found more rows: {len(results)}")
            results_urls.extend(results)

        if results_urls:
            logger.info(f"Rows Expected: {len(results_urls)} | {specialty['Occupation']} | {specialty['State']}")
            logger.info("Extracting details...")
            result_rows = []

            for index, results_url in enumerate(results_urls, start=1):
                row = self.parse_detail_page(self.fetch_detail_page(results_url, specialty, index))
                if row:
                    if specialty['Occupation'] == "Osteopathic Medicine":
                        row["License_Type"] = "DO"
                    elif specialty['Occupation'] == "Medicine":
                        row["License_Type"] = "MD"
                    result_rows.append(row)

            if result_rows:
                logger.info(f"Rows found: {len(result_rows)} | {specialty['Occupation']} | {specialty['State']}")
                self.save_to_csv(result_rows)
        else:
            logger.info(f"Page content not found: {specialty['Occupation']} | {specialty['State']}")

    def run(self):
        """Run the crawler concurrently for all specialties."""

        cookies, verification_token, specialties, states = self.fetch_initial_data()
        if not cookies or not verification_token:
            logger.error(f"Could not establish cookies.")
            return

        specialties = self.extract_specialities(specialties, states)
        logger.info(f"Specialities found: {len(specialties)}")
        if not specialties:
            logger.error(f"Could not establish a scraper.")
            return

        # Write CSV headers
        with open(self.output_file, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ["Full_Name", "License_Type", "License_Number", "Professional", "Status", "Issued", "Expired"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

        # Process each specialty concurrently
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = {
                executor.submit(self.crawl_specialty, specialty, s_index): (s_index, specialty)
                for s_index, specialty in enumerate(specialties, start=1)
            }

            # Wait for all futures to complete
            for future in as_completed(futures):
                s_index, specialty = futures[future]
                try:
                    # Optionally, you can get the result if needed
                    result = future.result()  # This will raise an exception if the task failed
                except Exception as e:
                    logger.error(f"Task generated an exception: {e} | {specialty['Occupation']} | {specialty['State']}")


if __name__ == '__main__':
    crawler = LicenseCrawler()
    crawler.run()
