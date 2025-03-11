import os  
import csv  
import requests  
import logging  
from bs4 import BeautifulSoup  
from concurrent.futures import ThreadPoolExecutor  
from threading import Lock  

# Set up logging  
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')  
logger = logging.getLogger(__name__)  

class LicenseCrawler:  

    def __init__(self, search_options=None):  
        """Initialize the crawler."""  
        self.base_url = 'https://www.kansas.gov/ssrv-ksbhada/search.html'  
        self.details_base_url = 'https://www.kansas.gov/ssrv-ksbhada/details.html?id='  
        # self.search_options = search_options or ["24"]  
        self.search_options = search_options or ["24", "01", "75", "05", "04", "21", "21A", "17", "18", "11", "14", "15", "12", "94", "22", "16", "19", "08", "T", "PP", "23", "TW"]  

        os.makedirs('result', exist_ok=True)  
        self.csv_lock = Lock()  

    def fetch_initial_cookies(self):  
        """Fetch initial cookies by visiting the base URL."""  
        try:  
            session = requests.Session()  
            session.get(self.base_url, timeout=10)  
            return session  
        except requests.exceptions.RequestException as e:  
            logger.error(f"Failed to fetch initial cookies: {e}")  
            return None  

    def fetch_page_content(self, session, profession):  
        """Fetch the results page for a given profession."""  
        try:  
            response = session.post(  
                self.base_url,  
                headers={  
                    'User-Agent': 'Mozilla/5.0',  
                    'Content-Type': 'application/x-www-form-urlencoded'  
                },  
                data={  
                    'lastName': '',  
                    'firstName': '',  
                    'middleInitial': '',  
                    'licenseNumber': '',  
                    'city': '',  
                    'profession': profession,  
                    'specialty': '',  
                    'submit': ''  
                },  
                timeout=10  
            )  
            response.raise_for_status()  
            return response.content  
        except requests.exceptions.RequestException as e:  
            logger.error(f"Failed to fetch page content for profession {profession}: {e}")  
            return None  

    def parse_license_page(self, html_content, profession):  
        """Parse license data from HTML content."""  
        if not html_content:  
            logger.warning("No HTML content to parse.")  
            return []  

        results = []  
        try:  
            soup = BeautifulSoup(html_content, 'html.parser')  
            table_rows = soup.select("#agency-content > table > tbody > tr")  
            
            for row in table_rows:  
                full_name_tag = row.select_one("td:nth-child(1) > a")  
                if full_name_tag:  
                    full_name = full_name_tag.text.strip()  
                    logger.info(f"Parsing data for {full_name}")  
                    details_href = full_name_tag['href']  
                    details_link = f"https://www.kansas.gov{details_href}"  
                    license_number = row.select_one("td:nth-child(3)").text.strip()  
                    professional = row.select_one("td:nth-child(2)").text.strip()  

                    # Fetch additional details from the details page  
                    details_response = requests.get(details_link, timeout=10)  
                    details_response.raise_for_status()  
                    details_html = details_response.content  
                    issued, expired, status = self.extract_details(details_html)  

                    results.append({  
                        "Full_Name": full_name,  
                        "License_Type": professional,
                        "License_Number": license_number,  
                        "Professional": professional,  
                        "Status": status,  
                        "Issued": issued,  
                        "Expired": expired  
                    })  
        except Exception as e:  
            logger.error(f"Error parsing license page for profession {profession}: {e}")  
        
        # Append the results to the CSV file  
        self.append_results_to_csv(results)  

    def extract_details(self, html_content):  
        """Extract detailed fields like Issued, Expired, and Status."""  
        try:  
            soup = BeautifulSoup(html_content, 'html.parser')  
            
            issued_raw = soup.select_one("#agency-content > div:nth-child(2) > div.column.span-8.colborder > ul > li:nth-child(5)").text  
            issued = issued_raw.replace('Original License Date: ', '').strip()  
            
            expired_raw = soup.select_one("#agency-content > div:nth-child(2) > div.column.span-8.colborder > ul > li:nth-child(4)").text  
            expired = expired_raw.replace('License Expiration Date: ', '').strip()  
            
            status_raw = soup.select_one("#agency-content > div:nth-child(2) > div.column.span-8.colborder > ul > li:nth-child(2)").text  
            status = status_raw.replace('License Type: ', '').strip()  

            return issued, expired, status  
        except Exception as e:  
            logger.error(f"Error extracting details: {e}")  
            return "", "", ""  

    def calculate_total_pages(self, html_content):  
        """Calculate total pages from initial data."""  
        try:  
            soup = BeautifulSoup(html_content, 'html.parser')  
            search_total_raw = soup.select_one("#agency-content > div:nth-child(4) > div:nth-child(1) > p").text  
            total_count = int(search_total_raw.split('of')[2].strip())  
            return (total_count // 25) + 1  
        except Exception as e:  
            logger.error(f"Error calculating total pages: {e}")  
            return 1  

    def crawl_profession(self, profession):  
        """Crawl all pages for a given profession."""  
        session = self.fetch_initial_cookies()  
        if not session:  
            logger.error(f"Could not establish a session for profession {profession}.")  
            return  

        page_content = self.fetch_page_content(session, profession)  
        if page_content is None:  
            return  

        # Extract total pages for the search results  
        total_pages = self.calculate_total_pages(page_content)  
        # Parse content and collect results  
        self.parse_license_page(page_content, profession)  

        for i in range(2, total_pages + 1):  
            try:  
                logger.info(f"Fetching page {i} for profession {profession}")  
                next_page_url = f'https://www.kansas.gov/ssrv-ksbhada/results.html?navigate=next&page={i}'  
                response = session.get(next_page_url, timeout=10)  
                response.raise_for_status()  
                self.parse_license_page(response.content, profession)
            except requests.exceptions.RequestException as e:  
                logger.error(f"Failed to fetch page {i} for profession {profession}: {e}")  

    def append_results_to_csv(self, results):  
        """Append results to CSV file in a thread-safe manner."""  
        with self.csv_lock:  
            with open('result/result.csv', 'a', newline='', encoding='utf-8') as csvfile:  
                fieldnames = ["Full_Name", "License_Type", "License_Number", "Professional", "Status", "Issued", "Expired"]  
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)  
                for result in results:  
                    writer.writerow(result)  

    def run(self):  
        """Run the crawler over all search options concurrently."""  
        output_file = 'result/result.csv'  
        os.makedirs('result', exist_ok=True)  
        
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:  
            fieldnames = ["Full_Name", "License_Type", "License_Number", "Professional", "Status", "Issued", "Expired"]  
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)  
            writer.writeheader()  

        with ThreadPoolExecutor(max_workers=len(self.search_options)) as executor:  
            for profession in self.search_options:  
                executor.submit(self.crawl_profession, profession)  


if __name__ == '__main__':  
    crawler = LicenseCrawler()  
    crawler.run()