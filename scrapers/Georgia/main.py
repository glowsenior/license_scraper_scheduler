import os
import csv
import json
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs
from python_anticaptcha import AnticaptchaClient, NoCaptchaTaskProxylessTask, AnticatpchaException
from twocaptcha import TwoCaptcha

from app.exception.http import BadRequestHTTPException
from app.config import get_settings
from app.utils.logging import AppLogger

settings = get_settings()
logger = AppLogger().get_logger()

class LicenseCrawler:
    def __init__(self):
        """Initialize the crawler."""
        # Constants
        self.site_url = 'https://gcmb.mylicense.com/verification/Search.aspx'
        self.site_key = '6Ldp57EUAAAAABWjdLVKT-QThpxati6v0KV8azOS'  # reCAPTCHA site key
        self.anti_captcha_api_key = settings.ANTI_CAPTCHA_API_KEY
        self.two_captcha_api_key = settings.TWO_CAPTCHA_API_KEY
        self.proxy_url = settings.PROXY_URL
        self.output_file = 'results/Georgia/results.csv'
        self.proxies = {
            "http": self.proxy_url,
            "https": self.proxy_url
        }
        self.processed_license_numbers = set()

    def get_initial_cookie(self, session):
        """Function to get the initial session cookie by visiting the site."""
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36'
        }

        try:
            response = session.get(self.site_url, headers=headers, proxies=self.proxies)
            response.raise_for_status()
            
            # Extract ASP.NET_SessionId from cookies
            for cookie in session.cookies:
                if cookie.name == "ASP.NET_SessionId":
                    return cookie.value
                
        except requests.RequestException as e:
            logger.error(f"Failed to get initial cookie: {e}")
        
        return None

    def solve_recaptcha(self):
        try:
            client = AnticaptchaClient(self.anti_captcha_api_key)
            task = NoCaptchaTaskProxylessTask("https://eservices.drives.ga.gov/_/#2", "6Lfw_FMUAAAAAMxdNaGt3tJ-b-e-xUAaBny-qxnX")
            job = client.createTask(task)
            job.join()
            captcha_response = job.get_solution_response()
            if not captcha_response:
                raise BadRequestHTTPException('Captcha solving failed or returned empty response')
            logger.info(f"Captcha Solved: {captcha_response}")
            return captcha_response
        except AnticatpchaException as e:
            logger.error(f"Error occurs on solving captcha: {e}")
            return ""
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return ""

    def solve_recaptcha_two(self):
        solver = TwoCaptcha(self.two_captcha_api_key)
        try:
            result = solver.recaptcha(
                sitekey=self.site_key,
                url=self.site_url)
            logger.info(result['code'])
            return result['code']
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return ""

    def perform_search(self, session, session_cookie, captcha_token):
        """Performs a search request to the site."""
        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-US,en;q=0.9',
            'accept-encoding': 'gzip, deflate, br, zstd',
            'cache-control': 'max-age=0',
            'content-type': 'application/x-www-form-urlencoded',
            'cookie': f'ASP.NET_SessionId={session_cookie}',
            'origin': 'https://gcmb.mylicense.com',
            'priority': 'u=0, i',
            'referer': 'https://gcmb.mylicense.com/verification/Search.aspx',
            'sec-ch-ua': '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36'
        }

        payload = {
            '__EVENTTARGET': '',
            '__EVENTARGUMENT': '',
            '__LASTFOCUS': '',
            '__VIEWSTATE': '/wEPDwUJNzM2NTgwNzkyD2QWAgIBD2QWAmYPZBYCAgEPZBYCAgEPZBYCAgEPZBYEAgQPZBYCAgMPZBYCZg8QZGQWAWZkAgUPZBYCAgMPZBYCZg8QZGQWAWZkZP4LppEEdofVCSGKqSwCrARc8wiYcYwitXDmS46gIzK7',
            '__VIEWSTATEGENERATOR': '85FE3531',
            '__EVENTVALIDATION': '/wEdADrDyqqCd90l8TsKC7XBb5uA4sE0J9V0lS8ObwZAtiyjzI8tp4A84ANkmBDS/HBNtfEAtxjR6rmyPFQQxXO4zd6Ial+rPD0txEqI2kkgsvyzXyXJ27e8QGYKMDRflHvWteifqbMOpYXbXmc48YDI534PIyYObqdfw9BWMSEAcnf00DvJovJAPt0FTgYmfTsos5ihYxyARydg9bthrmb2Kx3uOmr6QsG7mrYaqGbZ4DLtGRQevPsIuxKta2XlFpyr/CMTCRYX4oc+nTC57kj/EZE6NKnRtodM7c8whAv+XQES8pvroibL0Dco+srgtZfXRal1llAhdFRrwUhECtcImW/eWdiLfl6pV8HhS4e66rhuU7Xlmu65bleTny8qetYyqUwYDZn0AYxDkCyJdAXyVP7OLdLc7zDMF+eMpWqEZRN3ycGrdZTXX4rio9CqTVQobsF6AnL4L7H7TW0aaaWV6HEAqrl3G/IIYP1K67NOa3GDBBOryttv9Dy+wusBm8DplYfGJJOJafE5fvSsRiAGz+tKjdcBTo5Tq3dhw6W8mipYydXlOwbrGsZLdamR2pCn4dVh+b76HYMF1hL77SFjirTwYP8YVvx4vRA5X0DAzn2X19vhgQ2Th0a4c8Z2Xflnm2qRiyYh8hSCmFI4ho+/Rqhcnw1MyRP94ysi7E6HyNtRoDMC7OFJNK20maW82PC2uY+WvgCrIv8xkLPU1ImlZz2Xy7C1ZL8aCt36eVjm8om2vSNhlLq3zUueUpU592ydlSas8p3hseWnjWhqvCAOkOLQyH9w4iQeBSL/ik8LFn4tSewZrlmprBVZk6zBqwZaL5ldoniEDSuADKJ0xM+IED7j6UQxv4fmHhvuiMWIdq2jFh21fazGc0+jrt4MDLvZCaPoMyTKj+kS2FkSNi2EiraN1xF1+NXWLErgTt2x8F1KKOkp2nceFbDU5CoLh6wFys9hz54gCok1LHvCQe6+pX294nGlPZqRpQdlEW1yYqi8Y4C1a+ALLiUj+bBosP8VWPDi01xTevPb5q69JwEmeIiD7CMUuSC7MtdjZBbo2d2apWJ+JdXWRSTiw4gniyKrnx+sqKtFvPF7dcTCyk/b6tWWjDf/ZysOnPkKb7cETur/7hoynF3GKWVL9Kr898GZTHhY6jJMKxvobDaZa3sSXFph1/LZ+nflGlEFdLQiBCeTajOMKcTdPAXUosL7TStv6almdMpKnFiDikb9LuNH/9sJIAAMMEmMyA3XIqgpaBge1xSU9fXuxP/SQ3lrXO/gN1g=',
            't_web_lookup__first_name': ' ',
            't_web_lookup__license_type_name': '',
            't_web_lookup__last_name': '',
            't_web_lookup__license_status_name': '',
            't_web_lookup__license_no': '',
            't_web_lookup__addr_city': '',
            't_web_lookup__addr_state': '',
            'g-recaptcha-response': captcha_token,
            'sch_button': 'Search'
        }

        response = session.post(self.site_url, headers=headers, data=payload, proxies=self.proxies)
        
        if response.ok:
            soup = BeautifulSoup(response.text, 'html.parser')
            result_page = soup.find('span', id='btn_newsearch_top')
            
            if result_page:
                logger.info("Search successful.")
                form_data = self.get_form_data(response.text)
                with open('search_results.html', 'w') as file:
                    file.write(response.text)
                return form_data
            else:
                logger.error("Search Failed")
                return {}
        else:
            logger.error(f'Search failed with status code {response.status_code}')

    def get_form_data(self, response):
        soup = BeautifulSoup(response, 'html.parser')
        viewstate_input = soup.find('input', {'name': '__VIEWSTATE'})
        eventvalidation_input = soup.find('input', {'name': '__EVENTVALIDATION'})
        
        viewstate_value = viewstate_input.get('value') if viewstate_input else None
        eventvalidation_value = eventvalidation_input.get('value') if eventvalidation_input else None
        
        if not viewstate_value:
            logger.error("VIEWSTATE input not found")
        if not eventvalidation_value:
            logger.error("__EVENTVALIDATION input not found")

        return {
            "viewstate_value": viewstate_value,
            "eventvalidation_value": eventvalidation_value
        }

    def get_search_results_page(self, session, session_cookie, start_index, viewstate_value, eventvalidation_value):
        """Fetches the search results page."""
        search_results_url = 'https://gcmb.mylicense.com/verification/SearchResults.aspx'
        page_start = 0
        page_end = 40
        if start_index != 0:
            page_start = 1
            page_end = 41
        viewstate_val = viewstate_value
        eventvalidation_val = eventvalidation_value
        local_session = session
        local_session_cookie = session_cookie
        for i in range(page_start, page_end):
            try:
                logger.info(f'-------------------------- {start_index} -------------------------')
                logger.info(f'-------------------------- datagrid_results$ctl44$ctl{i} -------------------------')
                headers = {
                    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                    'accept-language': 'en-US,en;q=0.9',
                    'cache-control': 'max-age=0',
                    'cookie': f'ASP.NET_SessionId={local_session_cookie}',
                    'priority': 'u=0, i',
                    'referer': 'https://gcmb.mylicense.com/verification/Search.aspx',
                    'sec-ch-ua': '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"Windows"',
                    'sec-fetch-dest': 'document',
                    'sec-fetch-mode': 'navigate',
                    'sec-fetch-site': 'same-origin',
                    'sec-fetch-user': '?1',
                    'upgrade-insecure-requests': '1',
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36'
                }

                payload = {
                    'CurrentPageIndex': start_index,
                    '__EVENTTARGET': f'datagrid_results$_ctl44$_ctl{i}',
                    '__EVENTARGUMENT': '',
                    '__VIEWSTATE': f'{viewstate_val}',
                    '__VIEWSTATEGENERATOR': '3731BCAC',
                    '__EVENTVALIDATION': f'{eventvalidation_val}'
                }
                
                response = session.post(search_results_url, headers=headers, data=payload, proxies=self.proxies)
                if response.ok:
                    try:
                        form_data = self.get_form_data(response.text)
                        viewstate_val = form_data['viewstate_value']
                        eventvalidation_val = form_data['eventvalidation_value']
                        res = self.extract_detail_url(local_session, response.text)
                        if res == "":
                            logger.error("1. Detail Captcha Solve failed and returned with none RES")
                            local_session = requests.Session()
                            local_session_cookie = self.get_initial_cookie(local_session)
                            if not local_session_cookie:
                                logger.error(f"Failed to obtain initial session cookie for start index: {start_index}")
                                return
                            
                            # Solve captcha token with a retry mechanism
                            captcha_token = ""
                            max_retries = 3
                            retries = 0
                            while captcha_token == "" and retries < max_retries:
                                captcha_token = self.solve_recaptcha()
                                if captcha_token == "":
                                    time.sleep(5)
                                retries += 1
                            
                            if captcha_token == "":
                                logger.error(f"Failed solving captcha after {max_retries} attempts for start index: {start_index}")
                                return

                            logger.info(f"Captcha Token: {captcha_token}")
                            form_data = self.perform_search(local_session, local_session_cookie, captcha_token)
                            viewstate_val = form_data["viewstate_value"]
                            eventvalidation_val = form_data["eventvalidation_value"]
                            i = i - 1
                            continue
                    except json.JSONDecodeError:
                        logger.error(f"JSON decode error: {response.text}")
                    except Exception as e:
                        logger.error(f"An error occurred while parsing: {e}")
                else:
                    logger.error(f"Request to fetch search results failed with status code {response.status_code}")
                    logger.info(f"Refreshing token")
                    local_session = requests.Session()
                    local_session_cookie = self.get_initial_cookie(local_session)
                    if not local_session_cookie:
                        logger.error(f"Failed to obtain initial session cookie for start index: {start_index}")
                        return
                    
                    # Solve captcha token with a retry mechanism
                    captcha_token = ""
                    max_retries = 3
                    retries = 0
                    while captcha_token == "" and retries < max_retries:
                        captcha_token = self.solve_recaptcha()
                        if captcha_token == "":
                            time.sleep(5)
                        retries += 1
                    
                    if captcha_token == "":
                        logger.error(f"Failed solving captcha after {max_retries} attempts for start index: {start_index}")
                        return

                    form_data = self.perform_search(local_session, local_session_cookie, captcha_token)
                    viewstate_val = form_data["viewstate_value"]
                    eventvalidation_val = form_data["eventvalidation_value"]
                    i = i - 1
            except:
                local_session = requests.Session()
                local_session_cookie = self.get_initial_cookie(local_session)
                if not local_session_cookie:
                    logger.error(f"Failed to obtain initial session cookie for start index: {start_index}")
                    return
                
                # Solve captcha token with a retry mechanism
                captcha_token = ""
                max_retries = 3
                retries = 0
                while captcha_token == "" and retries < max_retries:
                    captcha_token = self.solve_recaptcha()
                    if captcha_token == "":
                        time.sleep(5)
                    retries += 1
                
                if captcha_token == "":
                    logger.error(f"Failed solving captcha after {max_retries} attempts for start index: {start_index}")
                    return

                form_data = self.perform_search(local_session, local_session_cookie, captcha_token)
                viewstate_val = form_data["viewstate_value"]
                eventvalidation_val = form_data["eventvalidation_value"]
                i = i - 1
        return "success"

    def extract_detail_url(self, session, html_content):
        """Extracts detail links on the result page"""
        base_url = "https://gcmb.mylicense.com/verification/"
        soup = BeautifulSoup(html_content, 'html.parser')

        # Find all the relevant 'a' elements using BeautifulSoup
        a_elements = soup.select("tr > td:nth-child(1) > table > tr:nth-child(1) > td > a")

        # Extract the href attribute and prepend the base URL
        urls = [base_url + a['href'] for a in a_elements if 'href' in a.attrs]

        # Append the nurse details to the CSV file
        with open(self.output_file, 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            # Write the nurse details
            for url in urls:
                logger.info(f"Processing URL: {url}")
                
                nurse_result = self.extract_detail_content(session, url)
                if nurse_result == {}:
                    return ""
                if nurse_result['Professional'] is not None and nurse_result['License_Number'] is not None and f"{nurse_result['Professional']}-{nurse_result['License_Number']}" in self.processed_license_numbers:
                    # Skip URLs that have already been processed
                    logger.info(f"Skipping duplicate License Number: {nurse_result['Professional']}-{nurse_result['License_Number']}")
                    continue
                writer = csv.DictWriter(csvfile, fieldnames=nurse_result.keys())
                writer.writerow(nurse_result)
                
                # Add URL to processed_urls set
                self.processed_license_numbers.add(f"{nurse_result['Professional']}-{nurse_result['License_Number']}")
        return urls

    def extract_detail_content(self, session, url):
        """Extract nurse detail content from a detail url"""
        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-US,en;q=0.9,ko;q=0.8',
            'cache-control': 'max-age=0',
            'priority': 'u=0, i',
            'referer': 'https://gcmb.mylicense.com/verification/SearchResults.aspx',
            'sec-ch-ua': '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
        }
        response = session.get(url, headers=headers, proxies=self.proxies)
        if response.ok:
            soup = BeautifulSoup(response.text, 'html.parser')
            captcha_flag = soup.find('span', id='label_solvecaptcha')
            
            # Check if captcha appeared
            if captcha_flag:
                soup = self.solve_detail_captcha(session, url)
                if soup == "":
                    logger.error("Detail Captcha pass failed")
                    return {}
                logger.info("Detail Captcha SUCCESS!")

            # Extract nurse data
            nurse_result = self.extract_nurse_data(soup, url)
            return nurse_result
        else:
            logger.error(f"Request to fetch nurse details failed with status code {response.status_code}")
            logger.error(f"403 text {response.text}")
            return {}

    def solve_detail_captcha(self, session, url):
        """Solve captcha in detail page"""
        # Solve captcha token
        captcha_token = ""
        while captcha_token == "":
            captcha_token = self.solve_recaptcha()
            if captcha_token == "":
                time.sleep(5)
        logger.info(f"Detail Captcha Solved")
        
        # Parse result ID from detail page
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)
        result_value = query_params.get('result', [None])[0]
        if result_value:
            url = f"https://gcmb.mylicense.com/verification/SolveCaptcha.aspx?result={result_value}"

            headers = {
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                "accept-language": "en-US,en;q=0.9,ko;q=0.8",
                "cache-control": "max-age=0",
                "content-type": "application/x-www-form-urlencoded",
                "priority": "u=0, i",
                "sec-ch-ua": "\"Google Chrome\";v=\"129\", \"Not=A?Brand\";v=\"8\", \"Chromium\";v=\"129\"",
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": "\"Windows\"",
                "sec-fetch-dest": "document",
                "sec-fetch-mode": "navigate",
                "sec-fetch-site": "same-origin",
                "sec-fetch-user": "?1",
                "upgrade-insecure-requests": "1",
            }

            data = {
                "__EVENTTARGET": "",
                "__EVENTARGUMENT": "",
                "__VIEWSTATE": "/wEPDwUJNzM2NTgwNzkyZGREZrd1ZR0+FyibTQSfgKJeAiVnfR+J4/nMxA0y9O5LOQ==",
                "__VIEWSTATEGENERATOR": "386B5A90",
                "__EVENTVALIDATION": "/wEdAAJy+TytmJXMj696YMUAV2NgYO7KKjH3yMmv1jAy4K5Q+oDq/MH8IE8chFDMnW1jCNH9WPCXxGxONxYUL0CUoKlf",
                "g-recaptcha-response": captcha_token,
                "submit_button": "Submit"
            }

            response = session.post(url, headers=headers, data=data, proxies=self.proxies)

            if response.status_code == 200:
                logger.info("captcha solved")
                soup = BeautifulSoup(response.text, 'html.parser')
                return soup
            else:
                logger.error(f"Failed to retrieve page with status code: {response.status_code}")
                logger.error(response.text)
                return ""
        else:
            logger.error("No 'result' parameter found in the given URL.")

    def extract_nurse_data(self, soup, url):
        """Extract the nurse data based on specific HTML structure"""
        nurse_data = {}

        full_name = soup.find('span', id='_ctl34__ctl1_full_name')
        if full_name:
            nurse_data['Full_Name'] = full_name.get_text().strip('"')
        else:
            nurse_data['Full_Name'] = ''
            
        license_type = soup.find('span', id='_ctl34__ctl1_udo_lic_degree_suffix')
        if license_type:
            nurse_data['License_Type'] = license_type.get_text().strip('"')
        else:
            nurse_data['License_Type'] = ''
            
        license_no = soup.find('span', id='_ctl40__ctl1_license_no')
        if license_no:
            nurse_data['License_Number'] = license_no.get_text().strip('"')
        else:
            nurse_data['License_Number'] = ''

        professional = soup.find('span', id='_ctl40__ctl1_license_type')
        if professional:
            nurse_data['Professional'] = professional.get_text().strip('"')
        else:
            nurse_data['Professional'] = ''
            
        status = soup.find('span', id='_ctl40__ctl1_status')
        if status:
            nurse_data['Status'] = status.get_text().strip('"')
        else:
            nurse_data['Status'] = ''
            
        issued = soup.find('span', id='_ctl40__ctl1_issue_date')
        if issued:
            nurse_data['Issued'] = issued.get_text().strip('"')
        else:
            nurse_data['Issued'] = ''
            
        expired = soup.find('span', id='_ctl40__ctl1_expiry')
        if expired:
            nurse_data['Expired'] = expired.get_text().strip('"')
        else:
            nurse_data['Expired'] = ''
        return nurse_data

    def initialize_csv(self):
        """Initialize necessary csv files"""
        os.makedirs('results/Georgia', exist_ok=True)
        with open(self.output_file, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['Full_Name', 'License_Type', 'License_Number', 'Professional', 'Status', 'Issued', 'Expired'])

    def worker(self, start_index):
        """Thread task init for each worker"""
        session = requests.Session()
        session_cookie = self.get_initial_cookie(session)
        if not session_cookie:
            logger.error(f"Failed to obtain initial session cookie for start index: {start_index}")
            return
        
        captcha_token = ""
        max_retries = 3
        retries = 0
        while captcha_token == "" and retries < max_retries:
            captcha_token = self.solve_recaptcha()
            if captcha_token == "":
                time.sleep(5)
            retries += 1
        
        if captcha_token == "":
            logger.error(f"Failed solving captcha after {max_retries} attempts for start index: {start_index}")
            return

        logger.info("Captcha Solved")
        form_data = self.perform_search(session, session_cookie, captcha_token)
        self.get_search_results_page(session, session_cookie, start_index, form_data['viewstate_value'], form_data['eventvalidation_value'])

    def run(self):
        """Main function to run the scraper"""
        self.initialize_csv()
        self.worker(0)  # Start with index 0

if __name__ == '__main__':
    crawler = LicenseCrawler()
    crawler.run()
