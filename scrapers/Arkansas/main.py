import requests
from bs4 import BeautifulSoup
import re
import logging
import os
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
from tqdm import tqdm  # Import tqdm for progress bar

class LicenseCrawler:
    def __init__(self):
        self.proxies = {
            'http': 'http://ptm24webscraping:ptmxx248Dime_country-us@us.proxy.iproyal.com:12323',
            'https': 'http://ptm24webscraping:ptmxx248Dime_country-us@us.proxy.iproyal.com:12323'
        }
        self.session = requests.Session()
        self.session.proxies = self.proxies
        self.headers = {
            'Host': 'www.armedicalboard.org',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:131.0) Gecko/20100101 Firefox/131.0',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/png,image/svg+xml,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
            'Priority': 'u=0, i',
            'Te': 'trailers',
            'Connection': 'close',
        }
        self.alphabet = "a"
        self.filepath = r'results/results.csv'

    import pandas as pd
    
    def remove_duplicates_from_csv(self):
        """
        Remove duplicate rows from a CSV file.
        
        Args:
            file_path (str): Path to the CSV file.
        """
        # Load the CSV into a DataFrame
        df = pd.read_csv(self.filepath)
        
        # Drop duplicate rows
        df.drop_duplicates(inplace=True)
        
        # Save the cleaned DataFrame back to the original CSV file
        df.to_csv(self.filepath, index=False)

    def return_alphabet(self, update:str):
        alphabet_dict = {chr(letter): None for letter in range(ord('A'), ord('Z') + 1)}
        alphabet_dict = list(alphabet_dict.keys())
        if update:
            if alphabet_dict.index(update.upper()) == 'Z':
                print("Work Done")
                self.remove_duplicates_from_csv()
                exit()
            return alphabet_dict[alphabet_dict.index(update.upper())+1]
        
    def append_to_csv(self, details):
        """Append scraped details to the CSV file with consistent columns."""
        folder_name = 'results'
        if not os.path.exists(folder_name):
            os.makedirs(folder_name)
            print(f"Folder '{folder_name}' created successfully.")
        
        results_file_path = r'results/results.csv'
        file_exists = os.path.isfile(results_file_path)
        with open(results_file_path, mode='a', newline='') as csv_file:
            fieldnames = ["Full_Name", "License_Type", "License_Number", "Status", "Professional", "Issued", "Expired"]
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

            if not file_exists:
                writer.writeheader()
            details = {field: details.get(field, '') for field in fieldnames}
            writer.writerow(details)

    def _scrape_detail(self, link):
        """Scrape detail for a single link."""
        try:
            response = self.session.get(link)
            soup = BeautifulSoup(response.content, 'lxml')
            details = {
                "Full_Name": soup.select_one("#ctl00_MainContentPlaceHolder_lvResults_ctrl0_lblPhyname").get_text(strip=True),
                "License_Type": "MD",
                "License_Number": soup.select_one("#ctl00_MainContentPlaceHolder_lvResultsLicInfo_ctrl0_lblLicnumInfo").get_text(strip=True),
                "Status": soup.select_one("#ctl00_MainContentPlaceHolder_lvResultsLicInfo_ctrl0_lblStatusInfo").get_text(strip=True),
                "Professional": soup.select_one("#ctl00_MainContentPlaceHolder_lvResults_ctrl0_lblprimaryspecialty").get_text(strip=True),
                "Issued": soup.select_one("#ctl00_MainContentPlaceHolder_lvResultsLicInfo_ctrl0_lblORdateInfo").get_text(strip=True),
                "Expired": soup.select_one("#ctl00_MainContentPlaceHolder_lvResultsLicInfo_ctrl0_lblEndDateInfo").get_text(strip=True),
            }
            if "," in details['Full_Name']:
                details['License_Type'] = details['Full_Name'].split(',')[1].strip()
                details['Full_Name'] = details['Full_Name'].split(',')[0].strip()
            else:
                details['License_Type'] = "MD"
            self.append_to_csv(details)
        except Exception as e:
            logging.error(f"Error scraping details for {link}: {e}")

    def fetch_initial_state(self):
        """Fetch initial VIEWSTATE, VIEWSTATEGENERATOR, and EVENTVALIDATION values."""
        u1 = "https://www.armedicalboard.org/public/verify/default.aspx"
        r1 = self.session.get(u1)
        soup = BeautifulSoup(r1.content, 'lxml')
        return (
            soup.find('input', {'name': '__VIEWSTATE'}).get('value'),
            soup.find('input', {'name': '__VIEWSTATEGENERATOR'}).get('value'),
            soup.find('input', {'name': '__EVENTVALIDATION'}).get('value')
        )

    def scrape_links(self, soup):
        """Find all result links on the page and scrape details concurrently."""
        result_links = soup.find_all('a', href=re.compile(r"results\.aspx\?"))
        result_links_main_1 = [f"https://www.armedicalboard.org/Public/verify/{link.get('href')}" for link in result_links]
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(self._scrape_detail, link) for link in result_links_main_1]
            for future in as_completed(futures):
                future.result()

    def run(self):
        """Main runner to execute scraping with pagination."""
        VIEWSTATE, VIEWSTATEGENERATOR, EVENTVALIDATION = self.fetch_initial_state()

        page_number = 1
        total_pages = 22  # assuming you will loop through A to Z (26 letters)
        with tqdm(total=total_pages, desc="Scraping Alphabet", unit="letter") as pbar:
            started = False
            while True:
                # Prepare data and headers for post request
                try:  
                    data = {
                        '__EVENTTARGET': 'ctl00$MainContentPlaceHolder$gvVerifyLicenseResultsLookup',
                        '__EVENTARGUMENT': f'Page${page_number}',
                        '__VIEWSTATE': VIEWSTATE,
                        '__VIEWSTATEGENERATOR': VIEWSTATEGENERATOR,
                        '__VIEWSTATEENCRYPTED': '',
                        '__EVENTVALIDATION': EVENTVALIDATION,
                    }
                    u3 = f'https://www.armedicalboard.org/Public/verify/lookup.aspx?LName={self.alphabet.upper()}'
                    # print(self.alphabet)
                    response = self.session.post(url=u3, headers=self.headers, data=data)
                    with open('response.html', 'wb') as file:
                        file.write(response.content)
                    # rand_list = [12, 123 3,42,34,23,42,3,23,2,34,234,2,12,23,2,4,4,1,3,23,4,4,23,3,4234,423,2342,2]
                    # print(f'working {random.randint(1, 999)} alpha {self.alphabet}')
                    # os.system('clear')
                    soup_temp = BeautifulSoup(response.content, 'lxml')
                    pager_elem = soup_temp.find('h3').text
                    # exit()
                    # print(pager_elem)
                    if 'due to an error' in pager_elem.lower(): 
                        # print('ERROR PAGE REACHED')
                        self.alphabet = self.return_alphabet(self.alphabet)
                        page_number = 1
                        # started = False
                        print(self.alphabet)
                        # print("something")
                        pbar.set_postfix(letter=self.alphabet.upper())  # Update progress bar postfix
                        pbar.update(1)  # Move progress bar forward
                        # exit()

                    soup = BeautifulSoup(response.content, 'lxml')
                    VIEWSTATE = soup.find('input', {'name': '__VIEWSTATE'}).get('value')
                    VIEWSTATEGENERATOR = soup.find('input', {'name': '__VIEWSTATEGENERATOR'}).get('value')
                    EVENTVALIDATION = soup.find('input', {'name': '__EVENTVALIDATION'}).get('value')

                    self.scrape_links(soup)
                    page_number += 1
                except KeyboardInterrupt:
                    self.remove_duplicates_from_csv()
                    exit()
                except:
                    pass
            

scraper = LicenseCrawler()
scraper.run()