import requests
import json
import csv
import os
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from tenacity import retry, stop_after_attempt, wait_fixed


class LicenseCrawler:
    def __init__(self, max_threads=10):
        self.url = "https://dohenterprise.my.site.com/ver/s/sfsites/aura?r=13&other.SearchComponentController3.searchRecords=1"
        self.output_file = "result/result.csv"
        self.headers = {
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9',
            'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'cookie': 'renderCtx=%7B%22pageId%22%3A%2232683f59-6b51-4ed4-abd0-2bc00ca748b6%22%2C%22schema%22%3A%22Published%22%2C%22viewType%22%3A%22Published%22%2C%22brandingSetId%22%3A%225f76badf-fc0f-4df6-bd73-20881d5e08e3%22%2C%22audienceIds%22%3A%22%22%7D; CookieConsentPolicy=0:1; LSKey-c$CookieConsentPolicy=0:1; pctrk=b2300a4f-25a6-42bd-8378-ebbae98679ec; BrowserId=ObCW-L1nEe-5BRXGn4DgqA; ak_bmsc=0AF4DB0A1975CB1337A4AD0684B1D137~000000000000000000000000000000~YAAQzgwVAjT//duTAQAAZjJw3xpSefx8YXGwuDs4B2+fOH9N7XDhcnIJop5mlo7eKBIzWZN5evKAjoqaAZUrNHc110Dxm8S5UJaO2TBaEKHrs6oXlq1wMUKz4ZAc03/+SJ04L+g0p+q9zDTO9InXCss5NKNawH9HBPmakX8l6oY8Ap5mweoCpAmzhJy6epveH5p6wYvfJtIIn41YyCCYm5W2bPkhBGrfa86/s1kw9RBGgcCjF/6VQqVZk//9PBl4OdxePip1sdisBOlEyhFv21kxoIHvLPavOHWs79/AcY8bkcCt2cvj9d/rvdfHoipzEaIS/X/I3mQ0fq/O4laW8kJwyv5+ZR9gVJgFelInaI9KG18rLr/7F+tmvUS9nMSZLEslYPueV5o4wg==; bm_sv=46A4DEBEABC8AEE6D26EF47DBA12BF5A~YAAQtBZlX4UXf9+TAQAA15un3xp3Gw/2CSRFZ+9erANgcBfbvita1YFPmaxjtz36nC5pIDxZb2UikMflELdzacz+QMUqYRahSyLrfiC/CKq+PSoZSMydguJrUvQ9LwKy1L7cDU0+yOTJ/sJvUNONxFTfFet1YEWRVDaniSV7ASvjE2KNLa6TsSvJ3hSvS0SB5VSG6k+Vs80lIgMytaZ2FAuxQsAs9IQJz9Pps4koyv2VawQ35bWV9wfYUc2ArJl7rw==~1; bm_sv=46A4DEBEABC8AEE6D26EF47DBA12BF5A~YAAQpRZlX/9SErWTAQAALBmo3xovXHUTGgY/i5DgixlxfE9OfUGoVR51fb/UXdDfM2E2F5wjIAVbcsVYIoJkt9q1YmbCeJdyAfn2HNn/HvVbJuhVYyXwFL2eJJI5RuQ4cxxIpaDU0Kcj7rViRYcjtkUprTxWCVTUr3RRafqUSVrPI9fPIv1zFFlkDeZdm1LyFWR+P3JeJPxfaJv7AZao5mYp3P5GBxYo2U+DUeDWszBQIOO3KoCkLps7D26g6iO2wEI=~1',
            'origin': 'https://dohenterprise.my.site.com',
            'priority': 'u=1, i',
            'referer': 'https://dohenterprise.my.site.com/ver/s/',
            'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'x-b3-sampled': '0',
            'x-b3-spanid': 'fed84ae77ffac2c2',
            'x-b3-traceid': '9e5b85a13c323ff6',
            'x-sfdc-page-scope-id': '875dec9b-4c33-43f3-b068-1fcde3da620a',
            'x-sfdc-request-id': '2397927600000f8856'
        }
        self.csv_headers = ["Full_Name", "License_Type", "License_Number", "Status", "Professional", "Issued",
                            "Expired"]
        self.existing_records = set()
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
        self.max_threads = max_threads

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2), reraise=True)
    def fetch_license_data(self, license_prefix):
        """Fetch data for a given license prefix with retries."""

        try:
            payload = f"message=%7B%22actions%22%3A%5B%7B%22id%22%3A%22136%3Ba%22%2C%22descriptor%22%3A%22apex%3A%2F%2FSearchComponentController3%2FACTION%24searchRecords%22%2C%22callingDescriptor%22%3A%22markup%3A%2F%2Fc%3ALicVerificationV1%22%2C%22params%22%3A%7B%22Profession%22%3A%22MEDICINE%22%2C%22LicenseType%22%3A%220%22%2C%22FirstName%22%3A%22%22%2C%22LastName%22%3A%22%22%2C%22LicenseNumber%22%3A%22{license_prefix}%22%2C%22SSN%22%3A%22%22%2C%22Status%22%3A%220%22%7D%7D%5D%7D&aura.context=%7B%22mode%22%3A%22PROD%22%2C%22fwuid%22%3A%22eUNJbjV5czdoejBvRlA5OHpDU1dPd1pMVExBQkpJSlVFU29Ba3lmcUNLWlE5LjMyMC4y%22%2C%22app%22%3A%22siteforce%3AcommunityApp%22%2C%22loaded%22%3A%7B%22APPLICATION%40markup%3A%2F%2Fsiteforce%3AcommunityApp%22%3A%221183_iYPVTlE11xgUFVH2RcHXYA%22%2C%22MODULE%40markup%3A%2F%2Flightning%3Af6Controller%22%3A%22299_KnLaqShH2xCBVYsJK-AI7g%22%2C%22COMPONENT%40markup%3A%2F%2Finstrumentation%3Ao11ySecondaryLoader%22%3A%22342_x7Ue1Ecg1Vom9Mcos08ZPw%22%7D%2C%22dn%22%3A%5B%5D%2C%22globals%22%3A%7B%7D%2C%22uad%22%3Afalse%7D&aura.pageURI=%2Fver%2Fs%2F&aura.token=null"
            response = requests.post(self.url, headers=self.headers, data=payload, timeout=10)
            response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
            data = json.loads(response.text)
            return data.get('actions', [{}])[0].get('returnValue', [])
        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed for prefix {license_prefix}: {e}")
            raise

    def transform_record(self, record):
        """Transform a single record into the desired format."""

        if not record:
            return None

        # Extract the status
        status = record.get('status__c', '')

        # Only process records with 'ACTIVE' status
        if status != 'Active':
            return None  # Skip non-active records

        return {
            "Full_Name": f"{record.get('First_Name__c', '')} {record.get('Last_Name__c', '')}",
            "License_Type": record.get('Liense_Type__c', ''),
            "License_Number": record.get('license_Number__c', ''),
            "Status": status,
            "Professional": record.get('Liense_Type__c', ''),
            "Issued": record.get('Issued_Date__c', ''),
            "Expired": record.get('Expiration_Date__c', ''),
        }

    def save_to_csv(self, records):
        """Save records to the CSV file, ensuring no duplicates."""
        try:
            file_exists = os.path.isfile(self.output_file)
            with open(self.output_file, mode='a', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=self.csv_headers)
                if not file_exists:
                    writer.writeheader()

                for record in records:
                    if not record:  # Skip None records
                        continue
                    license_number = record["License_Number"]
                    if license_number and license_number not in self.existing_records:
                        writer.writerow(record)
                        self.existing_records.add(license_number)
        except PermissionError:
            logging.error(f"File {self.output_file} is open. Could not write data.")

    def process_license_prefix(self, license_prefix):
        """Recursively process a license prefix until the number of records is below 40."""
        try:
            logging.info(f"Processing prefix: {license_prefix}")
            records = self.fetch_license_data(license_prefix)

            if len(records) >= 40:
                # Add a digit (0-9) to narrow down the search
                for i in range(10):
                    self.process_license_prefix(f"{license_prefix}{i}")
            else:
                # Save records to CSV if less than or equal to 40
                transformed_records = [self.transform_record(record) for record in records]
                self.save_to_csv(transformed_records)

                # Further narrow down if needed
                for i in range(10):
                    sub_prefix = f"{license_prefix}{i}"
                    sub_records = [record for record in records if
                                   record.get('license_Number__c', '').startswith(sub_prefix)]
                    if len(sub_records) >= 40:
                        self.process_license_prefix(sub_prefix)

        except Exception as e:
            logging.error(f"Failed to process prefix {license_prefix}: {e}")

    def run(self):
        """Run the crawler with multithreading."""
        with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            # Start processing for both MD and DO prefixes
            futures = []
            for prefix in ["MD", "DO"]:
                for i in range(10):
                    futures.append(executor.submit(self.process_license_prefix, f"{prefix}{i}"))

            # Ensure all threads complete and handle exceptions
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logging.error(f"Error during processing: {e}")

        logging.info("Crawling complete. Results saved to the CSV file.")


if __name__ == "__main__":
    crawler = LicenseCrawler(max_threads=10)
    crawler.run()
