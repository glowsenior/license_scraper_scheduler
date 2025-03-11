import os
import requests
import json
import csv
import string

from app.config import get_settings
from app.utils.logging import AppLogger

logger = AppLogger().get_logger()
settings = get_settings()

class LicenseCrawler:
    def __init__(self):
        self.search_url = "https://www.pals.pa.gov/api/Search/SearchForPersonOrFacilty"
        self.detail_url = "https://www.pals.pa.gov/api/SearchLoggedIn/GetPersonOrFacilityDetails"
        self.headers = {
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive',
            'Content-Type': 'application/json;charset=UTF-8',
            'Cookie': 'ai_user=jXtC3|2024-12-31T11:20:15.316Z; _ga=GA1.2.319863118.1735644023; _gid=GA1.2.1247653138.1735644023; ASP.NET_SessionId=eoc3jqo2rerwtxxpnx0nl24f',
            'Origin': 'https://www.pals.pa.gov',
            'Referer': 'https://www.pals.pa.gov/',
            'Request-Id': '|sP4BM.RC5Lq',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"'
        }
        self.output_file = "results/Pennsylvania/results.csv"
        self.csv_headers = [
            "Full_Name", "License_Type", "License_Number", "Status", "Professional", "Issued", "Expired"
        ]
        self.existing_licenses = set()

        # Ensure the output directory exists
        if os.path.dirname(self.output_file):  # Only create directories if a directory is specified
            os.makedirs(os.path.dirname(self.output_file), exist_ok=True)

        # Initialize the CSV file with headers if it doesn't exist and load existing licenses
        self._initialize_csv_and_load_existing()

    def _initialize_csv_and_load_existing(self):
        """
        Initialize the CSV file if it doesn't exist and load existing licenses into a set.
        """
        if not os.path.exists(self.output_file):
            with open(self.output_file, "w", newline="", encoding="utf-8") as file:
                writer = csv.writer(file)
                writer.writerow(self.csv_headers)
        else:
            # Load existing license numbers into a set for deduplication
            with open(self.output_file, "r", newline="", encoding="utf-8") as file:
                reader = csv.DictReader(file)
                for row in reader:
                    self.existing_licenses.add(row["License_Number"])

    def fetch_details(self, person_id, license_id, license_number, is_facility):
        """
        Fetch detailed information for a specific record.
        """

        # creating details page link
        payload = json.dumps({
            "PersonId": person_id,
            "LicenseId": license_id,
            "LicenseNumber": license_number,
            "IsFacility": is_facility,
        })
        response = requests.post(self.detail_url, headers=self.headers, data=payload)

        if response.status_code == 200:
            return response.json()
        else:
            logger.error(
                f"Error fetching details for PersonId: {person_id}, Status Code: {response.status_code}, Response: {response.text}")
            return None

    def save_to_csv(self, data_list):
        """
        Save the list of data to a CSV file, avoiding duplicates.
        """
        if not data_list:
            logger.warning("No data to save to CSV.")
            return

        with open(self.output_file, "a", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            for row in data_list:
                if row[2] not in self.existing_licenses:  # Check for duplicate License_Number
                    writer.writerow(row)
                    self.existing_licenses.add(row[2])

    def search_recursively(self, lastname, all_results, record_index, profession_id, license_id):
        """
        Recursive function to search through all combinations of last names from A:A to Z:Z.
        """
        # Set search parameters
        payload = json.dumps({
            "OptPersonFacility": "Person",
            "ProfessionID": profession_id,
            "LicenseTypeId": license_id,
            "LastName": lastname,
            "State": "",
            "Country": "ALL",
            "County": None,
            "IsFacility": 0,
            "PersonId": None,
            "PageNo": 1
        })

        logger.info(f"Searching for: {lastname}")
        response = requests.post(self.search_url, headers=self.headers, data=payload)

        # if result returned
        if response.status_code != 200:
            logger.error(f"Error for {lastname}: {response.status_code}, Response: {response.text}")
            return record_index

        data = response.json()

        # Search again if returns more than 50 records
        if len(data) >= 50:
            logger.info("Too many records. Searching deeper...")
            for char in string.ascii_uppercase:
                record_index = self.search_recursively(lastname + char, all_results, record_index, profession_id,
                                                       license_id)

        # If less than 50 records, Save the current results to the all_results list
        elif data:
            temp_results = []  # Temporary list to hold results for this specific combination
            count = 0 # counter
            for record in data:
                person_id = record.get("PersonId")
                license_id = record.get("LicenseId")
                license_number = record.get("LicenseNumber")
                is_facility = record.get("IsFacility")

                details = self.fetch_details(person_id, license_id, license_number, is_facility)
                if details:
                    full_name = f"{details.get('FirstName', '')} {details.get('MiddleName', '')} {details.get('LastName', '')}".strip()

                    # Determine the license type based on ProfessionID
                    license_type = "MD" if profession_id == 37 else "DO" if profession_id == 36 else "Unknown"

                    row = [
                        full_name,
                        license_type,
                        details.get("LicenseNumber"),
                        details.get("Status"),
                        details.get("LicenseType"),
                        details.get("IssueDate"),
                        details.get("ExpiryDate"),
                    ]
                    logger.info(f"Record {count}")
                    temp_results.append(row)
                    count += 1

            # Save results immediately after processing this combination
            self.save_to_csv(temp_results)

        return record_index

    def run(self):
        """
        Wrapper function to handle all initial letters A to Z and write results to a CSV file.
        """
        # Define license types for DO and MD professions
        combinations = [
            {"ProfessionID": 36, "LicenseTypeId": 195},
            {"ProfessionID": 37, "LicenseTypeId": 84},
        ]

        for combination in combinations:
            profession_id = combination["ProfessionID"]
            license_type_id = combination["LicenseTypeId"]

            record_index = 1  # Reset the record index for each combination
            for char in string.ascii_uppercase:
                all_results = []

                # Perform recursive search for each letter and combination
                record_index = self.search_recursively(
                    char, all_results, record_index, profession_id, license_type_id
                )


if __name__ == '__main__':
    crawler = LicenseCrawler()
    crawler.run()