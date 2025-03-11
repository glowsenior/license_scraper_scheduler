import csv
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

import undetected_chromedriver as uc
from selenium.common import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class LicenseCrawler:

    def __init__(self):

        """Initialize the crawler."""
        self.base_url = 'https://secure.utah.gov/llv/search/index.html'
        self.pagination_url = "https://secure.utah.gov/llv/search/search.html?currentPage={}&orderBy=full_name&descending=false"

        self.output_file = 'results/results.csv'
        self.csv_lock = Lock()

        self.search_filter_list = [("PHYSICIAN", '767', 'MD'), ('OSTEOPATHIC PHYSICIAN', '695', 'DO')]

        os.makedirs('results', exist_ok=True)

    def waited_for_windows_load(self, ch_driver, time_out=100):
        """Wait for the driver to load the window"""
        try:
            WebDriverWait(ch_driver, time_out).until(
                lambda ch_driver: ch_driver.execute_script("return document.readyState") == "complete"
            )
            return True
        except TimeoutException:
            logger.error("Page load timed out.")
            return False

    def remove_duplicates_in_csv(self):
        """Remove duplicate rows in the CSV based on all columns."""
        with self.csv_lock:
            # Read existing data from the file
            try:
                with open(self.output_file, 'r', encoding='utf-8') as csvfile:
                    reader = csv.DictReader(csvfile)
                    existing_data = list(reader)
            except FileNotFoundError:
                existing_data = []

            duplicate_count = 0
            # Remove duplicates based on all columns
            seen_rows = set()
            unique_data = []
            for row in existing_data:
                # Create a tuple of all values in the row to track duplicates
                row_tuple = tuple(row.items())  # Hashable representation of the row
                if row_tuple not in seen_rows:
                    seen_rows.add(row_tuple)
                    unique_data.append(row)
                else:
                    duplicate_count += 1

            logger.info(f"Duplicates found: {duplicate_count}")

            # Write the unique data back to the CSV
            if existing_data:  # Ensure there is data to infer fieldnames
                fieldnames = existing_data[0].keys()
            else:
                fieldnames = []

            with open(self.output_file, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(unique_data)

    def save_to_csv(self, results):
        """Save results to CSV in a thread-safe manner."""
        with self.csv_lock:
            with open(self.output_file, 'a', newline='', encoding='utf-8') as csvfile:
                fieldnames = ["Full_Name", "License_Type", "License_Number", "Status", "Professional", "Issued",
                              "Expired"]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                for result in results:
                    writer.writerow(result)

    def wait_for_details(self, ch_driver, tag_name, tag_text, time_out=100):
        """
        Wait for an element to have the text the target text.

        :param ch_driver: The WebDriver instance.
        :param licensee_name: The name to be included in the text.
        :param time_out: Timeout in seconds. Default is 100 seconds.
        :return: The WebElement if the text is found, None if it times out.
        """

        try:
            _ = WebDriverWait(ch_driver, time_out).until(
                EC.text_to_be_present_in_element((By.TAG_NAME, tag_name), tag_text)
            )
            return True
        except Exception:
            logger.error(f"{tag_name} element with text '{tag_text}' did not appear within the timeout.")
            return False

    def crawl_search_filter(self, search_filter_index, search_filter_data, path):

        """Crawl and process licensee data for a given search filter."""
        professional, search_filter, license_type = search_filter_data
        logger.info(f"Crawling data for search filter#{search_filter_index} | {search_filter_data}")
        time.sleep(search_filter_index * 3)
        driver = uc.Chrome(options=uc.ChromeOptions(), headless=True, driver_executable_path=path)

        driver.get(self.base_url)
        if not self.waited_for_windows_load(driver):
            try:
                driver.quit()
            except Exception as e:
                pass
            return None

        # select physician value = 767
        driver.find_element(By.XPATH, f"//input[@value={search_filter}]").click()
        time.sleep(1)
        # submit the form
        driver.find_element(By.XPATH, "//input[@value='Search']").click()
        time.sleep(1)
        waited = self.wait_for_details(driver, "h2", "Search Results")
        if not waited:
            logger.info("No results found")
            return None

        licence_numbers_for_the_page = 0
        page_num = 0
        has_next_page = True
        while has_next_page:

            page_num += 1
            page_url = self.pagination_url.format(f"{page_num}")
            logger.info(f"Processing page #{page_num} | filter#{search_filter_index} | value: {search_filter}")

            wait_retires = 3
            while wait_retires:
                wait_retires -= 1
                driver.get(page_url)
                if self.waited_for_windows_load(driver):
                    break
                if not wait_retires:
                    try:
                        driver.quit()
                    except OSError:
                        pass  # Silently suppress the OSError
                    except Exception as e:
                        # Handle other exceptions if needed or log them
                        pass

                    return None

            table = driver.find_element(By.XPATH, "//table[@class='resultsTable']")
            if table:
                table_rows = table.find_elements(By.TAG_NAME, "tr")
                if len(table_rows) == 1:
                    has_next_page = False

                for row in table_rows:
                    row_tds = row.find_elements(By.TAG_NAME, "td")
                    if len(row_tds) >= 5:
                        status = row_tds[-1].text.strip()
                        if "ACTIVE" in status:
                            licence_number = row_tds[-2].text.strip()
                            full_name = row_tds[0].text.strip()
                            # Construct the output dictionary
                            record = {
                                "Full_Name": full_name,
                                "License_Type": license_type,
                                "License_Number": licence_number,
                                "Issued": "",
                                "Expired": "01/31/2026",
                                "Status": status,
                                "Professional": professional
                            }

                            if record:
                                self.save_to_csv([record])
                                licence_numbers_for_the_page += 1

            logger.info(f"Total Active licenses Till Now: {licence_numbers_for_the_page}")

        try:
            driver.quit()
        except OSError:
            pass  # Silently suppress the OSError
        except Exception as e:
            # Handle other exceptions if needed or log them
            pass

    def run(self):
        """Run the crawler concurrently for all search items."""

        path = ChromeDriverManager().install()

        # Write CSV headers
        with open(self.output_file, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ["Full_Name", "License_Type", "License_Number", "Status", "Professional", "Issued", "Expired"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

        # Process each specialty concurrently
        with ThreadPoolExecutor(max_workers=1) as executor:
            futures = {
                executor.submit(self.crawl_search_filter, search_filter_index, search_filter, path):
                    search_filter_index for search_filter_index, search_filter in
                enumerate(self.search_filter_list, start=1)
            }

            # Wait for all futures to complete
            for future in as_completed(futures):
                try:
                    # Optionally, you can get the result if needed
                    _ = future.result()  # This will raise an exception if the task failed
                except Exception as e:
                    search_filter_index = futures[future]
                    logger.error(f"Task generated an exception: {e} | {search_filter_index}")

            logger.info("Completed")
            logger.info("Removing duplicates if any.")
            self.remove_duplicates_in_csv()


if __name__ == '__main__':
    crawler = LicenseCrawler()
    crawler.run()
