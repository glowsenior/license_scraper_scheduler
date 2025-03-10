import schedule
import time
import scraper_manager  # Import your scraper manager

def job():
    print("Running scraper job...")
    scraper_manager.trigger_scraping_cycle()
    print("Scraper job finished.")

# Schedule to run every 2 weeks
schedule.every(2).weeks.do(job)

if __name__ == "__main__":
    print("Scheduler started. Will run scraper every 2 weeks.")
    while True:
        schedule.run_pending()
        time.sleep(1) # Check for pending jobs every second