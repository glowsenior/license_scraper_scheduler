from flask import Flask, render_template, jsonify
import scraper_manager  # Import the scraper management script

app = Flask(__name__)

@app.route("/")
def home():
    bot_statuses = scraper_manager.get_all_bot_statuses()
    return render_template("index.html", bot_statuses=bot_statuses)

@app.route("/get_bot_statuses")
def get_bot_statuses():
    bot_statuses = scraper_manager.get_all_bot_statuses()
    return jsonify(bot_statuses)

@app.route("/trigger_scrape")
def trigger_scrape():
    scraper_manager.trigger_scraping_cycle() # Manually trigger a scrape cycle
    return "Scraping cycle triggered!", 200 # or redirect back to the main page

if __name__ == "__main__":
    app.run(debug=True) # debug=True for development only