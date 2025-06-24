# 🚗 European Used Car Price Scraper

### Context & Problem  
As a data analyst working with **B2B used car transaction data**, I realized we lacked visibility into the **B2C side of the market**. Understanding consumer-facing price trends across European countries could bridge that gap — and help explain price dynamics in wholesale auctions.

### Solution & Action  
To address this, I built a Python-based web scraper targeting [AutoScout24], one of the largest online marketplaces for used cars in Europe. The script collects structured data including:
- Make, model, fuel type, transmission
- First registration, mileage, and price
- Geo-location (country, city, ZIP)
- Scraping timestamp

It loops through listings from 🇧🇪 Belgium, 🇳🇱 Netherlands, 🇩🇪 Germany, 🇫🇷 France, and 🇮🇹 Italy — collecting up to 50 pages per country, and appends new results to a local CSV file (`used_car_prices.csv`), maintaining a historical data trail.

### Result  
This tool enables continuous monitoring of B2C pricing trends. By aligning them with B2B auction price data, we can:
- Identify pricing deltas between market segments
- Detect seasonal or geographic trends
- Support pricing strategy, inventory decisions, and market forecasting

> 📌 Built with `requests`, `BeautifulSoup`, and `pandas`.  
> 🔁 Ready for scheduled automation via task schedulers (e.g. Windows Task Scheduler or cron).

---

### 📁 Sample Output Columns
| make | model | price | mileage | first_registration | fuel_type | country | city | date |
|------|-------|-------|---------|---------------------|-----------|---------|------|------|

---

### 🛠 To Run
1. Clone this repo
2. Install dependencies: `pip install requests beautifulsoup4 pandas`
3. Run the script: `python scraper.py`
