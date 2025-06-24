import requests
from bs4 import BeautifulSoup
import json
import pandas as pd
from datetime import datetime
import os

# Function to scrape data
def scrape_data():
    # List of country codes
    country_codes = ['be', 'nl', 'de', 'fr', 'it']
    
    # Initialize an empty list to store car information
    cars_info = []
    
    # Create a session object
    session = requests.Session()
    
    # Current date
    current_date = datetime.now().strftime('%Y-%m-%d')
    
    # Loop through each country code
    for country_code in country_codes:
        # Loop through the first 50 pages for each country
        for page in range(1, 51):
            # Define the URL
            # Include '/nl/' in the URL path only if the country code is 'be'
            url_path = '/nl' if country_code == 'be' else ''
            products_list_url = f'https://www.autoscout24.{country_code}{url_path}/lst?atype=C&page={page}'
            prod_url = f'https://www.autoscout24.{country_code}'

            
            # Send a GET request to the products list URL
            response = session.get(products_list_url)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find the script tag with the JSON data
            script_tag = soup.find('div', class_='main-app').find('script', type='application/json')
            
            # Parse the JSON data
            data = json.loads(script_tag.contents[0])
            
            # Navigate to the listings
            listings = data['props']['pageProps']['listings']
            
            # Extract car information for each listing
            for listing in listings:
                # Extract the required details
                make = listing['vehicle']['make']
                model = listing['vehicle']['model']
                url = prod_url + listing['url']
                price = listing['tracking']['price']
                mileage = listing['tracking']['mileage']
                first_registration = listing['tracking']['firstRegistration']
                fuel_type = listing['tracking']['fuelType']
                transmission = listing['vehicleDetails'][1]['data']  # Assuming the second dictionary contains the transmission
                car_type = listing['vehicleDetails'][3]['data']  # Assuming the fourth dictionary contains the car type
                country = listing['location']['countryCode']
                zipcode = listing['location']['zip']
                city = listing['location']['city']
                
                # Append the current date to the car information
                listing['scraping_date'] = current_date
                
                # Append the car information to the list
                cars_info.append({
                 'make': make,
                 'model': model,
                 'url': url,
                 'price': price,
                 'mileage': mileage,
                 'first_registration': first_registration,
                 'fuel_type': fuel_type,
                 'transmission': transmission,
                 'car_type': car_type,
                 'country': country,
                 'zip_code': zipcode,
                 'city': city,
                 'date': current_date
        })
    
    # Convert the list of car information to a DataFrame
    new_data = pd.DataFrame(cars_info)
    
    # Read the existing data
    try:
        existing_data = pd.read_csv(r'C:\Users\ninh\used_car_prices.csv')
        # Combine the new data with the existing data
        updated_data = pd.concat([existing_data, new_data]).drop_duplicates()
    except FileNotFoundError:
        # If the file does not exist, use the new data as the updated data
        updated_data = new_data

    try:
        existing_data = pd.read_csv(r'C:\Users\ninh\used_car_prices.csv')
        # Combine the new data with the existing data
        updated_data = pd.concat([existing_data, new_data]).drop_duplicates()
    except FileNotFoundError:
        # If the file does not exist, use the new data as the updated data
        updated_data = new_data
    
    # Save the updated data to a CSV file
    csv_file_path = r'C:\Users\ninh\used_car_prices.csv'
    updated_data.to_csv(csv_file_path, index=False)
    
    # Create a running log file
    log_file_path = os.path.join(os.path.dirname(csv_file_path), 'running_log.txt')
    with open(log_file_path, 'w') as log_file:
        log_file.write('Script ran successfully on ' + current_date)

# Run the scrape_data function
scrape_data()
