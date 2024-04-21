import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import requests
from bs4 import BeautifulSoup
import time
import pickle

# Data fetching and serialization functions
def scrape_brewers_association_stats(url):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    data = []
    session = requests.Session()
    while url:
        try:
            response = session.get(url, headers=headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            table = soup.find('table')
            if table:
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all('td')
                    if cells:
                        extracted = [cell.text.strip() for cell in cells]
                        data.append(extracted)
            next_page = soup.find('a', attrs={'rel': 'next'})
            url = next_page['href'] if next_page else None
        except requests.RequestException as e:
            st.error(f"Request failed: {e}")
            break
    return data

class Brewery:
    def __init__(self, name, brewery_type, website=None):
        self.name = name
        self.brewery_type = brewery_type
        self.website = website

def fetch_and_serialize_breweries(params, filepath='breweries.pkl'):
    base_url = "https://api.openbrewerydb.org/breweries"
    breweries = []
    session = requests.Session()
    while True:
        response = session.get(base_url, params=params)
        if 'X-RateLimit-Reset' in response.headers:
            reset_time = float(response.headers['X-RateLimit-Reset'])
            time.sleep(max(0, reset_time - time.time()))
        if not response.json():
            break
        for item in response.json():
            breweries.append(Brewery(item['name'], item['brewery_type'], item.get('website_url')))
        params['page'] = params.get('page', 1) + 1
    with open(filepath, 'wb') as f:
        pickle.dump(breweries, f)
    return breweries

# Data loading and preprocessing
def load_and_preprocess_sales_data(filepath):
    try:
        sales_data = pd.read_csv(filepath, on_bad_lines='skip')
        if 'sales_date' not in sales_data.columns:
            raise ValueError("No 'sales_date' column found.")
        sales_data['sales_date'] = pd.to_datetime(sales_data['sales_date'], errors='coerce')
        sales_data.dropna(subset=['sales_date', 'RETAIL SALES'], inplace=True)
        return sales_data, 'sales_date'
    except Exception as e:
        st.error(f"Failed to load or process data: {e}")
        return pd.DataFrame(), ''

def main():
    st.title('Craft Beer Industry Analysis')
    url = st.text_input('Enter URL for web scraping:', 'https://www.brewersassociation.org/statistics-and-data/national-beer-stats/')
    if st.button('Fetch Data'):
        data = scrape_brewers_association_stats(url)
        if data:
            st.write("Successfully retrieved data.")
            st.write(data)
        else:
            st.write("Failed to retrieve data.")

    sales_data_path = st.text_input('Enter the path to the sales data CSV:', 'path/to/Warehouse_and_Retail_Sales.csv')
    sales_data, date_col = load_and_preprocess_sales_data(sales_data_path)
    if not sales_data.empty:
        date_range = st.sidebar.date_input("Select Date Range", [])
        if date_range:
            filtered_data = sales_data[(sales_data[date_col] >= date_range[0]) & (sales_data[date_col] <= date_range[1])]
        else:
            filtered_data = sales_data
        if not filtered_data.empty:
            fig, ax = plt.subplots()
            sns.lineplot(data=filtered_data, x=date_col, y='RETAIL SALES', ax=ax)
            plt.title('Craft Beer Sales Over Time')
            plt.xlabel('Date')
            plt.ylabel('Sales')
            st.pyplot(fig)
        else:
            st.write("No data available for the selected range.")
    else:
        st.write("No data loaded. Please check the file path or data file.")

if __name__ == "__main__":
    main()
