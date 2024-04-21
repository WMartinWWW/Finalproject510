import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import requests
from bs4 import BeautifulSoup
import time
import pickle

# Function to scrape brewery statistics
def scrape_brewers_association_stats(url):
    headers = {'User-Agent': 'Mozilla/5.0'}
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
                        data.append([cell.text.strip() for cell in cells])
            next_page = soup.find('a', attrs={'rel': 'next'})
            url = next_page['href'] if next_page else None
        except requests.RequestException as e:
            st.error(f"Request failed: {e}")
            break
    return data

# Function to fetch and serialize breweries
def fetch_and_serialize_breweries(params, filepath='breweries.pkl'):
    base_url = "https://api.openbrewerydb.org/breweries"
    breweries = []
    session = requests.Session()
    while True:
        response = session.get(base_url, params=params)
        if 'X-RateLimit-Reset' in response.headers:
            time.sleep(max(0, float(response.headers['X-RateLimit-Reset']) - time.time()))
        if not response.json():
            break
        for item in response.json():
            breweries.append({'name': item['name'], 'type': item['brewery_type'], 'website': item.get('website_url')})
        params['page'] = params.get('page', 1) + 1
    with open(filepath, 'wb') as f:
        pickle.dump(breweries, f)
    return breweries

# Function to load and preprocess sales data

    
    
    
def load_and_preprocess_sales_data(filepath):
    try:
        sales_data = pd.read_csv(filepath, on_bad_lines='skip')
        # Attempt to automatically detect a date column
        date_col = next((col for col in sales_data.columns if 'date' in col.lower()), None)
        if not date_col:
            st.warning("Date column not found. Proceeding without date conversion.")
            return sales_data, None  # Return None for date_col if not found

        sales_data[date_col] = pd.to_datetime(sales_data[date_col], errors='coerce')
        sales_data.dropna(subset=[date_col], inplace=True)
        return sales_data, date_col
    except Exception as e:
        st.error(f"Failed to load sales data: {e}")
        return pd.DataFrame(), None  # Ensure to return two values, even in case of error
    

# Class to manage data
class DataManager:
    def __init__(self, beer_stats, breweries, sales_data):
        self.beer_stats = beer_stats
        self.breweries = breweries
        self.sales_data = sales_data

    def save_data(self, filepath):
        with open(filepath, 'wb') as f:
            pickle.dump(self, f)
        st.info(f"DataManager saved to {filepath}")

    @staticmethod
    def load_data(filepath):
        with open(filepath, 'rb') as f:
            return pickle.load(f)

# Streamlit interface
def main():
    st.title('Craft Beer Industry Analysis Dashboard')

    file_path = st.text_input("Enter path to sales data CSV:", "/Users/w./Desktop/DS549/510 final/Warehouse_and_Retail_Sales.csv")
    if file_path:
        sales_data, date_col = load_and_preprocess_sales_data(file_path)
        if not sales_data.empty:
            st.subheader("Sales Data Preview")
            st.write(sales_data.head())

            if date_col:
                st.subheader("Sales Over Time")
                fig, ax = plt.subplots()
                sns.lineplot(data=sales_data, x=date_col, y='RETAIL SALES', ax=ax)
                plt.title('Retail Sales Over Time')
                plt.xlabel('Date')
                plt.ylabel('Sales ($)')
                st.pyplot(fig)
            else:
                st.error("No date column available for plotting.")
        else:
            st.error("No sales data loaded. Please check the file path or data file.")

if __name__ == "__main__":
    main()
