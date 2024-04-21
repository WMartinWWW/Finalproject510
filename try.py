import requests


def scrape_brewers_association_stats(url):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    data = []
    session = requests.Session()
    while url:
        try:
            print(f"Fetching data from: {url}")
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
                        print("Extracted row:", extracted)  # Print each extracted row for transparency
                        data.append(extracted)
            next_page = soup.find('a', attrs={'rel': 'next'})
            url = next_page['href'] if next_page else None  # Navigate to the next page if available
        except requests.RequestException as e:
            print(f"Request failed: {e}")
            break
    return data

url = "https://www.brewersassociation.org/statistics-and-data/national-beer-stats/"
data = scrape_brewers_association_stats(url)
if data:
    print("Successfully retrieved data. Total rows extracted:", len(data))
else:
    print("Failed to retrieve data.")

import requests
import pickle
import time

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
    return breweries  # Return the list of Brewery objects instead of a string

print(fetch_and_serialize_breweries(params={'per_page': 50}))

import pandas as pd
def load_and_preprocess_sales_data(filepath):
    try:

        sales_data = pd.read_csv(filepath, on_bad_lines='skip', dtype={'product_id': str})


        if 'sales_date' in sales_data.columns:
            sales_data['sales_date'] = pd.to_datetime(sales_data['sales_date'], errors='coerce')
        else:
            print("Warning: 'sales_date' column not found. Proceeding without date conversion.")

        # Data Cleaning and Preprocessing Steps
        sales_data.dropna(inplace=True)  # Remove rows with missing values

        # Print a preview of the data and a summary
        print(sales_data.head())
        print(f"Sales data contains {sales_data.shape[0]} rows and {sales_data.shape[1]} columns.")
        return sales_data
    except pd.errors.ParserError as e:
        print(f"ParserError: {e}")
        return None
    except Exception as e:
        print(f"Failed to load sales data: {e}")
        return None

sales_data_path = "/Users/w./Desktop/DS549/510 final/Warehouse_and_Retail_Sales.csv"
sales_data = load_and_preprocess_sales_data(sales_data_path)

import pickle
class DataManager:
    def __init__(self, beer_stats, breweries, sales_data):
        # Data from web scraping
        self.beer_stats = beer_stats

        self.breweries = {brewery.name: brewery for brewery in breweries}

        self.sales_data = sales_data

    def save_data(self, filepath):

        with open(filepath, 'wb') as f:
            pickle.dump(self, f)
        print(f"DataManager saved to {filepath}")

    @staticmethod
    def load_data(filepath):

        with open(filepath, 'rb') as f:
            return pickle.load(f)

# Fetching and organizing data collected in previous cells
beer_stats = data
breweries = fetch_and_serialize_breweries({'per_page': 50})
sales_data = load_and_preprocess_sales_data("/Users/w./Desktop/DS549/510 final/Warehouse_and_Retail_Sales.csv")


data_manager = DataManager(beer_stats, breweries, sales_data)
filepath = 'data_manager.pkl'
# Save the DataManager to disk
data_manager.save_data(filepath)

loaded_data_manager = DataManager.load_data(filepath)


print("DataManager loaded successfully with the following data overview:")
print(f"Total breweries: {len(loaded_data_manager.breweries)}")
print(f"Total sales records: {len(loaded_data_manager.sales_data)}")

import pickle
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd


filepath = 'data_manager.pkl'
with open(filepath, 'rb') as f:
    data_manager = pickle.load(f)


breweries = data_manager.breweries
sales_data = data_manager.sales_data
beer_stats = data_manager.beer_stats

# Function to display summary information about breweries
def summarize_breweries(breweries):
    print(f"Total breweries loaded: {len(breweries)}")
    brewery_types = {}
    for brewery in breweries.values():
        if brewery.brewery_type in brewery_types:
            brewery_types[brewery.brewery_type] += 1
        else:
            brewery_types[brewery.brewery_type] = 1
    print("Brewery types and their counts:")
    for brewery_type, count in brewery_types.items():
        print(f"{brewery_type}: {count}")

# Function to visualize the distribution of breweries by type
def plot_brewery_distribution(breweries):
    types = [brewery.brewery_type for brewery in breweries.values()]
    plt.figure(figsize=(10, 5))
    sns.countplot(x=types, palette='viridis')
    plt.title('Distribution of Breweries by Type')
    plt.xlabel('Brewery Type')
    plt.ylabel('Count')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

# Sales Data Analysis
def sales_data_analysis(sales_data):
    if not sales_data.empty:
        plt.figure(figsize=(12, 6))
        top_products = sales_data.groupby('ITEM DESCRIPTION').sum()['RETAIL SALES'].nlargest(10)
        sns.barplot(x=top_products.values, y=top_products.index, palette='coolwarm')
        plt.title('Top 10 Products by Retail Sales')
        plt.xlabel('Total Sales')
        plt.ylabel('Product')
        plt.tight_layout()
        plt.show()

# Execute the summary and plotting functions
summarize_breweries(breweries)
plot_brewery_distribution(breweries)
sales_data_analysis(pd.DataFrame(sales_data))

import pandas as pd
def load_and_preprocess_sales_data(filepath):
    try:
        sales_data = pd.read_csv(filepath, on_bad_lines='skip', dtype={'product_id': str})

        if 'sales_date' in sales_data.columns:
            sales_data['sales_date'] = pd.to_datetime(sales_data['sales_date'], errors='coerce')
        else:
            print("Warning: 'sales_date' column not found. Proceeding without date conversion.")

        # Data Cleaning and Preprocessing Steps
        sales_data.dropna(inplace=True)  # Remove rows with missing values

        # Print a preview of the data and a summary
        print(sales_data.head())
        print(f"Sales data contains {sales_data.shape[0]} rows and {sales_data.shape[1]} columns.")
        return sales_data
    except pd.errors.ParserError as e:
        print(f"ParserError: {e}")
        return None
    except Exception as e:
        print(f"Failed to load sales data: {e}")
        return None

# Example use of the function:
sales_data_path = "/Users/w./Desktop/DS549/510 final/Warehouse_and_Retail_Sales.csv"  # Update this to the correct path
sales_data = load_and_preprocess_sales_data(sales_data_path)

import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def load_and_preprocess_sales_data(filepath):
    try:
        sales_data = pd.read_csv(filepath, on_bad_lines='skip')
        date_col = None
        for col in sales_data.columns:
            if 'date' in col.lower():
                date_col = col
                break
        if date_col is None:
            raise ValueError("No column suitable for 'sales_date' found.")

        sales_data[date_col] = pd.to_datetime(sales_data[date_col], errors='coerce')
        sales_data.dropna(subset=[date_col], inplace=True)
        if 'RETAIL SALES' in sales_data.columns:
            sales_data['RETAIL SALES'] = pd.to_numeric(sales_data['RETAIL SALES'], errors='coerce')
            sales_data.dropna(subset=['RETAIL SALES'], inplace=True)

        return sales_data, date_col
    except Exception as e:
        st.error(f"Failed to load or process data: {e}")
        return pd.DataFrame(), ''

def plot_sales_over_time(data, date_col):
    fig, ax = plt.subplots()
    sns.lineplot(data=data, x=date_col, y='RETAIL SALES', ax=ax)
    plt.title('Craft Beer Sales Over Time')
    plt.xlabel('Date')
    plt.ylabel('Sales')
    return fig

def main():
    st.title('Craft Beer Industry Analysis')

    # Load data
    sales_data_path = st.text_input('Enter the path to the sales data CSV:', '/Users/w./Desktop/DS549/510 final/Warehouse_and_Retail_Sales.csv')
    sales_data, date_col = load_and_preprocess_sales_data(sales_data_path)

    if not sales_data.empty:
        # Sidebar for user input features
        date_range = st.sidebar.date_input("Select Date Range", [])
        if date_range:
            filtered_data = sales_data[(sales_data[date_col] >= date_range[0]) & (sales_data[date_col] <= date_range[1])]
        else:
            filtered_data = sales_data

        # Visualization
        if not filtered_data.empty:
            fig = plot_sales_over_time(filtered_data, date_col)
            st.pyplot(fig)
            st.write("Filtered Data", filtered_data)
        else:
            st.write("No data available for the selected range.")
    else:
        st.write("No data loaded. Please check the file path or data file.")

if __name__ == "__main__":
    main()
