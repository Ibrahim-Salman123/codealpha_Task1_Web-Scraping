import requests
from bs4 import BeautifulSoup
import pandas as pd
from io import StringIO
import re

# List of Wikipedia pages to scrape
urls = [
    "https://en.wikipedia.org/wiki/List_of_countries_and_dependencies_by_population",
    "https://en.wikipedia.org/wiki/List_of_countries_by_GDP_(nominal)",
    "https://en.wikipedia.org/wiki/List_of_countries_by_literacy_rate"
]

all_data = []

headers = {"User-Agent": "Mozilla/5.0"}  # avoid blocking

for url in urls:
    print(f"Connecting to {url} ...")
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        print("Connected successfully.")
        soup = BeautifulSoup(response.text, "html.parser")
        tables = soup.find_all("table", {"class": "wikitable"})
        print(f"Found {len(tables)} tables on the page.")
        
        for i, table in enumerate(tables, start=1):
            df = pd.read_html(StringIO(str(table)))[0]

            # Handle MultiIndex columns (flatten them)
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = [' '.join([str(c) for c in col if str(c) != 'nan']).strip() 
                              for col in df.columns.values]
            else:
                df.columns = df.columns.astype(str).str.strip().str.replace("\n", " ")

            # Add source info
            df["source_page"] = url.split("/")[-1]
            df["table_number"] = i

            # Save individual file
            page_name = re.sub(r'\W+', '_', url.split('/')[-1])  # safe filename
            file_name = f"{page_name}_table_{i}.csv"
            df.to_csv(file_name, index=False, encoding="utf-8")
            print(f"Table {i} saved as {file_name} with {df.shape[0]} rows and {df.shape[1]} columns.")

            # Append to combined dataset
            all_data.append(df)
    else:
        print(f"Failed to connect to {url}, status code: {response.status_code}")

# Create one combined dataset
if all_data:
    final_df = pd.concat(all_data, ignore_index=True)
    final_df.to_csv("combined_wikipedia_data.csv", index=False, encoding="utf-8")
    print(f"Combined dataset saved as combined_wikipedia_data.csv with {final_df.shape[0]} rows and {final_df.shape[1]} columns.")


