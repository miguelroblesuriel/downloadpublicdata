import csv
import json

import requests

# Base URL of the dataset
def extract_tsv(number_of_ms2 = 300):
    url = "https://datasetcache.gnps2.org/datasette/database.json"

    # Define the query parameters

    datasets = ("MSV000078982", "MSV000079386")

    # Dynamically construct the SQL query
    placeholders = ", ".join(f"'{dataset}'" for dataset in datasets)  # Creates 'MSV000068104', 'MSV000068136'
    sql_query = f'select usi from uniquemri where "spectra_ms2" >= :p0 and "usi" like :p1 and dataset in ({placeholders}) order by usi'

    params = {
        "sql": sql_query,
        "p0": number_of_ms2,  # Replace this with the desired value for spectra_ms2
        "p1": "%mzML%",  # Replace this with the desired pattern for usi
        "_size": "max",  # Fetch the maximum size allowed
    }

    # Send the GET request to the URL with the query parameters
    response = requests.get(url, params=params)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the JSON content
        data = response.json()
        print("JSON response received successfully.")
        print(data["rows"])

        # Save the JSON data to a file
        with open("data.tsv", "w", newline="") as file:
            writer = csv.writer(file, delimiter='\t')  # Tab as delimiter
            writer.writerow(["usi"])
            writer.writerows(data["rows"])

        print("Data saved to data.tsv")
    else:
        print(f"Failed to fetch the JSON. Status code: {response.status_code}")
        print(f"Response content: {response.text}")