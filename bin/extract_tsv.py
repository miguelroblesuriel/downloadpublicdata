
import csv
import json
import requests
import argparse

def extract_tsv(datasets_file, number_of_ms2):
    url = "https://datasetcache.gnps2.org/datasette/database.json"

    # Read datasets from the file
    with open(datasets_file, "r") as file:
        datasets = [line.strip() for line in file if line.strip()]

    # Dynamically construct the SQL query
    placeholders = ", ".join(f"'{dataset}'" for dataset in datasets)
    sql_query = f'select usi from uniquemri where "spectra_ms2" >= :p0 and "usi" like :p1 and dataset in ({placeholders}) order by usi'

    params = {
        "sql": sql_query,
        "p0": number_of_ms2,
        "p1": "%mzML%",
        "_size": "max",
    }

    # Send the GET request to the URL with the query parameters
    response = requests.get(url, params=params)

    # Check if the request was successful
    if response.status_code == 200:
        data = response.json()
        print("JSON response received successfully.")
        print(data["rows"])

        # Save the JSON data to a file
        with open("data.tsv", "w", newline="") as file:
            writer = csv.writer(file, delimiter='\t')
            writer.writerow(["usi"])
            writer.writerows(data["rows"])

        print("Data saved to data.tsv")
    else:
        print(f"Failed to fetch the JSON. Status code: {response.status_code}")
        print(f"Response content: {response.text}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract USI data from a remote JSON API.")
    parser.add_argument("datasets_file", help="Path to the file containing the list of datasets.")
    parser.add_argument("number_of_ms2", type=int, help="Minimum number of MS2 spectra.")
    args = parser.parse_args()

    extract_tsv(args.datasets_file, args.number_of_ms2)