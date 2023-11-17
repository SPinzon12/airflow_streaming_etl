import pandas as pd

from airflow_dag.services.api import extract_data_api
from airflow_dag.services.db_postgres import create_table, insert_data


if __name__ == "__main__":
    print("Starting data extraction process...")
    create_table()

    # Extract data from csv file
    print("Reading data from CSV file...")
    cars_df = pd.read_csv("./data/used_cars_data.csv", dtype={"dealer_zip": str})
    insert_data(cars_df)

    # Extract data from API
    print("Extracting data from API...")
    anime_df = extract_data_api()
    anime_df.to_csv('./data/anime_data.csv')

    print("Data extraction process completed.")