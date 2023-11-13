import json
import logging

import numpy as np
import pandas as pd

from services.api import extract_data_api
from services.db_postgres import create_table_api, insert_data_api


def extract_api():
    logging.info("Starting data extraction process...")
    anime_df = extract_data_api()
    logging.info("Extraction completed")
    logging.info('The shape of the extracted data is: %s', anime_df.shape)
    return anime_df.to_json(orient="records")

def transform_api(**kwargs):
    logging.info("Starting data transformation process...")

    ti = kwargs["ti"]
    anime_df = pd.json_normalize(json.loads(ti.xcom_pull(task_ids="read_api")))

    # Drop columns that are not needed
    anime_df.drop(
        columns=[
            'demographic', 
            'aired_to', 
            'broadcast_time', 
            'broadcast_timezone', 
            'rank', 
            'studio_id', 
            'producer_id'
        ], 
        inplace=True
    )

    # Remove rows where score is null
    anime_df = anime_df[pd.notna(anime_df['score'])]

    # Remove rows where rating is null and replace rating values with more general values
    anime_df = anime_df[pd.notna(anime_df['rating'])]
    anime_df['rating'].replace({
        'PG-13 - Teens 13 or older': 'Teens',
        'G - All Ages': 'All Ages',
        'R - 17+ (violence & profanity)': 'Mature',
        'R+ - Mild Nudity': 'Mature',
        'PG - Children': 'All Ages',
        'Rx - Hentai': 'Mature'
    }, inplace=True)

    # Remove rows where genre is null
    anime_df = anime_df[pd.notna(anime_df['genre'])]

    # Remove rows where type is null
    anime_df['type'] = anime_df['type'].replace('Music', np.nan)
    anime_df = anime_df[pd.notna(anime_df['type'])]

    # Fill null values in episodes column with default values based on type
    default_seating_values = {'Movie': 1.0, 'ONA': 12.0, 'OVA': 1.0, 'Special': 1.0, 'TV': 12.0}
    anime_df['episodes'] = anime_df.apply(lambda row: default_seating_values.get(row['type'], row['episodes']) if pd.isna(row['episodes']) else row['episodes'], axis=1).astype(int)

    # Fill null values in year with aired_from
    anime_df['aired_from'] = pd.to_datetime(anime_df['aired_from'], errors='coerce')
    anime_df['year'] = anime_df['aired_from'].dt.year

    # Fill null values in season with month
    month_to_season = {
        1: 'winter', 2: 'winter', 3: 'winter',
        4: 'spring', 5: 'spring', 6: 'spring',
        7: 'summer', 8: 'summer', 9: 'summer',
        10: 'fall', 11: 'fall', 12: 'fall'
    }
    anime_df['season'] = anime_df['season'].fillna(anime_df['aired_from'].dt.month.map(month_to_season))

    # Fill null values in broadcast_day with aired_from
    anime_df['broadcast_day'] = anime_df['broadcast_day'].str.rstrip('s')
    anime_df['broadcast_day'].fillna(anime_df['aired_from'].dt.day_name(), inplace=True)

    # Fill null values in producer with Unknown
    anime_df['producer'].fillna('Unknown', inplace=True)

    # Fill null values in studio with Unknown
    anime_df['studio'].fillna('Unknown', inplace=True)

    # Rename columns
    anime_df = anime_df.rename(columns={
        'genre': 'genres',
        'rating': 'classification',
        'broadcast_day': 'airing_day',
        'producer': 'production',
        'studio': 'animation_studio',
        'type': 'content_type'
    })

    anime_df['aired_from'] = anime_df['aired_from'].dt.strftime('%Y-%m-%d')

    logging.info("Transformation completed")
    logging.info('The shape of the transformed data is: %s', anime_df.shape)

    return anime_df.to_json(orient="records")

def load_api(**kwargs):
    logging.info("Starting data loading process...")

    create_table_api()

    ti = kwargs["ti"]
    anime_df = pd.json_normalize(json.loads(ti.xcom_pull(task_ids="transform_api")))

    insert_data_api(anime_df)

    logging.info("Loading completed")