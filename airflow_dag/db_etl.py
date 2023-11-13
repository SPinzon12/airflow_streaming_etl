import json
import logging
import random

import numpy as np
import pandas as pd
from uszipcode import SearchEngine

from services.db_postgres import run_query, create_data_warehouse, insert_data_warehouse


def extract_db():
    logging.info("Starting data extraction process...")
    sql='''SELECT *
    FROM used_cars
    LIMIT 30000
    '''
    cars_df = run_query(sql)
    cars_df['listed_date'] = cars_df['listed_date'].dt.strftime('%Y-%m-%d')
    logging.info("Extraction completed")
    logging.info('The shape of the extracted data is: %s', cars_df.shape)
    return cars_df.to_json(orient='records')

def get_state(zip_code, engine, zip_dict):
    if zip_code in zip_dict:
        return zip_dict[zip_code]
    try:
        zipcode = engine.by_zipcode(zip_code)
        state = zipcode.state_long
        zip_dict[zip_code] = state
        return state
    except:
        return np.nan

def create_date_dimension(df):
    date_df = df[['listed_date']].copy()
    date_df['listed_date'] = pd.to_datetime(date_df['listed_date'])
    date_df = date_df.drop_duplicates().reset_index(drop=True)
    date_df['date_key'] = date_df['listed_date'].dt.strftime('%Y%m%d')
    date_df['year'] = date_df['listed_date'].dt.year
    date_df['short_month'] = date_df['listed_date'].dt.strftime('%b')
    date_df['day_of_week'] = date_df['listed_date'].dt.strftime('%A')
    date_df['listed_date'] = date_df['listed_date'].dt.strftime('%Y-%m-%d')
    date_df.rename(columns={'listed_date': 'date'}, inplace=True)
    date_df = date_df[['date_key', 'date', 'short_month', 'day_of_week','year']]
    return date_df

def create_model_dimension(df):
    model_df = df[['make_name', 'model_name', 'body_type']].copy()
    model_df = model_df.drop_duplicates().reset_index(drop=True)
    model_df['full_model_name'] = model_df['make_name'] + ' ' + model_df['model_name']
    model_df['model_key'] = model_df.index
    model_df = model_df[['model_key', 'make_name', 'model_name', 'full_model_name','body_type']]
    return model_df

def create_location_dimension(df):
    location_df = df[['city', 'dealer_state']].copy()
    location_df = location_df.drop_duplicates().reset_index(drop=True)
    location_df['location_key'] = location_df.index
    location_df['city_state'] = location_df['city'] + ', ' + location_df['dealer_state']
    location_df.rename(columns={'dealer_state': 'state'}, inplace=True)
    location_df = location_df[['location_key', 'city', 'state', 'city_state']]
    return location_df

def create_dealer_dimension(df):
    dealer_df = df[['seller_rating', 'sp_name']].copy()
    dealer_df = dealer_df.drop_duplicates().reset_index(drop=True)
    dealer_df = dealer_df.groupby('sp_name').agg({'seller_rating': 'max'}).reset_index()
    dealer_df['dealer_key'] = dealer_df.index
    dealer_df = dealer_df.rename(columns={
        'dealer_key': 'dealer_key',
        'seller_rating': 'reputation_score',
        'sp_name': 'dealer_name',
    })
    dealer_df= dealer_df[['dealer_key','dealer_name','reputation_score']]
    return dealer_df

def create_fact_table(df):
    fact_table = df.drop(columns=[
        'reputation_score',
        'dealer_name',
        'city_state',
        'state',
        'full_model_name',
        'dealer_state',
        'sp_name',
        'seller_rating',
        'model_name',
        'listed_date',
        'city',
        'dealer_zip',
        'body_type',
        'make_name'   
    ])
    
    fact_table = fact_table.rename(columns={
        'vin': 'vehicle_vin',
        'daysonmarket': 'days_on_market',
        'engine_type': 'engine',
        'frame_damaged': 'has_accidents',  
        'listing_color': 'color',  
        'maximum_seating': 'seating_capacity',
        'transmission': 'transmission',
        'wheel_system_display': 'drivetrain',
        'year': 'vehicle_year',
    })

    return fact_table

def transform_db(**kwargs):
    logging.info("Starting data transformation process...")
    ti = kwargs["ti"]
    json_data = json.loads(ti.xcom_pull(task_ids="read_db"))
    cars_df = pd.json_normalize(data=json_data)

    # Drop columns
    cars_df.drop(columns=[
        'bed', 'bed_height', 'bed_length', 'cabin', 'combine_fuel_economy',
        'is_certified', 'is_cpo', 'is_oemcpo', 'vehicle_damage_category',
        'engine_cylinders', 'exterior_color', 'franchise_make', 'wheel_system',
        'transmission_display', 'latitude', 'longitude', 'back_legroom',
        'front_legroom', 'power', 'has_accidents', 'salvage', 'theft_title',
        'fleet', 'engine_displacement', 'description', 'interior_color',
        'franchise_dealer', 'main_picture_url', 'major_options', 'iscab',
        'trimid', 'trim_name', 'city_fuel_economy', 'highway_fuel_economy',
        'length', 'height', 'width', 'torque', 'wheelbase', 'fuel_tank_volume',
        'savings_amount', 'sp_id', 'listing_id'
    ], inplace=True)

    # Replace NaNs with np.nan 
    cars_df.replace('NaN', np.nan, inplace=True)

    # Remove price outliers
    mean_price = cars_df['price'].mean()
    std_price = cars_df['price'].std()
    threshold = 2 * std_price
    outliers_price = cars_df[(cars_df['price'] < (mean_price - threshold)) | (cars_df['price'] > (mean_price + threshold))]
    logging.info('Number of price outliers: %s', len(outliers_price))
    cars_df = cars_df[(cars_df['price'] >= (mean_price - threshold)) & (cars_df['price'] <= (mean_price + threshold))]
    cars_df['price'] = cars_df['price'].astype(int)

    # Remove mileage outliers and fill NaNs
    mean_mileage = cars_df['mileage'].mean()
    std_mileage = cars_df['mileage'].std()
    threshold = 2 * std_mileage
    outliers_mileage = cars_df[(cars_df['mileage'] < (mean_mileage - threshold)) | (cars_df['mileage'] > (mean_mileage + threshold))]
    logging.info('Number of mileage outliers: %s', len(outliers_mileage))
    cars_df = cars_df[(cars_df['mileage'] >= (mean_mileage - threshold)) & (cars_df['mileage'] <= (mean_mileage + threshold))]
    cars_df['mileage'].fillna(mean_mileage, inplace=True)
    cars_df['mileage'] = cars_df['mileage'].astype(int)

    # Remove rows where 'body_type' is NaN 
    body_type_na = cars_df['body_type'].isna().sum()
    logging.info('Number of rows removed due to NaN in body_type: %s', body_type_na)
    cars_df = cars_df[pd.notna(cars_df['body_type'])]

    # Remove rows where 'fuel_type' is NaN
    fuel_type_na = cars_df['fuel_type'].isna().sum()
    logging.info('Number of rows removed due to NaN in fuel_type: %s', fuel_type_na)
    cars_df = cars_df[pd.notna(cars_df['fuel_type'])]

    # Fill NaNs in 'engine_type' with the most common engine type for that fuel type
    cars_df['engine_type'] = cars_df['engine_type'].str.split(' ').str[0]
    cars_df.loc[pd.isna(cars_df['engine_type']) & (cars_df['fuel_type'] == "Electric"), 'engine_type'] = "Electric"
    cars_df.loc[pd.isna(cars_df['engine_type']) & (cars_df['fuel_type'] == "Diesel"), 'engine_type'] = "Diesel"
    cars_df.loc[pd.isna(cars_df['engine_type']) & (cars_df['fuel_type'] == "Gasoline"), 'engine_type'] = "Gasoline"
    cars_df.loc[pd.isna(cars_df['engine_type']) & (cars_df['fuel_type'] == "Gasoline"), 'engine_type'] = "Gasoline"

    # Fill NaN values in 'transmission' with 'A'
    cars_df['transmission'].fillna('A', inplace=True)

    # Fill NaNs in 'wheel_system_display' with 'All-Wheel Drive'
    cars_df['wheel_system_display'].fillna('All-Wheel Drive', inplace=True)

    # Replace transmission codes with full names
    cars_df['transmission'] = cars_df['transmission'].replace({
        'A': 'Automatic', 
        'CVT': 'Continuously Variable Transmission', 
        'M': 'Manual'
    })

    # Capitalize first letter of 'listing_color'
    cars_df['listing_color'] = cars_df['listing_color'].str.capitalize()

    # Fill NaNs with random values in range 180-200
    cars_df['horsepower'] = cars_df['horsepower'].apply(lambda x: random.uniform(180, 200) if pd.isna(x) else x).astype(int)

    # Fill NaNs in 'maximum_seating' with default values based on 'body_type'
    cars_df['maximum_seating'].replace(["--"], np.nan, inplace=True)
    default_seating_values = {'Convertible': '4', 'Coupe': '4', 'Hatchback': '5', 'Minivan': '8', 'Pickup Truck': '6', 'SUV / Crossover': '5', 'Sedan': '5', 'Van': '2', 'Wagon': '5'}
    cars_df['maximum_seating'] = cars_df.apply(lambda row: default_seating_values.get(row['body_type'], str(row['maximum_seating']).replace(" seats", "")) if pd.isna(row['maximum_seating']) else str(row['maximum_seating']).replace(" seats", ""), axis=1).astype(int)

    # Fill NaNs in 'seller_rating' with 0 and round up
    cars_df['seller_rating'] = cars_df['seller_rating'].apply(np.ceil).fillna(0).astype(int)

    # Fill NaNs in 'owner_count' with -1    
    cars_df['owner_count'] = cars_df['owner_count'].fillna(-1).astype(int)

    # Fill NaNs in 'accident_count' with 0
    cars_df['frame_damaged'] = cars_df['frame_damaged'].fillna('false').replace({'false': 0, 'true': 1})

    # Replace NaNs in 'frame_damaged' with 'false'
    cars_df['is_new'] = cars_df['is_new'].replace({False: 0, True: 1})

    # Get state
    engine = SearchEngine()
    zip_dict = {}
    cars_df['dealer_state'] = cars_df['dealer_zip'].apply(lambda x: get_state(x, engine, zip_dict))

    # Date dimension
    date_df = create_date_dimension(cars_df)
    cars_df['listed_date'] = pd.to_datetime(cars_df['listed_date'])
    cars_df['date_key'] = cars_df['listed_date'].dt.strftime('%Y%m%d')
    ti.xcom_push(key='date_dimension', value=date_df.to_json(orient='records'))

    # Model dimension
    model_df = create_model_dimension(cars_df)
    cars_df = cars_df.merge(model_df, on=['make_name', 'model_name', 'body_type'], how='left')
    ti.xcom_push(key='model_dimension', value=model_df.to_json(orient='records'))
    
    # Location dimension
    location_df = create_location_dimension(cars_df)
    cars_df = cars_df.merge(location_df, left_on=['dealer_state','city'],right_on=['state','city'], how='left')
    ti.xcom_push(key='location_dimension', value=location_df.to_json(orient='records'))

    # Dealer dimension
    dealer_df = create_dealer_dimension(cars_df)
    cars_df = cars_df.merge(dealer_df, left_on=['sp_name'],right_on=['dealer_name'], how='left')
    ti.xcom_push(key='dealer_dimension', value=dealer_df.to_json(orient='records')) 

    # Fact table
    fact_table = create_fact_table(cars_df)
    
    logging.info("Transformation completed")
    logging.info('The shape of the transformed data is: %s', fact_table.shape)

    return fact_table.to_json(orient='records')

def load_db(**kwargs):
    logging.info("Starting data loading process...")
    ti = kwargs["ti"]
    
    create_data_warehouse()
    
    fact_table = pd.json_normalize(json.loads(ti.xcom_pull(task_ids="transform_db")))
    logging.info('Number of rows loaded into fact_used_cars: %s', len(fact_table))
    insert_data_warehouse(fact_table,'fact_used_cars')

    date_df = pd.json_normalize(json.loads(ti.xcom_pull(task_ids="transform_db", key='date_dimension')))
    logging.info('Number of rows loaded into dim_date: %s', len(date_df))
    insert_data_warehouse(date_df,'dim_date')

    model_df = pd.json_normalize(json.loads(ti.xcom_pull(task_ids="transform_db", key='model_dimension')))
    logging.info('Number of rows loaded into dim_model: %s', len(model_df))
    insert_data_warehouse(model_df,'dim_model')

    location_df = pd.json_normalize(json.loads(ti.xcom_pull(task_ids="transform_db", key='location_dimension')))
    logging.info('Number of rows loaded into dim_location: %s', len(location_df))
    insert_data_warehouse(location_df,'dim_location')

    dealer_df = pd.json_normalize(json.loads(ti.xcom_pull(task_ids="transform_db", key='dealer_dimension')))
    logging.info('Number of rows loaded into dim_dealer: %s', len(dealer_df))
    insert_data_warehouse(dealer_df,'dim_dealer')

    logging.info("Data loaded into data warehouse")