import json

import pandas as pd
import psycopg2


def create_connection():
    try:
        with open('db_config.json') as file:
            config = json.load(file)
        cnx = psycopg2.connect(
            host='localhost',
            user=config["user"],
            password=config["password"],
            database=config["database"]
        )
    except psycopg2.Error as e:
        cnx = None
        print('Unable to connect: %s', e)
    return cnx

def create_table():
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS used_cars(
        vin VARCHAR(255),
        back_legroom VARCHAR(255),
        bed VARCHAR(255),
        bed_height VARCHAR(255),
        bed_length VARCHAR(255),
        body_type VARCHAR(255),
        cabin VARCHAR(255),
        city VARCHAR(255),
        city_fuel_economy FLOAT,
        combine_fuel_economy FLOAT,
        daysonmarket BIGINT,
        dealer_zip TEXT,
        description TEXT,
        engine_cylinders VARCHAR(255),
        engine_displacement FLOAT,
        engine_type VARCHAR(255),
        exterior_color VARCHAR(255),
        fleet VARCHAR(255),
        frame_damaged VARCHAR(255),
        franchise_dealer BOOLEAN,
        franchise_make VARCHAR(255),
        front_legroom VARCHAR(255),
        fuel_tank_volume VARCHAR(255),
        fuel_type VARCHAR(255),
        has_accidents VARCHAR(255),
        height VARCHAR(255),
        highway_fuel_economy FLOAT,
        horsepower FLOAT,
        interior_color VARCHAR(255),
        isCab VARCHAR(255),
        is_certified VARCHAR(255),
        is_cpo VARCHAR(255),
        is_new BOOLEAN,
        is_oemcpo VARCHAR(255),
        latitude FLOAT,
        length VARCHAR(255),
        listed_date TIMESTAMP,
        listing_color VARCHAR(255),
        listing_id BIGINT,
        longitude FLOAT,
        main_picture_url TEXT,
        major_options TEXT,
        make_name VARCHAR(255),
        maximum_seating VARCHAR(255),
        mileage FLOAT,
        model_name VARCHAR(255),
        owner_count FLOAT,
        power VARCHAR(255),
        price FLOAT,
        salvage VARCHAR(255),
        savings_amount BIGINT,
        seller_rating FLOAT,
        sp_id BIGINT,
        sp_name VARCHAR(255),
        theft_title VARCHAR(255),
        torque VARCHAR(255),
        transmission VARCHAR(255),
        transmission_display VARCHAR(255),
        trimId VARCHAR(255),
        trim_name VARCHAR(255),
        vehicle_damage_category FLOAT,
        wheel_system VARCHAR(255),
        wheel_system_display VARCHAR(255),
        wheelbase VARCHAR(255),
        width VARCHAR(255),
        year BIGINT
    );
    '''
    cxn = None
    try:
        cnx = create_connection()
        cur = cnx.cursor()
        cur.execute(create_table_query)
        cur.close()
        cnx.commit()
        print('Table created successfully')
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if cnx is not None:
            cnx.close()

def insert_data(df):
    block_size = 10000
    blocks = [df[i:i + block_size] for i in range(0, df.shape[0], block_size)]
    
    try:
        cnx = create_connection()
        cur = cnx.cursor()
        
        column_names = df.columns.tolist()
        insert_query = f"""
        INSERT INTO used_cars({", ".join(column_names)})
        VALUES ({", ".join(["%s"] * len(column_names))})
        """
        
        inserted_rows = 0
        for i, block in enumerate(blocks, 1):
            for _, row in block.iterrows():
                values = tuple(row)
                cur.execute(insert_query, values)
                inserted_rows += 1
                print(f"Inserted {inserted_rows} data in block {i}")
        
        cur.close()
        cnx.commit()
        print('Data insertion successful')
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error during insertion:", error)
    finally:
        if cnx is not None:
            cnx.close()

def run_query(sql):
    cnx = create_connection()
    cur = cnx.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    columns = [col[0] for col in cur.description]
    df = pd.DataFrame(rows)
    df.rename(columns=dict(zip(range(len(columns)), columns)), inplace=True) 
    cnx.close()
    return df

def create_data_warehouse():
    create_date_dimension = '''
    CREATE TABLE IF NOT EXISTS dim_date(
        date_key INT PRIMARY KEY,
        date DATE,
        short_month VARCHAR(3),
        day_of_week VARCHAR(9),
        year INT
    );
    '''

    create_model_dimension = '''
    CREATE TABLE IF NOT EXISTS dim_model(
        model_key INT PRIMARY KEY,
        make_name VARCHAR(255),
        model_name VARCHAR(255),
        full_model_name VARCHAR(255),
        body_type VARCHAR(255)
    );
    '''

    create_location_dimension = '''
    CREATE TABLE IF NOT EXISTS dim_location(
        location_key INT PRIMARY KEY,
        city VARCHAR(255),
        state VARCHAR(255),
        city_state VARCHAR(255)
    );
    '''

    create_dealer_dimension = '''
    CREATE TABLE IF NOT EXISTS dim_dealer(
        dealer_key INT PRIMARY KEY,
        dealer_name VARCHAR(255),
        reputation_score INT 
    );
    '''

    create_used_cars_facts = '''
    CREATE TABLE IF NOT EXISTS fact_used_cars(
        vehicle_vin VARCHAR(255) PRIMARY KEY,
        days_on_market INT,
        engine VARCHAR(255),
        has_accidents INT,
        fuel_type VARCHAR(255),
        horsepower INT,
        is_new INT,
        color VARCHAR(255),
        seating_capacity INT,
        mileage INT,
        owner_count INT,
        price INT,
        transmission VARCHAR(255),
        drivetrain VARCHAR(255),
        vehicle_year INT,
        date_key VARCHAR(255),
        model_key INT,
        location_key INT,
        dealer_key INT
    );
    '''

    cxn = None
    try:
        cnx = create_connection()
        cur = cnx.cursor()
        cur.execute(create_used_cars_facts)
        cur.execute(create_dealer_dimension)
        cur.execute(create_location_dimension)
        cur.execute(create_model_dimension)
        cur.execute(create_date_dimension)
        cur.close()
        cnx.commit()
        print('Data Warehouse created successfully')
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if cnx is not None:
            cnx.close()

def insert_data_warehouse(df,table):
    column_names = df.columns.tolist()
    insert_query = f"""
        INSERT INTO {table}({", ".join(column_names)})
        VALUES ({", ".join(["%s"] * len(column_names))})
    """
    cxn = None
    try:
        cnx = create_connection()
        cur = cnx.cursor()
        for index, row in df.iterrows():
            values = tuple(row)
            cur.execute(insert_query, values)
        cur.close()
        cnx.commit()
        print(f"Data has been loaded into: {table}")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if cnx is not None:
            cnx.close()

def create_table_api():
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS anime_table(
        mal_id INT PRIMARY KEY,
        title VARCHAR(255),
        score FLOAT,
        season VARCHAR(255),
        year INT,
        aired_from DATE,
        popularity INT,
        favorites INT,
        members INT,
        episodes INT,
        genres VARCHAR(255),
        classification VARCHAR(255),
        status VARCHAR(255),
        airing_day VARCHAR(255),
        production VARCHAR(255),
        animation_studio VARCHAR(255),
        content_type VARCHAR(255)
    );
    '''
    cxn = None
    try:
        cnx = create_connection()
        cur = cnx.cursor()
        cur.execute(create_table_query)
        cur.close()
        cnx.commit()
        print('Table created successfully')
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if cnx is not None:
            cnx.close()

def insert_data_api(df):
    column_names = df.columns.tolist()
    insert_query = f"""
        INSERT INTO anime_table({", ".join(column_names)})
        VALUES ({", ".join(["%s"] * len(column_names))})
    """
    cxn = None
    try:
        cnx = create_connection()
        cur = cnx.cursor()
        for index, row in df.iterrows():
            values = tuple(row)
            cur.execute(insert_query, values)
        cur.close()
        cnx.commit()
        print(f"Data has been loaded into: anime_table")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if cnx is not None:
            cnx.close()