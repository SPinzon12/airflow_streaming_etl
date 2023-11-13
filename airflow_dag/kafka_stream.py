import time

from services.db_postgres import run_query
from services.kafka import kafka_producer


def stream_data():
    sql = '''SELECT * 
    FROM fact_used_cars
    '''
    cars_df = run_query(sql)
    for index, row in cars_df.iterrows():
        kafka_producer(row)
        time.sleep(1)