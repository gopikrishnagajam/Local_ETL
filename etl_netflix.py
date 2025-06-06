import pandas as pd
import logging
from sqlalchemy import create_engine
import psycopg2
import schedule
import time
from datetime import datetime

logging.basicConfig(
    filename='etl_log.txt',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

db_user = 'your_user'
db_password = 'your_pass'
db_host = 'localhost'
db_port = '5432'
db_name = 'your_db'

engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

# === ETL FUNCTION ===
def run_etl():
    logging.info("Starting ETL job...")

    # Load CSV
    df = pd.read_csv('netflix_titles.csv')

    # Clean
    df.drop_duplicates(inplace=True)
    df.fillna('Unknown', inplace=True)
    df['date_added'] = pd.to_datetime(df['date_added'], format='%B %d, %Y', errors='coerce')
    df[['duration_int', 'duration_type']] = df['duration'].str.extract(r'(\d+)\s*(\w+)')
    df['duration_int'] = df['duration_int'].fillna(0).astype(int)

    # Delta detection
    existing_ids = pd.read_sql('SELECT show_id FROM netflix_titles', engine)
    new_rows = df[~df['show_id'].isin(existing_ids['show_id'])]

    if not new_rows.empty:
        new_rows.to_sql('netflix_titles', engine, if_exists='append', index=False)
        logging.info(f"Inserted {len(new_rows)} new records.")
    else:
        logging.info("No new records to insert.")

schedule.every().day.at("10:00").do(run_etl)

while True:
    schedule.run_pending()
    time.sleep(60)
