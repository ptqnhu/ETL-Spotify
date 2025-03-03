import pandas as pd
import requests
import os
import sqlite3
import logging
import datetime
import pytz

# -------------------- CONFIGURATION --------------------
TOKEN_FILE_PATH = 'access_token.txt'
DATABASE_FILE = 'spotify_data.db'
SPOTIFY_API_URL = 'https://api.spotify.com/v1/me/player/recently-played'
LOCAL_TIMEZONE = pytz.timezone('Asia/Bangkok')  # Change this to your correct timezone

# Logging Setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# -------------------- EXTRACT --------------------
def get_access_token():
    if not os.path.exists(TOKEN_FILE_PATH):
        raise FileNotFoundError('Access token file not found')
    
    with open(TOKEN_FILE_PATH, 'r') as file:
        return file.read().strip()
    

def extract():
    token = get_access_token()
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}'
    }

    try:
        response = requests.get(f'{SPOTIFY_API_URL}?limit=50', headers=headers)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error(f'Error fetching data from Spotify API: {e}')
        return None

    return response.json()


# -------------------- TRANSFORM --------------------
def transform(data):
    if not data or 'items' not in data:
        return pd.DataFrame()  # Return empty DataFrame if data is invalid

    df = pd.DataFrame([
        {
            "id": int(datetime.datetime.strptime(item['played_at'], "%Y-%m-%dT%H:%M:%S.%fZ")
                     .replace(tzinfo=pytz.utc)  # Convert from UTC
                     .astimezone(LOCAL_TIMEZONE)  # Convert to local timezone
                     .timestamp() * 1000),  # Convert to milliseconds

            "date": datetime.datetime.strptime(item['played_at'], "%Y-%m-%dT%H:%M:%S.%fZ")
                    .replace(tzinfo=pytz.utc)
                    .astimezone(LOCAL_TIMEZONE)
                    .strftime("%Y-%m-%d"),  # Extract YYYY-MM-DD

            "song_name": item['track']['name'],
            "artist_name": item['track']['artists'][0]['name'],

            "played_at": datetime.datetime.strptime(item['played_at'], "%Y-%m-%dT%H:%M:%S.%fZ")
                         .replace(tzinfo=pytz.utc)
                         .astimezone(LOCAL_TIMEZONE)
                         .strftime("%Y-%m-%d %H:%M:%S")  # Extract HH:MM:SS
        }
        for item in data['items']
    ])

    return df
    

# -------------------- LOAD --------------------
def load(df, db_file=DATABASE_FILE, table_name='spotify_recent_tracks'):
    if df is None:
        logging.warning('No data to save')
        return

    try:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        # Create new table if not exist
        sql_query = f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                ID INTEGER PRIMARY KEY,
                date TEXT NOT NULL,
                song_name TEXT NOT NULL,
                artist_name TEXT NOT NULL,
                played_at TEXT NOT NULL
            )
        '''
        cursor.execute(sql_query)

        # Insert new records, skipping duplicates
        df.to_sql(table_name, conn, if_exists='append', index=False)
        logging.info(f"{len(df)} new songs added to '{table_name}'.")

        conn.commit()
    except sqlite3.IntegrityError:
        logging.info('No new songs to add. Database is up to date')
    except sqlite3.Error as e:
        logging.error(f'Error inserting data into SQLite: {e}')
    finally:
        conn.close()


# -------------------- MAIN ETL PROCESS --------------------
def spotify_etl():
    
    # Extract
    raw_data = extract()
    if raw_data is None:
        return

    # Transform
    df = transform(raw_data)
    if df is None:
        return

    # Load
    load(df)


# Run the ETL process
if __name__ == '__main__':
    spotify_etl()