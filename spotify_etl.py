import pandas as pd
import requests
import os
import sqlite3
import logging

# -------------------- CONFIGURATION --------------------
TOKEN_FILE_PATH = 'access_token.txt'
DATABASE_FILE = 'spotify_data.db'
SPOTIFY_API_URL = 'https://api.spotify.com/v1/me/player/recently-played'

# Logging Setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# -------------------- EXTRACT --------------------
def get_access_token():
    if not os.path.exists(TOKEN_FILE_PATH):
        raise FileNotFoundError('ACcess token file not found')
    
    with open(TOKEN_FILE_PATH, 'r') as file:
        return file.read().strip()
    

def extract_spotify_data():
    token = get_access_token()
    headers = {'Authorization': f'Bearer {token}'}

    try:
        response = requests.get(f'{SPOTIFY_API_URL}?limit=50', headers=headers)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error(f'Error fetching data from Spotify API: {e}')
        return None

    return response.json()


# -------------------- TRANSFORM --------------------
def transform_data(data):
    if not data or 'items' not in data:
        logging.warning('No valid data received from API')
        return None

    df = pd.DataFrame({
        'ID': [item['played_at'] for item in data['items']],  # Unique ID based on timestamp
        'date': [item['played_at'][:10] for item in data['items']],  # YYYY-MM-DD format
        'song_name': [item['track']['name'] for item in data['items']],
        'artist_name': [item['track']['artists'][0]['name'] for item in data['items']],
        'played_at': [item['played_at'] for item in data['items']]
    })

    logging.info(f'✅ Transformed {len(df)} records')
    return df


# -------------------- LOAD --------------------
def load_to_sqlite(df, db_file=DATABASE_FILE, table_name="spotify_recent_tracks"):
    if df is None:
        logging.warning('No data to save')
        return

    try:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        # Create table if not exists
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                ID TEXT PRIMARY KEY,
                date TEXT NOT NULL,
                song_name TEXT NOT NULL,
                artist_name TEXT NOT NULL,
                played_at TEXT NOT NULL
            )
        ''')

        # Insert new records, skipping duplicates
        df.to_sql(table_name, conn, if_exists='append', index=False)
        logging.info(f"✅ {len(df)} new songs added to '{table_name}'.")

        conn.commit()
    except sqlite3.IntegrityError:
        logging.info('✅ No new songs to add. Database is up to date')
    except sqlite3.Error as e:
        logging.error(f'Error inserting data into SQLite: {e}')
    finally:
        conn.close()


# -------------------- MAIN ETL PROCESS --------------------
def spotify_etl():
    
    # Extract
    raw_data = extract_spotify_data()
    if raw_data is None:
        return

    # Transform
    df = transform_data(raw_data)
    if df is None:
        return

    # Load
    load_to_sqlite(df)


# Run the ETL process
if __name__ == '__main__':
    spotify_etl()