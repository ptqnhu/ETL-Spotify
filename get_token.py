from flask import Flask, jsonify, redirect, request, session
import secrets
import os
from dotenv import load_dotenv
from urllib.parse import urlencode
import base64
import requests


# Load environment variables from .env file
load_dotenv()

# Initialize Flask app and Generate a secure random key for session management
app = Flask(__name__)
app.secret_key = secrets.token_hex(16)

# Spotify OAuth Settings
SPOTIFY_AUTH_URL = 'https://accounts.spotify.com/authorize'
SPOTIFY_TOKEN_URL = 'https://accounts.spotify.com/api/token'
CLIENT_ID = os.getenv('SPOTIFY_CLIENT_ID')
CLIENT_SECRET = os.getenv('SPOTIFY_CLIENT_SECRET')
REDIRECT_URI = os.getenv('SPOTIFY_REDIRECT_URI', 'http://localhost:8080/callback')
SCOPES = ['user-read-recently-played']

# Validate Spotify credentials
if not CLIENT_ID or not CLIENT_SECRET:
    raise ValueError(
        'Missing Spotify API credentials. Set SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET'
    )

# Validate SCOPES
if not isinstance(SCOPES, list) or not SCOPES:
    raise ValueError('Invalid SCOPES configuration. It should be a non-empty list')

# Generic error handler
import logging
logging.basicConfig(level=logging.INFO)

def handle_error(message, status_code=400):
    logging.error(f"Error: {message}")
    return jsonify({"error": message}), status_code

# Route 1: Home Route
@app.route('/')
def home():
    try:
        auth_url = build_spotify_auth_url()
        return redirect(auth_url)
    except ValueError as e:
        return handle_error(str(e))

# Function to build Spotify athentication URL
def build_spotify_auth_url():
    params = {
        'response_type': 'code',
        'client_id': CLIENT_ID,
        'redirect_uri':  REDIRECT_URI,
        'scope': ' '.join(SCOPES)
    }
    return f'{SPOTIFY_AUTH_URL}?{urlencode(params)}'


# Route 2: Callback Route
@app.route('/callback')
def callback():
    code = request.args.get('code')
    if not code:
        return handle_error('Authorization not found')
    
    try:
        access_token = get_access_token(code)
        session['access_token'] = access_token
        save_access_token(access_token)
        return jsonify({'message': 'Access token obtained and stored successfully'}), 200
    except Exception as e:
        return handle_error(str(e), 400)

def get_access_token(code):
    try:
        auth_value = base64.b64encode(f'{CLIENT_ID}:{CLIENT_SECRET}'.encode()).decode()
        auth_header = {
            'Authorization': f'Basic {auth_value}',
            'Content-Type': 'application/x-www-form-urlencoded'
        }

        # Data for token request
        token_data = {
            'grant_type': 'authorization_code',
            'code': code,
            'redirect_uri': REDIRECT_URI
        }

        # Request access token
        response = requests.post(SPOTIFY_TOKEN_URL, data=token_data, headers=auth_header)
        response.raise_for_status()

        # Extract token from response
        data = response.json()
        access_token = data.get('access_token')

        if not access_token:
            raise ValueError('Access token not found in the response')
        
        return access_token
    
    except Exception as e:
        return handle_error(str(e))

def save_access_token(token):
    try:
        with open('access_token.txt', 'w') as file:
            file.write(token)
        logging.info('Access token saved successfully')
    except IOError as e:
        logging.error(f'Failed to save access token: {e}')


# Run the Flask app
if __name__ == '__main__':
    app.run(debug=True, port=8080)