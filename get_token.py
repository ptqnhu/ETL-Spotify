from flask import Flask
import secrets
import os
from dotenv import load_dotenv


# Load environment variables from .env file
load_dotenv()

# Initialize Flask app
app = Flask(__name__)

# Generate a secure random key for session management
app.secret_key = secrets.token_hex(16)

# Get Spotify API credentials from environment variables
CLIENT_ID = os.getenv('SPOTIFY_CLIENT_ID')
CLIENT_SECRET = os.getenv('SPOTIFY_CLIENT_SECRET')

# Ensure required credentials are set
if not CLIENT_ID or not CLIENT_SECRET:
    raise ValueError(
        'Missing Spotify API credentials. Set SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET'
    )

# Set redirect URI (default for local development)
REDIRECT_URI = os.getenv('SPOTIFY_REDIRECT_URI', 'http://localhost:8080/callback')

# Set Spotify API Endpoints
SPOTIFY_AUTH_URL = 'https://accounts.spotify.com/authorize'
SPOTIFY_TOKEN_URL = 'https://accounts.spotify.com/api/token'




# Run the Flask app
if __name__ == '__main__':
    app.run(debug=True, port=8080)