import requests
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

print("Testing TMDB API connection...")

try:
    # Get API key and base URL
    api_key = os.getenv('TMDB_API_KEY')
    base_url = os.getenv('TMDB_BASE_URL')
    
    # Test API call - get popular movies
    url = f"{base_url}/movie/popular"
    params = {
        'api_key': api_key,
        'page': 1
    }
    
    response = requests.get(url, params=params)
    
    # Check if request was successful
    if response.status_code == 200:
        data = response.json()
        print("✓ TMDB API connection successful!")
        print(f"✓ Found {len(data['results'])} popular movies")
        print(f"✓ First movie: {data['results'][0]['title']}")
    else:
        print(f"✗ API request failed with status code: {response.status_code}")
        print(f"Response: {response.text}")
    
except Exception as e:
    print(f"✗ TMDB API connection failed!")
    print(f"Error: {str(e)}")
    print("\nCheck your TMDB_API_KEY in .env file")