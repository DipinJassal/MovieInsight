import requests
import time
import os
from dotenv import load_dotenv

load_dotenv()

class TMDBClient:
    """
    This class handles all communication with TMDB API.
    Think of it as a phone that calls TMDB to get movie data.
    """
    
    def __init__(self):
        # Get API credentials from .env file
        self.api_key = os.getenv('TMDB_API_KEY')
        self.base_url = os.getenv('TMDB_BASE_URL')
        
        # Create a session (like keeping the phone line open)
        self.session = requests.Session()
        
        print(f"TMDB Client initialized with base URL: {self.base_url}")
    
    def get_popular_movies(self, pages=10):
        """
        Get popular movies from TMDB.
        
        Parameters:
        - pages: How many pages to fetch (each page has ~20 movies)
        
        Returns:
        - List of movie dictionaries
        """
        movies = []
        
        print(f"Fetching {pages} pages of popular movies...")
        
        for page in range(1, pages + 1):
            # Build the URL
            url = f"{self.base_url}/movie/popular"
            
            # Parameters for the API call
            params = {
                'api_key': self.api_key,
                'page': page,
                'language': 'en-US'
            }
            
            try:
                # Make the API call
                response = self.session.get(url, params=params)
                
                # Check if it worked (status code 200 = success)
                response.raise_for_status()
                
                # Convert JSON response to Python dictionary
                data = response.json()
                
                # Add movies from this page to our list
                movies.extend(data.get('results', []))
                
                print(f"  ✓ Page {page}: Got {len(data.get('results', []))} movies")
                
                # Wait a bit to be nice to TMDB servers (rate limiting)
                time.sleep(0.25)  # Sleep for 250 milliseconds
                
            except Exception as e:
                print(f"  ✗ Error fetching page {page}: {str(e)}")
                continue  # Skip this page and move to next
        
        print(f"✅ Total movies fetched: {len(movies)}")
        return movies
    
    def get_movie_details(self, movie_id):
        """
        Get detailed information about a specific movie.
        
        Parameters:
        - movie_id: The TMDB movie ID
        
        Returns:
        - Dictionary with movie details, or None if failed
        """
        url = f"{self.base_url}/movie/{movie_id}"
        
        # append_to_response asks TMDB to include extra info in one call
        params = {
            'api_key': self.api_key,
            'append_to_response': 'credits,keywords,production_companies'
        }
        
        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            return response.json()
            
        except Exception as e:
            print(f"  ✗ Error fetching movie {movie_id}: {str(e)}")
            return None
    
    def get_movie_credits(self, movie_id):
        """
        Get cast and crew for a movie.
        
        Parameters:
        - movie_id: The TMDB movie ID
        
        Returns:
        - Dictionary with 'cast' and 'crew' lists
        """
        url = f"{self.base_url}/movie/{movie_id}/credits"
        params = {'api_key': self.api_key}
        
        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            return response.json()
            
        except Exception as e:
            print(f"  ✗ Error fetching credits for {movie_id}: {str(e)}")
            return None
    
    def get_genres(self):
        """
        Get list of all movie genres.
        
        Returns:
        - List of genre dictionaries [{'id': 28, 'name': 'Action'}, ...]
        """
        url = f"{self.base_url}/genre/movie/list"
        params = {'api_key': self.api_key}
        
        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            genres = response.json().get('genres', [])
            print(f"✅ Fetched {len(genres)} genres")
            return genres
            
        except Exception as e:
            print(f"✗ Error fetching genres: {str(e)}")
            return []
        
    def close(self):
        """
        Close the HTTP session.
        """
        self.session.close()
        print("✓ TMDB Client session closed")


# Test the client if we run this file directly
if __name__ == "__main__":
    print("Testing TMDB Client...\n")
    
    client = TMDBClient()
    
    # Test 1: Get genres
    print("\n--- Test 1: Fetching Genres ---")
    genres = client.get_genres()
    print(f"Genres: {genres[:3]}...")  # Print first 3
    
    # Test 2: Get popular movies (just 1 page for testing)
    print("\n--- Test 2: Fetching Popular Movies ---")
    movies = client.get_popular_movies(pages=1)
    print(f"First movie: {movies[0]['title']}")
    
    # Test 3: Get movie details
    print("\n--- Test 3: Fetching Movie Details ---")
    if movies:
        movie_id = movies[0]['id']
        details = client.get_movie_details(movie_id)
        print(f"Budget: ${details.get('budget'):,}")
        print(f"Revenue: ${details.get('revenue'):,}")
    
    print("\n✅ All tests passed!")