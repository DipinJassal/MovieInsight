from pymongo import MongoClient
import os
from dotenv import load_dotenv

# Load our secret keys
load_dotenv()

def setup_mongodb():
    """
    This function creates collections (like tables) in MongoDB
    and adds indexes for fast searching
    """
    
    print("Connecting to MongoDB...")
    
    # Connect to MongoDB using connection string from .env
    client = MongoClient(os.getenv('MONGO_URI'))
    db = client[os.getenv('MONGO_DB_NAME')]
    
    print("Creating collections...")
    
    # List of collections we need
    # Think of these like different drawers in a filing cabinet
    collections = [
        'raw_movies',      # Will store movie data
        'raw_credits',     # Will store actor/director data
        'raw_genres',      # Will store genre categories
        'raw_companies'    # Will store production companies
    ]
    
    # Create each collection
    for coll_name in collections:
        # Check if collection already exists
        if coll_name not in db.list_collection_names():
            db.create_collection(coll_name)
            print(f"  ✓ Created collection: {coll_name}")
        else:
            print(f"  ↻ Collection already exists: {coll_name}")
    
    print("\nCreating indexes for faster searches...")
    
    # Indexes are like bookmarks - make searching faster
    # Create index on 'id' field in movies (so we can quickly find a movie by ID)
    db.raw_movies.create_index([("id", 1)], unique=True)
    print("  ✓ Created index on raw_movies.id")
    
    # Index on movie_id in credits (to find all cast for a movie)
    db.raw_credits.create_index([("movie_id", 1)])
    print("  ✓ Created index on raw_credits.movie_id")
    
    # Index on genre id
    db.raw_genres.create_index([("id", 1)], unique=True)
    print("  ✓ Created index on raw_genres.id")
    
    # Index on company id
    db.raw_companies.create_index([("id", 1)], unique=True)
    print("  ✓ Created index on raw_companies.id")
    
    print("\n✅ MongoDB setup completed successfully!")
    
    # Close connection
    client.close()

# This runs when you execute the script
if __name__ == "__main__":
    setup_mongodb()