from utils.tmdb_api import TMDBClient
from utils.mongo_handler import MongoHandler
import time

print("Step 1: Creating TMDB client...")
start = time.time()
tmdb = TMDBClient()
print(f"  Done in {time.time() - start:.2f} seconds")

print("\nStep 2: Creating MongoDB handler...")
start = time.time()
mongo = MongoHandler()
print(f"  Done in {time.time() - start:.2f} seconds")

print("\nStep 3: Fetching genres from TMDB...")
start = time.time()
genres = tmdb.get_genres()
print(f"  Done in {time.time() - start:.2f} seconds")
print(f"  Got {len(genres)} genres")

print("\nStep 4: Inserting to MongoDB...")
start = time.time()
stats = mongo.insert_genres(genres)
print(f"  Done in {time.time() - start:.2f} seconds")
print(f"  Stats: {stats}")

tmdb.close()
mongo.close()

print("\n✅ All steps completed!")