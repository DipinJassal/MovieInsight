from pymongo import MongoClient
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

print("Testing MongoDB connection...")

try:
    # Get connection string from .env
    mongo_uri = os.getenv('MONGO_URI')
    
    # Connect to MongoDB
    client = MongoClient(mongo_uri)
    
    # Try to list databases (this will fail if connection is bad)
    databases = client.list_database_names()
    
    print("✓ MongoDB connection successful!")
    print(f"✓ Available databases: {databases}")
    
    # Close connection
    client.close()
    
except Exception as e:
    print(f"✗ MongoDB connection failed!")
    print(f"Error: {str(e)}")
    print("\nCheck your MONGO_URI in .env file")