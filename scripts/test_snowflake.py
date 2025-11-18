import snowflake.connector
from dotenv import load_dotenv
import os



# Explicitly load from root project path
env_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '.env'))
load_dotenv(dotenv_path=env_path)

# Load environment variables

print("USER:", os.getenv('SNOWFLAKE_USER'))
print("ACCOUNT:", os.getenv('SNOWFLAKE_ACCOUNT'))
print("WAREHOUSE:", os.getenv('SNOWFLAKE_WAREHOUSE'))
print("DATABASE:", os.getenv('SNOWFLAKE_DATABASE'))
print("SCHEMA:", os.getenv('SNOWFLAKE_SCHEMA'))
print("ROLE:", os.getenv('SNOWFLAKE_ROLE'))


print("Testing Snowflake connection...")

try:
    # Create connection
    conn = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE')
    )
    
    # Create cursor to execute queries
    cursor = conn.cursor()
    
    # Test query
    cursor.execute("SELECT CURRENT_VERSION()")
    version = cursor.fetchone()
    
    print("✓ Snowflake connection successful!")
    print(f"✓ Snowflake version: {version[0]}")
    
    # Close connections
    cursor.close()
    conn.close()
    
except Exception as e:
    print(f"✗ Snowflake connection failed!")
    print(f"Error: {str(e)}")
    print("\nCheck your Snowflake credentials in .env file")