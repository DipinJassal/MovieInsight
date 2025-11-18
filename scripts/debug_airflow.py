"""
Debug why Airflow scheduler dies
"""
import os
import sys

print("="*70)
print(" AIRFLOW DEBUG ".center(70))
print("="*70)

# Check AIRFLOW_HOME
airflow_home = os.getenv('AIRFLOW_HOME')
print(f"\n1. AIRFLOW_HOME: {airflow_home}")

if airflow_home:
    # Check if database exists
    db_path = os.path.join(airflow_home, 'airflow.db')
    print(f"2. Database exists: {os.path.exists(db_path)}")
    
    # Check if dags folder exists
    dags_path = os.path.join(airflow_home, 'dags')
    print(f"3. DAGs folder exists: {os.path.exists(dags_path)}")
    
    if os.path.exists(dags_path):
        dags = [f for f in os.listdir(dags_path) if f.endswith('.py')]
        print(f"4. DAG files found: {len(dags)}")
        for dag in dags:
            print(f"   - {dag}")
    
    # Check logs
    logs_path = os.path.join(airflow_home, 'logs', 'scheduler')
    if os.path.exists(logs_path):
        print(f"5. Scheduler logs exist: Yes")
        # Find latest log
        import glob
        log_files = glob.glob(os.path.join(logs_path, '*/scheduler.log'))
        if log_files:
            latest_log = max(log_files, key=os.path.getmtime)
            print(f"6. Latest log: {latest_log}")
            
            # Show last lines
            print("\n7. Last 10 lines of scheduler log:")
            print("-"*70)
            with open(latest_log, 'r') as f:
                lines = f.readlines()
                for line in lines[-10:]:
                    print(line.rstrip())
    else:
        print("5. Scheduler logs: Not found")
else:
    print("ERROR: AIRFLOW_HOME not set!")

print("\n" + "="*70)

# Test DAG imports
print("\n8. Testing DAG imports...")
sys.path.insert(0, os.path.join(airflow_home, 'dags'))

try:
    import tmdb_extraction_dag
    print("   ✅ tmdb_extraction_dag imports successfully")
except Exception as e:
    print(f"   ❌ Error importing DAG: {str(e)}")

print("\n" + "="*70)