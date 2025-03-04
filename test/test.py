import time
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from datetime import datetime, timezone, timedelta
import logging
import uuid  # Standard UUID module

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),  # Log to console
        logging.FileHandler('test.log')  # Log to a file
    ]
)

def convertIntoMs(time):
    return int(time.timestamp() * 1000)

# Function to calculate dynamic timestamps
def get_dynamic_timestamps():
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=1)
    return int(start_date.timestamp() * 1000), int(end_date.timestamp() * 1000)

# Connect to Cassandra
start_total = time.time()
logging.info("Connecting to Cassandra...")
try:
    start = time.time()
    cluster = Cluster(['67.225.129.101'])
    cassandra_session = cluster.connect('thingsboard')
    end = time.time()
    total = end - start
    logging.info(f"Cassandra connection established in {total:.2f} seconds.")
except Exception as e:
    logging.error(f"Failed to connect to Cassandra: {e}")
    raise

# Set a smaller fetch size for pagination
cassandra_session.default_fetch_size = 200000  # Fetch rows at a time

keys = (
    'AC_Active_Power_Watt', 'Energy_Daily_kWh'
)

ids = (
    'e7fd9ef0-cce5-11ef-aad0-5f8aea70db1f',
    'e7ccf200-cce5-11ef-aad0-5f8aea70db1f',
    'e78847e0-cce5-11ef-aad0-5f8aea70db1f',
    'e750bd20-cce5-11ef-aad0-5f8aea70db1f',
    'e7228130-cce5-11ef-aad0-5f8aea70db1f',
    'e6ef1520-cce5-11ef-aad0-5f8aea70db1f',
    'e6b9d450-cce5-11ef-aad0-5f8aea70db1f',
    'e67f1540-cce5-11ef-aad0-5f8aea70db1f',
    'e6514e80-cce5-11ef-aad0-5f8aea70db1f',
    'e61cd100-cce5-11ef-aad0-5f8aea70db1f',
    'e5e60990-cce5-11ef-aad0-5f8aea70db1f',
    'e5b8b800-cce5-11ef-aad0-5f8aea70db1f',
    'e5830200-cce5-11ef-aad0-5f8aea70db1f',
    'e54e3660-cce5-11ef-aad0-5f8aea70db1f',
    'e51720d0-cce5-11ef-aad0-5f8aea70db1f',
    'e4e896c0-cce5-11ef-aad0-5f8aea70db1f'
)

# Convert IDs to UUID objects
try:
    entity_ids = [uuid.UUID(id_str) for id_str in ids]
except ValueError as e:
    logging.error(f"Invalid UUID found in IDs: {e}")
    raise

startTs, endTs = get_dynamic_timestamps()
logging.info(f"Start Timestamp: {startTs}, End Timestamp: {endTs}")

# Cassandra query (use %s for placeholders, do not use f-string here)
query = f"""
    SELECT entity_id, key, ts, dbl_v, long_v   
    FROM thingsboard.ts_kv_cf
"""

startExe = time.time()
statement = SimpleStatement(query, fetch_size=200000)

# Ensure keys and entity_ids are passed as tuples
logging.info("Executing Cassandra query...")
result = cassandra_session.execute(statement)
endExe = time.time()
exe = endExe - startExe
logging.info(f"Time to execute is {exe:.2f} seconds.")
#count = 0
#for row in result:
 #   count += 1
#print(count)
# Close connections
logging.info("Closing database connections...")
cassandra_session.shutdown()
cluster.shutdown()
logging.info("All connections closed.")
end_total = time.time()
total_time = end_total - start_total
logging.info(f"Total time: {total_time:.2f} seconds")
