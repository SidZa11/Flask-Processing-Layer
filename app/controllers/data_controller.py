import uuid  # Import the uuid module for UUID handling
from ..models import fetch_postgres_data, fetch_cassandra_data
from ..utils import get_postgres_conn
from flask import current_app


def get_combined_dataset():
    postgres_conn = get_postgres_conn()  # Get a connection from the pool
    cassandra_session = current_app.cassandra_session

    # Define the body with UUIDs as strings
    body = {
        "ids": [
            "e7fd9ef0-cce5-11ef-aad0-5f8aea70db1f",
            "e7ccf200-cce5-11ef-aad0-5f8aea70db1f",
            "e78847e0-cce5-11ef-aad0-5f8aea70db1f",
            "e750bd20-cce5-11ef-aad0-5f8aea70db1f",
            "e7228130-cce5-11ef-aad0-5f8aea70db1f",
            "e6ef1520-cce5-11ef-aad0-5f8aea70db1f",
            "e6b9d450-cce5-11ef-aad0-5f8aea70db1f",
            "e67f1540-cce5-11ef-aad0-5f8aea70db1f",
            "e6514e80-cce5-11ef-aad0-5f8aea70db1f",
            "e61cd100-cce5-11ef-aad0-5f8aea70db1f",
            "e5e60990-cce5-11ef-aad0-5f8aea70db1f",
            "e5b8b800-cce5-11ef-aad0-5f8aea70db1f",
            "e5830200-cce5-11ef-aad0-5f8aea70db1f",
            "e54e3660-cce5-11ef-aad0-5f8aea70db1f",
            "e51720d0-cce5-11ef-aad0-5f8aea70db1f",
            "e4e896c0-cce5-11ef-aad0-5f8aea70db1f",
        ],
        "tags": ["AC_Active_Power_Watt", "Energy_Daily_kWh"],
        "startTs" : 1740009600000,
        "endTs" : 1740441600000
    }

    # Convert the string IDs to UUID objects
    entity_ids = [uuid.UUID(id_str) for id_str in body['ids']]

    # Cassandra query
    cass_query = "SELECT * FROM ts_kv_cf WHERE entity_type = 'DEVICE' AND entity_id IN ? AND key IN ? AND ts > ? AND ts < ? LIMIT 50 ALLOW FILTERING"

    # Execute the Cassandra query with the converted UUIDs
    cass_data = fetch_cassandra_data(session=cassandra_session,query=cass_query, params=(entity_ids, body['tags'], body['startTs'], body['endTs']), is_prepared=True)
    # print("CASS DATA : ", cass_data)

    # PostgreSQL query
    pg_query = "SELECT * FROM daily_device_summary LIMIT 50;"
    pg_data = fetch_postgres_data(postgres_conn, pg_query)

    # Combine the datasets
    dataset = {
        "postgres": pg_data.to_dict(orient="records"),
        "cassandra": cass_data.to_dict(orient="records")
    }
    return dataset