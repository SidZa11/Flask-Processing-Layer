import psycopg2.pool
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import DCAwareRoundRobinPolicy, RetryPolicy
import time
from flask import g, current_app

# PostgreSQL connection pool
def init_postgres_pool(app):
    app.postgres_pool = psycopg2.pool.ThreadedConnectionPool(
        minconn=1,  # Minimum number of connections
        maxconn=20,  # Maximum number of connections
        dsn=app.config["POSTGRES_URI"]
    )

# Cassandra cluster and session initialization
def init_cassandra(app):
    retries = 3
    for attempt in range(retries):
        try:
            profile = ExecutionProfile(
                request_timeout=60,  # Increase timeout
                retry_policy=RetryPolicy(),  # Use default retry policy
                load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='datacenter1')  # Match your Cassandra setup
            )
            cluster = Cluster(
                app.config["CASSANDRA_HOSTS"],
                protocol_version=5,
                connect_timeout=30,  # Increase connection timeout
                execution_profiles={EXEC_PROFILE_DEFAULT: profile},
                control_connection_timeout=30  # Timeout for control connection
            )
            session = cluster.connect(app.config["CASSANDRA_KEYSPACE"])
            app.cassandra_cluster = cluster
            app.cassandra_session = session
            print("Cassandra connected successfully!")
            return session
        except Exception as e:
            print(f"Attempt {attempt + 1}/{retries} failed: {e}")
            if attempt < retries - 1:
                time.sleep(5)  # Wait before retrying
            else:
                app.cassandra_cluster = None
                app.cassandra_session = None
                return None

# Get PostgreSQL connection for the current request
def get_postgres_conn():
    if "postgres_conn" not in g:
        g.postgres_conn = current_app.postgres_pool.getconn()
    return g.postgres_conn

# Close PostgreSQL connection after request
def close_postgres_conn(e=None):
    conn = g.pop("postgres_conn", None)
    if conn is not None:
        current_app.postgres_pool.putconn(conn)
        print("Postgres conn closed.")

# Shutdown Cassandra cluster when app context ends
def shutdown_cassandra(e=None):
    cluster = getattr(current_app, 'cassandra_cluster', None)  # Safely get attribute
    if cluster is not None:
        cluster.shutdown()
        print("Cassandra cluster shut down.")

# Initialize app with pools and cleanup
def init_db(app):
    init_postgres_pool(app)
    init_cassandra(app)
    app.teardown_appcontext(close_postgres_conn)
    # app.teardown_appcontext(shutdown_cassandra)