import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    POSTGRES_URI = os.getenv("POSTGRES_URI")
    CASSANDRA_HOSTS = os.getenv("CASSANDRA_HOSTS").split(",")
    CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE")
    # SECRET_KEY = os.getenv("SECRET_KEY", "your-secret-key")