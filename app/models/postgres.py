import pandas as pd

def fetch_postgres_data(conn, query):
    """
    Fetch data from PostgreSQL and return as a Pandas DataFrame.
    :params conn: Postgres Connection
    :params query: Postgres Query String
    :return: Pandas DataFrame containing the query results
    """
    if conn is None:
        print("Postgres connection is None, cannot execute query")
        return pd.DataFrame()  # Return an empty DataFrame if no connection
    
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        
        # Extract column names from the cursor description
        columns = [desc[0] for desc in cursor.description]
        
        # Create a DataFrame
        df = pd.DataFrame(rows, columns=columns)

        cursor.close()
        return df
      
    except Exception as e:
        print(f'Failed to execute Postgres query {query} : {e}')
        return pd.DataFrame()  # Return an empty DataFrame on error