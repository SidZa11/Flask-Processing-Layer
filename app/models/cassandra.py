import pandas as pd

def fetch_cassandra_data(session, query, params=None, is_prepared=False):
    """
    Fetch data from Cassandra.

    :param session: Cassandra session object.
    :param query: The query string.
    :param params: Parameters to bind to the prepared statement (if applicable).
    :param is_prepared: Boolean indicating whether the query is a prepared statement.
    :return: List of rows returned by the query.
    """
    if session is None:
        print("Cassandra session is None, cannot execute query.")
        return []

    try:
        if is_prepared:
            # make the query is a prepared statement
            prepared_query = session.prepare(query) 
            # bind the parameters
            if params is None:
                raise ValueError("Parameters must be provided for a prepared statement.")
            data = session.execute(prepared_query, params)
        else:
            # If the query is a regular CQL string, execute it directly
            data = session.execute(query)
        
        # Extract column names from the metadata
        columns = data.column_names

        # Convert rows to a list of dictionaries
        result = [dict(zip(columns, row)) for row in data]

        # Create a DataFrame
        df = pd.DataFrame(result)
        # print(df.head())
        print(f"Cassandra query executed and the length of result is: {len(df)}")
        return df
    except Exception as e:
        print(f"Failed to execute Cassandra query '{query}': {e}")
        return []