import uuid
from ...models import fetch_cassandra_data
from flask import current_app
import pandas as pd
import numpy as np

def section1(section1):
    # Cassandra session
    cassandra_session = current_app.cassandra_session

    # Convert the string IDs to UUID objects
    entity_ids = [uuid.UUID(id_str) for id_str in section1['ids']]

    # Cassandra query
    cass_query = """
        SELECT entity_id, ts, key, long_v, dbl_v 
        FROM ts_kv_cf 
        WHERE entity_type = 'DEVICE' 
          AND entity_id IN ? 
          AND key IN ? 
          AND ts >= ? 
          AND ts <= ? 
        ALLOW FILTERING
    """

    # Execute the Cassandra query with the converted UUIDs
    cass_data = fetch_cassandra_data(
        session=cassandra_session,
        query=cass_query,
        params=(entity_ids, section1['keys'], section1['startTs'], section1['endTs']),
        is_prepared=True
    )

    # Convert the result to a pandas DataFrame
    df = pd.DataFrame(cass_data)

    # Check if the DataFrame is empty
    if df.empty:
        print("No data returned from Cassandra.")
        return []

    # Combine `long_v` and `dbl_v` into a single column
    df['value'] = df['long_v'].combine_first(df['dbl_v'])

    # Convert `ts` to datetime
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')

    # Generate a full 5-minute time grid for the day
    start_time = pd.to_datetime(section1['startTs'], unit='ms')
    end_time = pd.to_datetime(section1['endTs'], unit='ms')

    # Ceil start_time to the next 5-minute interval
    start_time = start_time.ceil('5T')  # Rounds up to the next 5-minute mark

    # Ensure start_time does not exceed end_time
    if start_time > end_time:
        print("Start time exceeds end time after rounding up. No data to process.")
        return []

    # Generate the time grid starting from the next interval
    full_time_grid = pd.date_range(start=start_time, end=end_time, freq='5T')

    # Ceil timestamps to the next 5-minute interval
    df['ts_ceil'] = df['ts'].dt.ceil('5T')

    # Create a MultiIndex for all combinations of time grid and keys
    all_ts = pd.DataFrame({
        'ts_ceil': np.repeat(full_time_grid, len(section1['keys'])),
        'key': np.tile(section1['keys'], len(full_time_grid))
    })

    # Merge with the original data to fill gaps
    merged_df = pd.merge(all_ts, df, on=['ts_ceil', 'key'], how='left')

    # Forward-fill missing values within each key
    merged_df['value'] = merged_df.groupby('key')['value'].ffill().bfill()

    # Pivot to the final format
    pivoted_df = merged_df.pivot(index='ts_ceil', columns='key', values='value')

    # Handle missing keys by reindexing with `section1["keys"]`
    pivoted_df = pivoted_df.reindex(columns=section1['keys'], fill_value=0)

    # Add `ts` column as the first column
    pivoted_df.insert(0, 'ts', (pivoted_df.index.astype(int) // 10**6).values)

    # Fill NaNs with 0 (if any)
    pivoted_df = pivoted_df.fillna(0)

    # Remove the first row => because bucketing after 5 min interval
    pivoted_df = pivoted_df.iloc[1:]

    # Modify the last row's 'ts' by subtracting one minute => 12:00 = 11:59
    pivoted_df['ts'].iloc[-1] = pivoted_df['ts'].iloc[-1] - (1000*60)

    # Print the length of the final DataFrame
    print("Length of final DataFrame:", len(pivoted_df))

    # Return the result as a list of dictionaries
    return pivoted_df.to_dict(orient="records")