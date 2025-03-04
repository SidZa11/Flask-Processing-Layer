import uuid
from ...models import fetch_cassandra_data
import pandas as pd
import pytz
from flask import current_app
from datetime import datetime

def section2(section2):
    # Cassandra session
    cassandra_session = current_app.cassandra_session

    # Convert the string IDs to UUID objects
    entity_ids = [uuid.UUID(id_str) for id_str in section2['ids']]
    
    # Cassandra query
    cass_query = """
        SELECT entity_id, ts, key, long_v, dbl_v 
        FROM ts_kv_cf 
        WHERE entity_type = 'DEVICE' AND entity_id IN ? 
        AND key IN ? AND ts >= ? AND ts <= ? ALLOW FILTERING
    """

    # Execute the Cassandra query with the converted UUIDs
    cass_data = fetch_cassandra_data(
        session=cassandra_session,
        query=cass_query,
        params=(entity_ids, section2['keys'], section2['startTs'], section2['endTs']),
        is_prepared=True
    )

    # Convert Cassandra data to DataFrame
    df = cass_data

    # Ensure timestamps are in datetime format
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')

    # Convert timestamps to local timezone
    plant_timezone = pytz.timezone(section2['plantTimeZone'])
    df['ts'] = df['ts'].dt.tz_localize('UTC').dt.tz_convert(plant_timezone)

    # Pivot the DataFrame to have keys as columns
    df_pivot = df.pivot(index=['entity_id', 'ts'], columns='key', values='dbl_v').reset_index()

    # Group by entity_id and calculate metrics
    result = []
    for entity_id, group in df_pivot.groupby('entity_id'):
        # Filter non-zero power values for start and stop times
        non_zero_power = group[group['AC_Active_Power_Watt'] > 0]

        # Calculate start time and stop time
        start_time = non_zero_power['ts'].min()
        stop_time = non_zero_power['ts'].max()

        # Calculate last values for specific keys
        last_reactive_power = group['AC_Reactive_Power_var'].dropna().iloc[-1] if not group['AC_Reactive_Power_var'].dropna().empty else None
        last_energy_daily = group['Energy_Daily_kWh'].dropna().iloc[-1] if not group['Energy_Daily_kWh'].dropna().empty else None
        last_energy_total = group['Energy_Total_kWh'].dropna().iloc[-1] if not group['Energy_Total_kWh'].dropna().empty else None

        # Calculate min, max, and avg power
        min_power = group['AC_Active_Power_Watt'].min()
        max_power = group['AC_Active_Power_Watt'].max()
        avg_power = group['AC_Active_Power_Watt'].mean()

        # Append results for this entity_id
        result.append({
            "entity_id": str(entity_id),
            "AC_Reactive_Power_var": last_reactive_power,
            "Energy_Daily_kWh": last_energy_daily,
            "Energy_Total_kWh": last_energy_total,
            "start_time": start_time,
            "stop_time": stop_time,
            "min_power": min_power,
            "max_power": max_power,
            "avg_power": avg_power
        })

    return result