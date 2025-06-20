import pandas as pd
import json
import time
from kafka import KafkaProducer
from datetime import datetime
import pytz  # install via pip install pytz if needed

# Config
CSV_FILE = 'l1_day.csv'
KAFKA_TOPIC = 'mock_l1_stream'
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
START_TIME_STR = '2024-08-01T13:36:32Z'
END_TIME_STR   = '2024-08-01T13:45:14Z'

def main():
    producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
    df = pd.read_csv(CSV_FILE, dtype=str)  # read all as strings initially
        
    #Parse ts_event to datetime
    df['ts_event_dt'] = pd.to_datetime(df['ts_event'], utc=True, errors='coerce')

    # Drop rows with missing values
    df = df.dropna(subset=['ts_event_dt'])
        
    #Define start/end as pandas Timestamps
    START_TIME = pd.to_datetime(START_TIME_STR)  # yields timezone-aware if 'Z' present
    END_TIME   = pd.to_datetime(END_TIME_STR)
        
    # Filter rows by timestamp
    df = df[(df['ts_event_dt'] >= START_TIME) & (df['ts_event_dt'] <= END_TIME)]

    # Sort by datetime
    df = df.sort_values('ts_event_dt')
        
    last_ts_dt = None
    # Group by ts_event_dt to simulate per-timestamp batch
    for ts_dt, group in df.groupby('ts_event_dt'):
            
            # For each row in this timestamp
            for _, row in group.iterrows():
                # Build the snapshot dict; convert types as needed.
                # Here we guard parsing numeric fields; if parse fails, skip that row.
                #try:
                snapshot = {
                        'publisher_id': int(row['publisher_id']),
                        'ask_px_00': float(row['ask_px_00']),
                        'ask_sz_00': int(row['ask_sz_00']),
                        'ts_event': row['ts_event'],  # original string
                        'ts_event_dt': ts_dt.isoformat(),
                }


if __name__ == '__main__':
    main()
