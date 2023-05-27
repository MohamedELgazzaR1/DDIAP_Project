import os
import glob
import pandas as pd
from elasticsearch import Elasticsearch
import time

# Elasticsearch configuration
es_host = 'elasticsearch'
es_port = 9200
es_index = 'stations_index'
es_scheme = 'http'

parquet_dir = './Parque_Storage/Data'
es = Elasticsearch(hosts=[{'host': es_host, 'port': es_port, 'scheme': es_scheme}])

while True:
    parquet_files = glob.glob(os.path.join(parquet_dir, '*.parquet'))

    if len(parquet_files) == 0:
        # No files found, wait for a while before checking again
        time.sleep(5)
        continue

    for file in parquet_files:
        print("Processing file:", file)

        df = pd.read_parquet(file)

        for _, row in df.iterrows():
            # Prepare document to be indexed
            document = {
                'station_id': int(row['station_id']),
                's_no': int(row['s_no']),
                'battery_status': row['battery_status'],
                'status_timestamp': int(row['status_timestamp']),
                'weather': {
                    'humidity': int(row['humidity']),
                    'temperature': int(row['temperature']),
                    'wind_speed': int(row['wind_speed'])
                }
            }

            # Index document into Elasticsearch
            es.index(index=es_index, body=document)

        print(f"Indexed {len(df)} documents from {file}")

        # Delete the local parquet file
        os.remove(file)

    print("Indexing complete for all files. Waiting for new files...")
