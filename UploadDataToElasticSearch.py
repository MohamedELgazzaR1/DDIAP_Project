import os
import glob
import pandas as pd
from elasticsearch import Elasticsearch

# Elasticsearch configuration
es_host = 'localhost'
es_port = 9200
es_index = 'stations_index'
es_scheme = 'http'  


parquet_dir = 'D:\ParquetDummy'


es = Elasticsearch(hosts=[{'host': es_host, 'port': es_port, 'scheme': es_scheme}])


parquet_files = glob.glob(os.path.join(parquet_dir, '*.parquet'))


for file in parquet_files:
    print("here")

    
    df = pd.read_parquet(file)
    
    
    for _, row in df.iterrows():
        # Prepare document to be indexed
        document = {
            'station_id': int(row['station_id']),
            's_no': int(row['s_no']),
            'battery_status': row['battery_status'],
            'status_timestamp': int(row['status_timestamp']),
            'weather': {
                'humidity': int(row['weather_humidity']),
                'temperature': int(row['weather_temperature']),
                'wind_speed': int(row['weather_wind_speed'])
            }
        }
        
        # Index document into Elasticsearch
        es.index(index=es_index, body=document)
        
    print(f"Indexed {len(df)} documents from {file}")

print("Indexing complete.")
