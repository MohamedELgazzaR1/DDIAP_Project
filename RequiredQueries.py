from elasticsearch import Elasticsearch

# Elasticsearch configuration
es_host = 'localhost'
es_port = 9200
es_index = 'stations_index'
es_scheme = 'http'  # Replace with 'https' if using HTTPS

# Initialize Elasticsearch client
es = Elasticsearch(hosts=[{'host': es_host, 'port': es_port, 'scheme': es_scheme}])

# Count of low-battery statuses per station
low_battery_query = {
    "size": 0,
    "query": {
        "term": {
            "battery_status.keyword": "low"
        }
    },
    "aggs": {
        "stations": {
            "terms": {
                "field": "station_id",
                "size": 1
            }
        }
    }
}


# Send queries to Elasticsearch
low_battery_response = es.search(index=es_index, body=low_battery_query)

# Process low-battery response
low_battery_counts = low_battery_response['aggregations']['stations']['buckets']
print("Count of low-battery statuses per station:")
for bucket in low_battery_counts:
    station_id = bucket['key']
    count = bucket['doc_count']
    print(f"Station ID: {station_id}, Count: {count}")

