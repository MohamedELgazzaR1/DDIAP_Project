from elasticsearch import Elasticsearch

# Elasticsearch configuration
es_host = 'localhost'
es_port = 9200
es_index = 'stations_index'
es_scheme = 'http'  # Replace with 'https' if using HTTPS

# Initialize Elasticsearch client
es = Elasticsearch(hosts=[{'host': es_host, 'port': es_port, 'scheme': es_scheme}])

# Perform a simple search query
query = {
    'query': {
        'match': {
            'battery_status': 'high'
        }
    }
}
response = es.search(index=es_index, body=query)

# Process the search results
for hit in response['hits']['hits']:
    source = hit['_source']
    print(source)
    # Access the fields in the indexed document
    station_id = source['station_id']
    s_no = source['s_no']
    battery_status = source['battery_status']
    # ...


