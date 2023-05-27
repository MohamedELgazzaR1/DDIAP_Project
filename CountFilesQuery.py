from elasticsearch import Elasticsearch

# Elasticsearch configuration
es_host = 'localhost'
es_port = 9200
es_index = 'stations_index'
es_scheme = 'http'  # Replace with 'https' if using HTTPS

all_Messages_Number = 600

# Initialize Elasticsearch client
es = Elasticsearch(hosts=[{'host': es_host, 'port': es_port, 'scheme': es_scheme}])

# Construct count query
query = {
    "query": {
        "match_all": {}
    }
}

# Send count request to Elasticsearch
response = es.count(index=es_index, body=query)

# Extract the count from the response
file_count = response['count']

print(f"Number of Dropped Messeges: {all_Messages_Number - file_count}")
