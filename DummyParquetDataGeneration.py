import os
import random
import pandas as pd

# Output directory for Parquet files
output_dir = 'D:\ParquetDummy'

# Number of Parquet files to generate
num_files = 500

# Number of messages per service
messages_per_service = 100

# Variation percentages
low_percentage = 0.3
medium_percentage = 0.4
high_percentage = 0.3

# Random seed (optional)
random.seed(42)

# Generate and write Parquet files
for file_num in range(num_files):
    # Create DataFrame to hold data
    data = {
        'station_id': [],
        's_no': [],
        'battery_status': [],
        'status_timestamp': [],
        'weather.humidity': [],
        'weather.temperature': [],
        'weather.wind_speed': []
    }
    
    # Generate data for each message per service
    for s_no in range(messages_per_service):
        # Generate random battery status based on percentages
        rand_num = random.random()
        if rand_num < low_percentage:
            battery_status = 'low'
        elif rand_num < low_percentage + medium_percentage:
            battery_status = 'medium'
        else:
            battery_status = 'high'
        
        # Generate other random data
        station_id = 1
        status_timestamp = random.randint(0, 9999999999)
        humidity = random.randint(0, 100)
        temperature = random.randint(-50, 150)
        wind_speed = random.randint(0, 100)
        
        # Append data to DataFrame
        data['station_id'].append(station_id)
        data['s_no'].append(s_no + 1)  # Auto-increment s_no starting from 1
        data['battery_status'].append(battery_status)
        data['status_timestamp'].append(status_timestamp)
        data['weather.humidity'].append(humidity)
        data['weather.temperature'].append(temperature)
        data['weather.wind_speed'].append(wind_speed)
    
    # Create DataFrame from data
    df = pd.DataFrame(data)
    
    # Write DataFrame to Parquet file
    file_path = os.path.join(output_dir, f'file_{file_num + 1}.parquet')
    df.to_parquet(file_path, engine='pyarrow')
    
    print(f"Generated Parquet file: {file_path}")

print("Data generation complete.")
