import csv
import random
from datetime import datetime, timedelta

NUM_ROWS = 500
CSV_FILE = 'shipment_data.csv'

statuses = ['in-transit', 'delivered', 'pending', 'canceled']
locations = ['New York, NY', 'Los Angeles, CA', 'Chicago, IL', 'Houston, TX', 'Phoenix, AZ']

def generate_random_timestamp(start_date, end_date):
    delta = end_date - start_date
    random_seconds = random.randint(0, int(delta.total_seconds()))
    return (start_date + timedelta(seconds=random_seconds)).isoformat() + 'Z'

data = []
for i in range(1, NUM_ROWS + 1):
    shipment_id = f'SH{i:06d}'
    origin = random.choice(locations)
    destination = random.choice([loc for loc in locations if loc != origin])
    status = random.choice(statuses)
    timestamp = generate_random_timestamp(
        datetime(2023, 1, 1, 0, 0, 0),
        datetime(2024, 12, 31, 23, 59, 59)
    )
    data.append([shipment_id, origin, destination, status, timestamp])


with open(CSV_FILE, mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['shipment_id', 'origin', 'destination', 'status', 'timestamp'])
    writer.writerows(data)

print(f"CSV file '{CSV_FILE}' with {NUM_ROWS} rows created successfully.")
