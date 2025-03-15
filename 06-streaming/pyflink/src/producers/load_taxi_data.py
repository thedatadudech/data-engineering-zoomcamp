import gzip
import json
import pandas as pd
from kafka import KafkaProducer
from time import time

# Kafka Configuration
KAFKA_BROKER = "localhost:9092"  # Update if necessary
TOPIC_NAME = "green-trips"

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# Function to clean and preprocess data
def clean_data(df):
    """Cleans and preprocesses the taxi dataset."""
    df = df[
        [
            "lpep_pickup_datetime",
            "lpep_dropoff_datetime",
            "PULocationID",
            "DOLocationID",
            "passenger_count",
            "trip_distance",
            "tip_amount",
        ]
    ]

    # Fill missing values with appropriate defaults
    df["passenger_count"] = pd.to_numeric(df["passenger_count"], errors="coerce").fillna(0).astype(int)
    df["PULocationID"] = pd.to_numeric(df["PULocationID"], errors="coerce").fillna(0).astype(int)
    df["DOLocationID"] = pd.to_numeric(df["DOLocationID"], errors="coerce").fillna(0).astype(int)
    df["trip_distance"] = pd.to_numeric(df["trip_distance"], errors="coerce").fillna(0.0)
    df["tip_amount"] = pd.to_numeric(df["tip_amount"], errors="coerce").fillna(0.0)

    return df

# Load Data
def load_data(filename):
    """Loads and cleans the taxi dataset from a compressed CSV file."""
    with gzip.open(filename, "rt", encoding="utf-8") as f:
        df = pd.read_csv(f)
    return clean_data(df)

# Send Data to Kafka
def send_data(df):
    """Sends taxi trip data to Kafka topic."""
    t0 = time()
    count = 0

    for _, row in df.iterrows():
        message = row.to_dict()
        try:
            producer.send(TOPIC_NAME, value=message)
            count += 1
        except Exception as e:
            print(f"❌ Failed to send message: {message}, Error: {e}")

    producer.flush()
    t1 = time()

    print(f"✅ Sent {count} messages in {t1 - t0:.2f} seconds.")

if __name__ == "__main__":
    filename = "src/data/green_tripdata_2019-10.csv.gz"  # Update the filename if needed
    df = load_data(filename)
    send_data(df)
