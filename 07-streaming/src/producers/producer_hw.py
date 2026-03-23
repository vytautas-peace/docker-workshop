import pandas as pd
import time
import sys
from pathlib import Path
import json

# Allow running as `python src/consumers/consumer_hw.py` from project root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from models.models_hw import Ride, ride_from_row, ride_serializer
from kafka import KafkaProducer


LOG_PATH = "/home/vytautas.peace/data-engineering-zoomcamp/.cursor/debug-d0d984.log"


def _debug_log(hypothesisId: str, location: str, message: str, data: dict | None = None) -> None:
    """Append a single NDJSON debug record to the shared log file."""
    payload = {
        "sessionId": "d0d984",
        "runId": "pre_fix",
        "hypothesisId": hypothesisId,
        "location": location,
        "message": message,
        "data": data or {},
        "timestamp": int(time.time() * 1000),
    }
    try:
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")
    except Exception:
        # Logging must never break the producer.
        pass


url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet"
columns = [ \
    'lpep_pickup_datetime', \
    'lpep_dropoff_datetime', \
    'PULocationID', \
    'DOLocationID', \
    'passenger_count', \
    'trip_distance', \
    'tip_amount', \
    'total_amount' \
    ]

df = pd.read_parquet(url, columns=columns)

server = 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=ride_serializer
)

topic_name = 'green-trips'

t0 = time.time()

for i, (_, row) in enumerate(df.iterrows()):
    ride = ride_from_row(row)

    # #region agent log
    if i == 0:
        _debug_log(
            hypothesisId="H_timestamp_format",
            location="producer_hw.py:sample_ride",
            message="Sample produced Ride fields",
            data={
                "pickup_datetime": ride.pickup_datetime,
                "dropoff_datetime": ride.dropoff_datetime,
                "PULocationID": ride.PULocationID,
                "DOLocationID": ride.DOLocationID,
                "passenger_count": ride.passenger_count,
                "trip_distance": ride.trip_distance,
                "tip_amount": ride.tip_amount,
                "total_amount": ride.total_amount,
            },
        )
    # #endregion

    producer.send(topic_name, value=ride)
    print(f"Sent: {ride}")
#    time.sleep(0.01)

producer.flush()

# #region agent log
_debug_log(
    hypothesisId="H_producer_send_completion",
    location="producer_hw.py:producer_flush",
    message="Producer flushed to Kafka",
    data={"topic": topic_name, "server": server},
)
# #endregion

t1 = time.time()
print(f'took {(t1 - t0):.2f} seconds')
