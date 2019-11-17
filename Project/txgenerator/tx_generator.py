from kafka import KafkaProducer
from loguru import logger
import pandas as pd
import json

NEW_TXS_TOPIC = "new_txs"

def on_send_success(record_metadata):
    logger.success(record_metadata)

def on_send_error(excp):
    logger.error(excp)

data = pd.read_csv('../data/test_identity_transaction.csv')


# kafka.master-thesis_kafka-network
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda m: json.dumps(m).encode('ascii'),
    key_serializer=str.encode,
    client_id="tx_generator",
    api_version=(0, 10, 1)
)

for index, row in data.head().iterrows():
    tx = row.to_json()
    print(row['TransactionID'])
    producer \
        .send(NEW_TXS_TOPIC, row.to_json(), str(row['TransactionID'])) \
        .add_callback(on_send_success).add_errback(on_send_error)

producer.flush()

metrics = producer.metrics()

logger.info(metrics)