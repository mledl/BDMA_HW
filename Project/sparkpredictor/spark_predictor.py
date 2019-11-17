from kafka import KafkaProducer, KafkaConsumer
from loguru import logger
import pandas as pd
import json

NEW_TXS_TOPIC = "new_txs"
FRAUD_TXS_TOPIC = "fraud_txs"

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

consumer = KafkaConsumer(NEW_TXS_TOPIC,
                         group_id='spark_predictor_group',
                         auto_offset_reset='earliest',
                         value_deserializer=lambda m: json.loads(m.decode('ascii')),
                         bootstrap_servers=['localhost:9092'])
for message in consumer:
    # if model true then produce message
    producer \
        .send(FRAUD_TXS_TOPIC, message.value, str(message.key)) \
        .add_callback(on_send_success).add_errback(on_send_error)

    producer.flush()


metrics = producer.metrics()

logger.info(metrics)