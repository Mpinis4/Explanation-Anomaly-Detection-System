from bytewax.connectors.kafka import KafkaSource
from config import KAFKA_BROKER, KAFKA_TOPIC

def kafka_input():
    return KafkaSource([KAFKA_BROKER], [KAFKA_TOPIC])