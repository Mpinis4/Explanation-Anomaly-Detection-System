from confluent_kafka import Consumer, KafkaException
import json

KAFKA_BROKER = "localhost:38441"  # adjust if needed
EXPLANATIONS_TOPIC = "explanations_topic"

conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'explanations_consumer',
    'auto.offset.reset': 'earliest',
}

consumer = Consumer(conf)
consumer.subscribe([EXPLANATIONS_TOPIC])

print("Listening for explanations... Ctrl+C to stop.")

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())
        
        payload = json.loads(msg.value())
        window_size = payload.get("window", {}).get("size_events", "?")
        explanations = payload.get("explanations", [])

        print(f"\n=== Window size: {window_size} ===")
        for exp in explanations:
            items = exp.get("items", [])
            risk_ratio = exp.get("risk_ratio", 0)
            support_outlier = exp.get("support_outlier", 0)
            support_inlier = exp.get("support_inlier", 0)
            print(f"Feature(s): {items}, Risk ratio: {risk_ratio:.2f}, Support (out/in): {support_outlier}/{support_inlier}")

except KeyboardInterrupt:
    print("Stopping consumer...")
finally:
    consumer.close()
