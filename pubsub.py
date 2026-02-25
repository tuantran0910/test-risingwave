from google.cloud import pubsub_v1

PROJECT_ID = "rainbow-data-production-483609"
TOPIC_ID = "risingwave"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project=PROJECT_ID, topic=TOPIC_ID)

data_str = "Hello from RisingWave! Another"
data = data_str.encode("utf-8")

future = publisher.publish(topic_path, data)
print(f"Published message ID: {future.result()}")
