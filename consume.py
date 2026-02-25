from google.cloud import pubsub_v1
import json

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(
    "rainbow-data-production-483609", "test-order-topic-sub"
)


def callback(message):
    data = json.loads(message.data.decode())
    print(f"Received: {data}")
    message.ack()


flow_control = pubsub_v1.types.FlowControl(max_messages=10)
streaming_pull_future = subscriber.subscribe(
    subscription_path, callback=callback, flow_control=flow_control
)

print(f"Listening for messages on {subscription_path}...")
try:
    streaming_pull_future.result()
except KeyboardInterrupt:
    streaming_pull_future.cancel()
