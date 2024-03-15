import time
import os

from confluent_kafka import KafkaException, Consumer

from include.kafka.consumer.consumer_settings import consumer_settings_json


kafka_topic = "users_create"
execution_time = 100
broker = os.getenv("KAFKA_CLUSTER_IP")
username = os.getenv("KAFKA_CLUSTER_USER")
password = os.getenv("xcHERCx0WrW7fqdQnKgV+pXoC+ImXA0KdB31LHeDa3e6jYkIxCUZcPuhTuSlC7pA")

c = Consumer(consumer_settings_json(broker, username, password))

c.subscribe([kafka_topic])

timeout = time.time() + int(execution_time)

try:
    while timeout > time.time():
        events = c.poll(0.1)
        if events is None:
            continue
        if events.error():
            raise KafkaException(events.error())
        print(
            events.topic(),
            events.partition(),
            events.offset(),
            events.value().decode("utf-8"),
        )
except KeyboardInterrupt:
    pass
finally:
    c.close()
