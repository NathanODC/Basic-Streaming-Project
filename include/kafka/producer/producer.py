import json

from confluent_kafka import Producer

from include.kafka.producer.delivery_reports import on_delivery_json
from include.kafka.producer.producer_settings import produce_settings_json


class Kafka(object):

    @staticmethod
    def json_producer(broker, username, password, object_name, kafka_topic):

        p = Producer(produce_settings_json(broker, username, password))

        get_data = object_name
        # for key, value in get_data.items(): #* If we want to define key - value pairs inside each message on the json data
        try:
            p.poll(0)
            p.produce(
                topic=kafka_topic,
                value=json.dumps(get_data).encode("utf-8"),
                callback=on_delivery_json,
            )
        except BufferError:
            print("Buffer full")
            p.poll(0.1)
        except ValueError:
            print("Invalid input")
        except KeyboardInterrupt:
            raise

        p.flush()
