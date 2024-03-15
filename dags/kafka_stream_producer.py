import uuid
from datetime import datetime
import time
import logging
import requests
import os

from airflow.decorators import (
    dag,
    task,
)
from confluent_kafka import Producer

from include.kafka.producer.producer import Kafka


default_args = {"owner": "NathanODC", "retries": 2}

KAFKA_IP = os.getenv("KAFKA_CLUSTER_IP")
KAFKA_USERNAME = os.getenv("KAFKA_CLUSTER_USER")
KAFKA_PASSWORD = os.getenv("KAFKA_CLUSTER_PASS")


def get_data() -> object:
    """Retrieve data from an API.

    Returns:
        object: Data retrieved from the API.
    """

    response = requests.get("https://randomuser.me/api/", timeout=2.5)
    response = response.json()
    response = response["results"][0]

    return response


def format_data(raw_data) -> dict:
    """Format raw data retrieved from the API.

    Args:
        ti (TaskInstance): Airflow task instance.

    Returns:
        dict: Formatted data.
    """

    data = {}
    location = raw_data["location"]
    data["id"] = str(uuid.uuid4())
    data["first_name"] = raw_data["name"]["first"]
    data["last_name"] = raw_data["name"]["last"]
    data["gender"] = raw_data["gender"]
    data["address"] = (
        f"{str(location['street']['number'])} {location['street']['name']}, "
        f"{location['city']}, {location['state']}, {location['country']}"
    )
    data["post_code"] = location["postcode"]
    data["email"] = raw_data["email"]
    data["username"] = raw_data["login"]["username"]
    data["dob"] = raw_data["dob"]["date"]
    data["registered_date"] = raw_data["registered"]["date"]
    data["phone"] = raw_data["phone"]
    data["picture"] = raw_data["picture"]["medium"]

    return data


@dag(
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    default_args=default_args,
    tags=["example"],
)
def gen_api_data_2_kafka() -> None:
    @task()
    def stream_to_kafka() -> None:
        """Stream data to Kafka topic."""

        curr_time = time.time()

        while True:
            if (
                time.time() > curr_time + 180
            ):  # We use 1 minute, but this is totally manageable
                break
            try:
                raw_data = get_data()
                formated_data = format_data(raw_data)

                Kafka.json_producer(
                    broker=KAFKA_IP,
                    username=KAFKA_USERNAME,
                    password=KAFKA_PASSWORD,
                    object_name=formated_data,
                    kafka_topic="users_created",
                )

            except Exception as e:
                logging.error(f"An error occurred: {e}")
                continue

    stream_to_kafka()


gen_api_data_2_kafka()
