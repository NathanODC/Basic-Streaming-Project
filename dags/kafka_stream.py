import uuid
from datetime import datetime
import json
import time
import logging
import requests
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer


default_args = {"owner": "NathanODC", "start_date": datetime(2023, 9, 3, 10, 00)}

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


def format_data(ti) -> dict:
    """Format raw data retrieved from the API.

    Args:
        ti (TaskInstance): Airflow task instance.

    Returns:
        dict: Formatted data.
    """

    raw_data = ti.xcom_pull(task_ids="get_api_data")

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


def stream_data(ti) -> None:
    """Stream data to Kafka topic.

    Args:
        ti (TaskInstance): Airflow task instance.
    """

    producer = KafkaProducer(
        bootstrap_servers="welcomed-puma-9297-us1-kafka.upstash.io:9092",
        sasl_mechanism="SCRAM-SHA-256",
        security_protocol="SASL_SSL",
        sasl_plain_username=KAFKA_USERNAME,
        sasl_plain_password=KAFKA_PASSWORD,
        max_block_ms=5000,
    )

    curr_time = time.time()

    while True:
        if (
            time.time() > curr_time + 10
        ):  # We use 1 minute, but this is totally manageable
            break
        try:
            res = ti.xcom_pull(task_ids="format_api_data")

            producer.send("users_created", json.dumps(res).encode("utf-8"))
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            continue


with DAG(
    "user_automation",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    get_api_data = PythonOperator(task_id="get_api_data", python_callable=get_data)

    format_api_data = PythonOperator(
        task_id="format_api_data", python_callable=format_data
    )

    stream_to_kafka = PythonOperator(
        task_id="stream_to_kafka", python_callable=stream_data
    )

    get_api_data >> format_api_data >> stream_to_kafka
