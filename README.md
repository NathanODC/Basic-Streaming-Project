# Random API Data Streaming to Kafka and Storing in Cassandra

This project demonstrates a data pipeline that retrieves data from a random API using Apache Airflow, streams it into a Kafka cluster, and then consumes the data with Apache Spark and finally storing it into Cassandra.

## Overview

The data pipeline consists of the following steps:

1. **Retrieving Data from a Random API**: Apache Airflow is used to periodically fetch data from a random API. This data includes information about random users.

2. **Streaming Data to Kafka**: The retrieved data is streamed into a Kafka cluster using a Python script. Kafka serves as a message broker for real-time data processing.

3. **Consuming Data with Apache Spark**: Apache Spark reads the data from Kafka using a structured streaming approach. It processes the data and performs necessary transformations.

4. **Storing Data in Cassandra**: The processed data is then stored in Cassandra, a distributed NoSQL database designed for handling large volumes of data.

## Prerequisites

Before running the pipeline, ensure you have the following installed:

- Python (Version used 3.9.5)
- Docker
- Apache Airflow
- Apache Kafka
- Apache Spark
- Cassandra

## Important notes

I used astronomer on my local environment to test the airflow stuff and confluent services to hold my kafka cluster. Take your cluster endpoint ip, kafka cluster user and password and store all those infos on .env file. This file serves as environment variables for each container.

## Roadmap

- Turn spark submit automatically;
- Create tables on Cassandra on container creation;
- Move to another kafka cluster solution to test with more data volume;

## License

This project is licensed under the [MIT License](LICENSE).
