import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

KAFKA_USERNAME = os.getenv("KAFKA_CLUSTER_USER")
KAFKA_PASSWORD = os.getenv("KAFKA_CLUSTER_PASS")


def create_spark_connection() -> object:
    """
    Create a Spark session.

    Returns:
        object: A SparkSession object.
    """

    s_conn = None

    try:
        s_conn = (
            SparkSession.builder.appName("SparkDataStreaming")
            .config(
                "spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,"
                "com.datastax.spark:spark-cassandra-connector_2.12:3.4.0,",
            )
            .config("spark.cassandra.connection.host", "host.docker.internal")
            .getOrCreate()
        )

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn


def connect_to_kafka(spark_conn: object) -> object:
    """
    Connect to Kafka cluster.

    Args:
        spark_conn (object): A SparkSession object.

    Returns:
        object: A DataFrame representing Kafka data.
    """

    spark_df = None

    try:
        spark_df = (
            spark_conn.readStream.format("kafka")
            .option(
                "kafka.bootstrap.servers",
                "welcomed-puma-9297-us1-kafka.upstash.io:9092",
            )
            .option("kafka.sasl.mechanism", "SCRAM-SHA-256")
            .option("kafka.security.protocol", "SASL_SSL")
            .option(
                "kafka.sasl.jaas.config",
                f'org.apache.kafka.common.security.scram.ScramLoginModule required username="{KAFKA_USERNAME}" password="{KAFKA_PASSWORD}";',
            )
            .option("startingOffsets", "earliest")
            .option("subscribe", "users_created")
            .load()
        )
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df


def create_selection_df_from_kafka(spark_df: object) -> object:
    """
    Create a DataFrame from Kafka data.

    Args:
        spark_df (object): A DataFrame representing Kafka data.

    Returns:
        object: A DataFrame with selected columns.
    """

    schema = StructType(
        [
            StructField("id", StringType(), False),
            StructField("first_name", StringType(), False),
            StructField("last_name", StringType(), False),
            StructField("gender", StringType(), False),
            StructField("address", StringType(), False),
            StructField("post_code", StringType(), False),
            StructField("email", StringType(), False),
            StructField("username", StringType(), False),
            StructField("registered_date", StringType(), False),
            StructField("phone", StringType(), False),
            StructField("picture", StringType(), False),
        ]
    )

    sel = (
        spark_df.selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema).alias("data"))
        .select("data.*")
    )
    return sel


if __name__ == "__main__":
    # create spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # connect to kafka with spark connection
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)

        logging.info("Streaming is being started...")

        streaming_query = (
            selection_df.writeStream.format("org.apache.spark.sql.cassandra")
            .option("checkpointLocation", "/tmp/checkpoint")
            .option("keyspace", "spark_streams")
            .option("table", "created_users")
            .start()
        )

        streaming_query.awaitTermination()
