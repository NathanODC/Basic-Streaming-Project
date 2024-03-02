import logging

from cassandra.cluster import Cluster


def create_keyspace(session: object) -> None:
    """Create a keyspace if it does not exist.

    Args:
        session (object): Cassandra session object.
    """

    session.execute(
        """
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """
    )


def create_table(session: object) -> None:
    """Create a table if it does not exist.

    Args:
        session (object): Cassandra session object.
    """

    session.execute(
        """
    CREATE TABLE IF NOT EXISTS spark_streams.created_users (
        id UUID PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        address TEXT,
        post_code TEXT,
        email TEXT,
        username TEXT,
        registered_date TEXT,
        phone TEXT,
        picture TEXT);
    """
    )


def insert_data(session: object, **kwargs) -> None:
    """Insert data into Cassandra table.

    Args:
        session (object): Cassandra session object.
        **kwargs: Arbitrary keyword arguments representing data to be inserted.
    """

    print("Inserting data...")

    user_id = kwargs.get("id")
    first_name = kwargs.get("first_name")
    last_name = kwargs.get("last_name")
    gender = kwargs.get("gender")
    address = kwargs.get("address")
    postcode = kwargs.get("post_code")
    email = kwargs.get("email")
    username = kwargs.get("username")
    dob = kwargs.get("dob")
    registered_date = kwargs.get("registered_date")
    phone = kwargs.get("phone")
    picture = kwargs.get("picture")

    try:
        session.execute(
            """
            INSERT INTO spark_streams.created_users(id, first_name, last_name, gender, address, 
                post_code, email, username, dob, registered_date, phone, picture)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
            (
                user_id,
                first_name,
                last_name,
                gender,
                address,
                postcode,
                email,
                username,
                dob,
                registered_date,
                phone,
                picture,
            ),
        )
        logging.info(f"Data inserted for {first_name} {last_name}")

    except Exception as e:
        logging.error(f"Could not insert data due to {e}")


def create_cassandra_connection() -> object:
    """Create a connection to Cassandra.

    Returns:
        object: Cassandra session object.
    """

    try:
        # Connecting to the Cassandra cluster
        cluster = Cluster(["host.docker.internal"])
        cas_session = cluster.connect()
        return cas_session
    except Exception as e:
        logging.error(f"Could not create Cassandra connection due to {e}")
        return None


if __name__ == "__main__":

    session = create_cassandra_connection()

    if session is not None:
        create_keyspace(session)
        create_table(session)

        logging.info("Keyspace and table created successfully!")
