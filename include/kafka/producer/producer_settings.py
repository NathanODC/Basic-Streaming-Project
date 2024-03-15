def produce_settings_json(broker, username, password) -> dict:
    json = {
        "client.id": "Real-Time-Project",
        "bootstrap.servers": broker,
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": username,
        "sasl.password": password,
        "acks": "all",
        "enable.idempotence": "true",
        "linger.ms": 100,
        "compression.type": "gzip",
        "max.in.flight.requests.per.connection": 5,
        # "session.timeout.ms": 45000,
    }
    return dict(json)
