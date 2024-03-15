def consumer_settings_json(broker, username, password) -> dict:
    json = {
        "bootstrap.servers": broker,
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": username,
        "sasl.password": password,
        "group.id": "",
        "auto.commit.interval.ms": 6000,
        "auto.offset.reset": "latest",
    }
    return dict(json)
