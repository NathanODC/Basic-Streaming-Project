# JSON File Format
def on_delivery_json(err, msg):
    if err is not None:
        print("Message delivery failed: {}".format(err))
    else:
        print(
            "Message sucessfully produced to {} [{}] at offset {}".format(
                msg.topic(), msg.partition(), msg.offset()
            )
        )


# AVRO File Format
def on_delivery_avro(err, msg, obj):
    if err is not None:
        print(
            "Message {} delivery failed for user {} with error {}".format(
                obj, obj.name, err
            )
        )
    else:
        print(
            "Message {} successfully produced to {} [{}] at offset {}".format(
                obj, msg.topic(), msg.partition(), msg.offset()
            )
        )
