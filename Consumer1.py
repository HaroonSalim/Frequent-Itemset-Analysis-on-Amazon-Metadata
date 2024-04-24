from confluent_kafka import Consumer, KafkaError

def consume_data(topic):
    c = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'consumer_group_1',
        'auto.offset.reset': 'earliest'
    })
    c.subscribe([topic])

    while True:
        msg = c.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break

        print('Consumer 1:', msg.value().decode('utf-8'))

    c.close()

if __name__ == "__main__":
    topic = 'preprocessed_data'
    consume_data(topic)
