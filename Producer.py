from confluent_kafka import Producer
import json
import time

def produce_data(topic, data_file):
    p = Producer({'bootstrap.servers': 'localhost:9092'})

    with open(data_file, 'r', encoding='utf-8') as file:
        for line in file:
            message = json.loads(line)
            p.produce(topic, json.dumps(message))
            time.sleep(0.1)  # Simulate real-time streaming
            p.poll(0)

    p.flush()

if __name__ == "__main__":
    topic = 'preprocessed_data'
    data_file = 'Preprocessed_Amazon_Meta.json'
    produce_data(topic, data_file)
