# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

from kafka import KafkaProducer, KafkaConsumer
import json
import random
import time
from datetime import datetime
from kafka.admin import KafkaAdminClient

# Step 1: Kafka Topic Names
BUILDING_SENSORS_TOPIC = "building_sensors_olena"
TEMPERATURE_ALERTS_TOPIC = "temperature_alerts_olena"
HUMIDITY_ALERTS_TOPIC = "humidity_alerts_olena"

# Function to list and print topics
def list_kafka_topics():
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers="localhost:9092",
            client_id="test_admin"
        )
        topics = admin_client.list_topics()
        print("Available Kafka Topics:")
        for topic in topics:
            print(f"- {topic}")
    except Exception as e:
        print(f"Error listing topics: {e}")

# Step 2: Sensor Data Producer
class SensorDataProducer:
    def __init__(self, topic):
        self.producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.topic = topic
        self.sensor_id = random.randint(1000, 9999)

    def send_data(self):
        while True:
            temperature = random.uniform(25, 45)
            humidity = random.uniform(15, 85)
            timestamp = datetime.now().isoformat()

            data = {
                "sensor_id": self.sensor_id,
                "timestamp": timestamp,
                "temperature": temperature,
                "humidity": humidity
            }

            self.producer.send(self.topic, data)
            print(f"Sent data: {data}")
            time.sleep(2)  # Sending data every 2 seconds

# Step 3: Data Processor
class SensorDataProcessor:
    def __init__(self, input_topic, temp_alert_topic, hum_alert_topic):
        self.consumer = KafkaConsumer(
            input_topic,
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )
        self.temp_producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.hum_producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.temp_alert_topic = temp_alert_topic
        self.hum_alert_topic = hum_alert_topic

    def process_data(self):
        for message in self.consumer:
            data = message.value
            sensor_id = data['sensor_id']
            temperature = data['temperature']
            humidity = data['humidity']
            timestamp = data['timestamp']

            if temperature > 40:
                alert = {
                    "sensor_id": sensor_id,
                    "timestamp": timestamp,
                    "temperature": temperature,
                    "message": "High temperature alert!"
                }
                self.temp_producer.send(self.temp_alert_topic, alert)
                print(f"Sent temperature alert: {alert}")

            if humidity > 80 or humidity < 20:
                alert = {
                    "sensor_id": sensor_id,
                    "timestamp": timestamp,
                    "humidity": humidity,
                    "message": "Humidity level out of range!"
                }
                self.hum_producer.send(self.hum_alert_topic, alert)
                print(f"Sent humidity alert: {alert}")

# Step 4: Alert Reader
class AlertReader:
    def __init__(self, topics):
        self.consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )

    def read_alerts(self):
        for message in self.consumer:
            alert = message.value
            print(f"Received alert: {alert}")

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["produce", "process", "read", "list_topics"], help="Mode to run: produce, process, read, or list_topics")
    args = parser.parse_args()

    if args.mode == "produce":
        producer = SensorDataProducer(BUILDING_SENSORS_TOPIC)
        producer.send_data()

    elif args.mode == "process":
        processor = SensorDataProcessor(BUILDING_SENSORS_TOPIC, TEMPERATURE_ALERTS_TOPIC, HUMIDITY_ALERTS_TOPIC)
        processor.process_data()

    elif args.mode == "read":
        reader = AlertReader([TEMPERATURE_ALERTS_TOPIC, HUMIDITY_ALERTS_TOPIC])
        reader.read_alerts()

    elif args.mode == "list_topics":
        list_kafka_topics()

