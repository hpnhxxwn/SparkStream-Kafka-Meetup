from kafka import KafkaProducer, KafkaClient, SimpleProducer
import json, requests

kafka = KafkaClient('localhost:9092')
producer = KafkaProducer(bootstrap_servers=["localhost:9092"])
"""
producer = SimpleProducer(kafka)
producer.send_messages('meetup',line)
"""

from contextlib import closing


r = requests.get("https://stream.meetup.com/2/rsvps", stream=True)
print type(producer)

for line in r.iter_lines():
    if line:
        producer.send("meetup", line)
        print(line)
        print type(line)

kafka.close()

