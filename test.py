import base64
from json import loads
from kafka import KafkaConsumer
consumer = KafkaConsumer(
    'demo1',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='test-consumer-group',
    bootstrap_servers=['192.168.62.212:9092','192.168.62.214:9093','192.168.62.209:9094']
)
print(consumer.bootstrap_connected)

for msg in consumer:
    print(msg.value)
print("d")