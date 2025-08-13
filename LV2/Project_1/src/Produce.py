import json
from kafka import KafkaProducer

# Cấu hình Kafka
BOOTSTRAP_SERVERS = 'kafka-0:9092,kafka-1:9092,kafka-2:9092'  # Server Kafka chung của team
USERNAME = '[Không tiết lộ được]'  # Thay bằng username thực tế
PASSWORD = '[Không tiết lộ được]'  # Thay bằng password thực tế
TOPIC = '[Không tiết lộ được]'  # Thay bằng topic chung của team

def produce_message(message):
    try:
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            security_protocol='SASL_PLAINTEXT',
            sasl_mechanism='PLAIN',
            sasl_plain_username=USERNAME,
            sasl_plain_password=PASSWORD,
            value_serializer=lambda v: json.dumps(v).encode('utf-8') if isinstance(v, dict) else v.encode('utf-8')
        )
        producer.send(TOPIC, message)
        producer.flush()
        producer.close()
        print(f"Message produced to topic {TOPIC}: {message}")
    except Exception as e:
        print(f"Error producing message: {e}")

if __name__ == "__main__":
    # Ví dụ gửi message
    produce_message("Test message from Producer via Kafka KRaft (Team Shared)")
    # Hoặc gửi JSON
    # produce_message({"key": "value", "message": "Test JSON from Team"})