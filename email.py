import json

from kafka import KafkaConsumer

ORDER_CONFIRMED_KAFKA_TOPIC = "order_confirmed"

consumer = KafkaConsumer(
    ORDER_CONFIRMED_KAFKA_TOPIC,
    bootstrap_servers="localhost:29092",
)

email_sent_sofar = set()

print("Email is listening...")

while True:
    for message in consumer:
        print("Sending email...")
        consumed_message = json.loads(message.value.decode("utf-8"))
        customer_email = consumed_message["customer_email"]
        print(f"Sending email to {customer_email}")
        email_sent_sofar.add(customer_email)
        print(f"So far emails sent to: {len(email_sent_sofar)} unique emails...")