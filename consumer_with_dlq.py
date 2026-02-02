#!/usr/bin/env python3
import pika
import json

connection = pika.BlockingConnection(
    pika.ConnectionParameters(
        host='localhost',
        port=5672,
        credentials=pika.PlainCredentials('admin', 'admin123')
    )
)
channel = connection.channel()

def callback(ch, method, properties, body):
    message = json.loads(body)
    print(f" [x] Processing order {message['order_id']}")

    # Simulate validation failure
    if "Faulty Item" in message.get('items', []):
        print(f" [✗] Invalid item detected! Rejecting message...")
        # Reject without requeue → goes to DLQ
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    else:
        print(f" [✓] Order processed successfully")
        ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_consume(
    queue='orders.with_dlq',
    on_message_callback=callback,
    auto_ack=False
)

print(' [*] Waiting for orders (with DLQ). Press CTRL+C to exit')
channel.start_consuming()
