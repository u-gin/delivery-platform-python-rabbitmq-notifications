#!/usr/bin/env python3
import pika
import json
import time

# Connection parameters
connection = pika.BlockingConnection(
    pika.ConnectionParameters(
        host='localhost',
        port=5672,
        credentials=pika.PlainCredentials('admin', 'admin123')
    )
)
channel = connection.channel()

# Declare queue (ensure it exists)
channel.queue_declare(queue='orders.process', durable=True)

# Set QoS (Quality of Service) - process 1 message at a time
channel.basic_qos(prefetch_count=1)

def callback(ch, method, properties, body):
    """Process received message"""
    try:
        # Parse JSON message
        message = json.loads(body)
        print(f" [x] Received order: {message['order_id']}")
        print(f"     Customer: {message['customer']}")
        print(f"     Total: ${message['total']}")

        # Simulate processing time
        time.sleep(2)

        print(f" [✓] Order {message['order_id']} processed")

        # Acknowledge message (remove from queue)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        print(f" [✗] Error processing message: {e}")
        # Reject and requeue message
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

# Start consuming
print(' [*] Waiting for orders. To exit press CTRL+C')
channel.basic_consume(
    queue='orders.process',
    on_message_callback=callback,
    auto_ack=False  # Manual acknowledgment
)

try:
    channel.start_consuming()
except KeyboardInterrupt:
    print('\n [!] Interrupted')
    connection.close()