#!/usr/bin/env python3
import pika
import json

# Connection parameters
connection = pika.BlockingConnection(
    pika.ConnectionParameters(
        host='localhost',
        port=5672,
        credentials=pika.PlainCredentials('admin', 'admin123')
    )
)
channel = connection.channel()

# Declare queue (idempotent - safe to call multiple times)
channel.queue_declare(queue='orders.process', durable=True)

# Message to send
message = {
    "order_id": 2001,
    "customer": "Maria Santos",
    "items": ["Laptop", "Mouse"],
    "total": 850.00
}

# Publish message
channel.basic_publish(
    exchange='',  # Default exchange
    routing_key='orders.process',  # Queue name
    body=json.dumps(message),
    properties=pika.BasicProperties(
        delivery_mode=2,  # Make message persistent
        content_type='application/json'
    )
)

print(f" [x] Sent order {message['order_id']}")

connection.close()