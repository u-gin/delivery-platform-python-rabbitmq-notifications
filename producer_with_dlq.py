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

# Declare Dead Letter Exchange
channel.exchange_declare(
    exchange='dlx.exchange',
    exchange_type='direct',
    durable=True
)

# Declare Dead Letter Queue
channel.queue_declare(
    queue='dlq.failed_orders',
    durable=True
)

# Bind DLQ to DLX
channel.queue_bind(
    exchange='dlx.exchange',
    queue='dlq.failed_orders',
    routing_key='failed'
)

# Declare main queue with DLX configured
channel.queue_declare(
    queue='orders.with_dlq',
    durable=True,
    arguments={
        'x-dead-letter-exchange': 'dlx.exchange',
        'x-dead-letter-routing-key': 'failed'
    }
)

# Publish test message
message = {
    "order_id": 3001,
    "customer": "Test Customer",
    "items": ["Faulty Item"],  # This will fail processing
    "total": 99.99
}

channel.basic_publish(
    exchange='',
    routing_key='orders.with_dlq',
    body=json.dumps(message),
    properties=pika.BasicProperties(delivery_mode=2)
)

print(" [x] Published order with DLQ support")
connection.close()
