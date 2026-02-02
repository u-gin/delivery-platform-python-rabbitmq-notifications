#!/usr/bin/env python3
import pika
import json
from datetime import datetime
import sys

class EventProducer:
    def __init__(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host='localhost',
                port=5672,
                credentials=pika.PlainCredentials('admin', 'admin123')
            )
        )
        self.channel = self.connection.channel()

        # Declare exchange
        self.channel.exchange_declare(
            exchange='exchange.events',
            exchange_type='topic',
            durable=True
        )

    def publish_event(self, routing_key, event_data):
        """Publish event with routing key"""
        message = json.dumps(event_data, indent=2)

        self.channel.basic_publish(
            exchange='exchange.events',
            routing_key=routing_key,
            body=message,
            properties=pika.BasicProperties(
                delivery_mode=2,
                content_type='application/json',
                timestamp=int(datetime.now().timestamp())
            )
        )

        print(f" [x] Published event: {routing_key}")
        print(f"     {message}")

    def close(self):
        self.connection.close()

# Example usage
if __name__ == '__main__':
    producer = EventProducer()

    # Event 1: Order created
    producer.publish_event(
        routing_key='order.created.restaurant_a',
           event_data={
            "event": "order_created",
            "order_id": "ORD-2026-100",
            "timestamp": datetime.now().isoformat(),
            "customer": {
                "name": "Pedro Alves",
                "email": "pedro@example.com",
                "phone": "+351915000000"
            },
            "items": ["Pizza Margherita", "Coke"],
            "total": 15.50
        }
    )

    # Event 2: Order shipped
    producer.publish_event(
        routing_key='order.shipped.north_zone',
        event_data={
            "event": "order_shipped",
            "order_id": "ORD-2026-100",
            "timestamp": datetime.now().isoformat(),
            "courier": "João Rápido",
            "estimated_delivery": "30 minutes"
        }
    )

    producer.close()
    print("\n [✓] All events published")
