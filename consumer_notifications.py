#!/usr/bin/env python3
import pika 
import json

class NotificationConsumer:
    def __init__(self, queue_name, notification_type):
        self.queue_name = queue_name
        self.notification_type = notification_type

        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host='localhost',
                port=5672,
                credentials=pika.PlainCredentials('admin', 'admin123')
            )
        )
        self.channel = self.connection.channel()

        # Ensure queue exists
        self.channel.queue_declare(queue=queue_name, durable=True)
        self.channel.basic_qos(prefetch_count=1)

    def process_message(self, ch, method, properties, body):
        """Process notification"""
        try:
            event = json.loads(body)

            print(f"\n [{self.notification_type.upper()}] New notification")
            print(f" Event: {event.get('event', 'unknown')}")
            print(f" Order ID: {event.get('order_id', 'N/A')}")

            if event.get('event') == 'order_created':
                customer = event.get('customer', {})
                print(f" → Sending {self.notification_type} to {customer.get('name')}")
                print(f"    Contact: {customer.get('email') or customer.get('phone')}")

            elif event.get('event') == 'order_shipped':
                print(f" → Courier: {event.get('courier')}")
                print(f"    ETA: {event.get('estimated_delivery')}")

            # Simulate sending notification
            import time
            time.sleep(1)

            print(f" [✓] {self.notification_type.capitalize()} notification sent!")

            ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            print(f" [✗] Error: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    def start(self):
        """Start consuming messages"""
        print(f' [*] {self.notification_type.upper()} consumer started')
        print(f' [*] Waiting for messages from queue: {self.queue_name}')
        print(' [*] Press CTRL+C to exit\n')

        self.channel.basic_consume(
            queue=self.queue_name,
            on_message_callback=self.process_message,
            auto_ack=False
        )

        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            print('\n [!] Interrupted')
            self.connection.close()

# Example usage
if __name__ == '__main__':
    import sys

    if len(sys.argv) < 2:
        print("Usage: python consumer_notifications.py [created|shipped|audit]")
        sys.exit(1)

    consumer_type = sys.argv[1]

    queue_map = {
        'created': ('notifications.created', 'email/push'),
        'shipped': ('notifications.shipped', 'sms'),
        'audit': ('audit.logs', 'audit')
    }

    if consumer_type not in queue_map:
        print(f"Invalid type. Choose: {', '.join(queue_map.keys())}")
        sys.exit(1)

    queue_name, notif_type = queue_map[consumer_type]
    consumer = NotificationConsumer(queue_name, notif_type)
    consumer.start()
