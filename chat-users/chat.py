import pika
import threading
import os
from dotenv import load_dotenv

load_dotenv()
url = os.getenv("rabbitMQURL")


def init():
    params = pika.URLParameters(url)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    exchange_name = "chatUserExchange"
    channel.exchange_declare(
        exchange=exchange_name, exchange_type="fanout", durable=True
    )

    channel.close()
    connection.close()


def send(message, user_id):
    params = pika.URLParameters(url)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    properties = pika.BasicProperties(user_id=user_id)
    channel.basic_publish(exchange="chatUserExchange", routing_key="", body=message, properties=properties)

    channel.close()
    connection.close()


def receive(queueName):
    params = pika.URLParameters(url)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    channel.queue_declare(queue=queueName, auto_delete=True, exclusive=True)
    channel.queue_bind(exchange="chatUserExchange", queue=queueName)

    channel.basic_consume(queue=queueName, on_message_callback=callback, auto_ack=True)

    channel.start_consuming()


def callback(ch, method, properties, body):
    user_id = getattr(properties, 'user_id', '-')
    print(user_id + ": " + body.decode("utf-8"))


# Main
if __name__ == "__main__":
    user_id = input("User ID: ")
    init()
    consumer_thread = threading.Thread(target=receive, args=[str(os.getpid())])
    consumer_thread.daemon = (
        True  # Set as a daemon so it will be killed once the main thread is dead
    )
    consumer_thread.start()

    while True:
        message = input()
        send(message, user_id)
