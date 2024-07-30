import pika
import threading
import os
from dotenv import load_dotenv

load_dotenv()
url = os.getenv('rabbitMQURL')

def init():
    params = pika.URLParameters(url)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    exchange_name = 'chatGroupExchange'
    channel.exchange_declare(exchange=exchange_name, exchange_type='direct', durable=True)

    channel.close()
    connection.close()

def send(message, groupName):
    params = pika.URLParameters(url)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    channel.basic_publish(exchange="chatGroupExchange",
                      routing_key=groupName,
                      body= message)
    
    channel.close()
    connection.close()

def receive(groupName):
    params = pika.URLParameters(url)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    channel.queue_declare(queue=groupName, auto_delete=False)
    channel.queue_bind(exchange='chatGroupExchange', queue=groupName, routing_key=groupName)
    
    channel.basic_consume(queue=groupName, on_message_callback=callback, auto_ack=True)

    channel.start_consuming()

def callback(ch, method, properties, body):
    print(body.decode("utf-8"))

# Main
if __name__ == "__main__":
    init()
    groupName = input("Group Name: ")
    consumer_thread = threading.Thread(target=receive, args=[groupName])
    consumer_thread.daemon = True  # Set as a daemon so it will be killed once the main thread is dead
    consumer_thread.start()

    while True:
        message = input()
        send(message, groupName)