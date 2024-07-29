import pika
import threading
import os

url = os.environ(['home'])

def init():
    params = pika.URLParameters(url)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    exchange_name = 'chatExchange'
    channel.exchange_declare(exchange=exchange_name, exchange_type='fanout', durable=True)

    channel.close()
    connection.close()

def send(message, username):
    params = pika.URLParameters(url)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    channel.basic_publish(exchange="chatExchange",
                      routing_key='',
                      body= username + ": " + message)
    
    channel.close()
    connection.close()

def receive(queueName):
    params = pika.URLParameters(url)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    channel.queue_declare(queue=queueName, auto_delete=True, exclusive=True)
    channel.queue_bind(exchange='chatExchange', queue=queueName)
    
    channel.basic_consume(queue=queueName, on_message_callback=callback, auto_ack=True)

    channel.start_consuming()

def callback(ch, method, properties, body):
    print(body.decode("utf-8"))

# Main
if __name__ == "__main__":
    init()
    username = input("Username: ")
    consumer_thread = threading.Thread(target=receive, args=[username])
    consumer_thread.daemon = True  # Set as a daemon so it will be killed once the main thread is dead
    consumer_thread.start()

    while True:
        message = input()
        send(message, username)