from cobra.helper.logging import Logger
import pika
import pickle

class Client:
    
    def __init__(self, queue):
        
        self.l = Logger(self)
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=queue)
        self.routing_key = queue
        
    def send_message(self, message):
        
        self.l.debug('send')
        self.channel.basic_publish(exchange='', routing_key=self.routing_key, body=pickle.dumps(message))