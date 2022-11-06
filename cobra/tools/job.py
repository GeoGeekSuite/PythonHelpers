from cobra.helper.logging import Logger
import uuid
import pika
import pickle

class Job:
    
    def __init__(self, args, job_queue=None, job_type=None):
        
        self.args = args
        self.id = uuid.uuid1()
        self.job_type = job_type
        self.job_queue = job_queue

class JobClient:

    def __init__(self):
        
        self.l = Logger(self)
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
        self.channel = self.connection.channel()
        
        
    def _send_message(self, job):
        
        self.l.debug('send')
        self.channel.queue_declare(queue=job.job_queue)
        self.channel.basic_publish(exchange='', routing_key=job.job_queue, body=pickle.dumps(job))