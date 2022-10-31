from cobra.helper.logging import Logger
import time
import pika
import pickle
import uuid
import subprocess

class GdalJob:
    
    def __init__(self, args):
        
        self.args = args
        self.id = uuid.uuid1()


class GdalEngine:
    
    def __init__(self):
        
        self.l = Logger(self)
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue='gdal')
        self.busy = False
        
    def listen(self):
        
        self.l.debug('listen')
        while(True):
            time.sleep(5)
            if not self.busy:
                method_frame, header_frame, body = self.channel.basic_get('gdal')
                if method_frame:
                    self.handle_job(pickle.loads(body))
                    self.channel.basic_ack(method_frame.delivery_tag)
                
    def handle_job(self, job):
        
        self.l.info(f'Handle Job: { job.id } ')
        self.busy = True
        self.l.debug(job.args)
        try: 
            return_value = subprocess.run(job.args)
        
            if return_value.returncode == 0:
                self.l.info(f'Job {job_id} - export to {filename} finished successfully')

            else: 
                self.l.error(f'Error in {job_id} - export {filename} failed')

        
        finally:
            self.busy = False

class GdalClient:
    
    def __init__(self):
        
        self.l = Logger(self)
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue='gdal')
        
    def _send_message(self, message):
        
        self.l.debug('send')
        self.channel.basic_publish(exchange='', routing_key='gdal', body=pickle.dumps(message))