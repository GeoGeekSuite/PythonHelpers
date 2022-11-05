from cobra.helper.logging import Logger
import pika
import pickle
import time

class JobEngine():

    def __init__(self, queue):

        self.l = Logger(self)
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
        self.channel = self.connection.channel()
        self.queue = queue
        self.channel.queue_declare(queue=queue)
        self.busy = False

    def listen(self):
        
        self.l.debug('listen')
        while(True):
            time.sleep(5)
            if not self.busy:
                method_frame, header_frame, body = self.channel.basic_get(self.queue)
                if method_frame:
                    self.handle_job(pickle.loads(body))
                    self.channel.basic_ack(method_frame.delivery_tag)
              
    def handle_job(self, job):
        
        self.l.info(f'Handle Job: { job.id } ')
        self.busy = True
        self.l.debug(job.args)

        try:

            self.int_handle_job(job)

        finally:

            self.busy = False

    def int_handle_job(self, job):

        raise Exception("To be subclassed")