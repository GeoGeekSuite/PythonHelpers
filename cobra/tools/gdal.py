from cobra.helper.logging import Logger
import time
import pika
import pickle
import uuid
import subprocess
import os

class GdalJob:
    
    def __init__(self, args, job_type=None):
        
        self.args = args
        self.id = uuid.uuid1()
        self.job_type = job_type

class ImportJob(GdalJob):

    def __init__(self, path_to_file, args=[]):

        self.args = args
        self.id = uuid.uuid1()
        self.path_to_file = path_to_file
        #TODO: Remove
        self.job_type = 'load_shape'


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

            if job.job_type == 'generic':

                return_value = self.handle_generic_job(job)

                if return_value.returncode == 0:

                    self.l.info(f'Job {job.id} finished successfully')

                else: 

                    self.l.error(f'Error in {job.id} failed')

            if job.job_type == 'load_shape':

                return_value = self.load_shape(job)
        
            

        finally:
            self.busy = False

    def handle_generic_job(self, job):

        self.l.info(f'Handle generic job')

        return_value = subprocess.run(job.args)
        print(return_value)
        return return_value

    def load_shape(self, job):

        self.l.info(f'load_shape ')

        host = os.environ['PGHOST']
        database = os.environ['PGDATABASE']
        user = os.environ['PGUSER']
        password = os.environ['PGPASSWORD']

        executable = ['ogr2ogr']
        target = ['-f','PostgreSQL' ]
        connection_string = [f'PG: host={host} dbname={database} user={user} password={password}']
        
        #TODO: Add schema

        args = executable + target + connection_string + [job.path_to_file]
        
        return_value = subprocess.run(args)
        print(return_value)
        return return_value

class GdalClient:
    
    def __init__(self):
        
        self.l = Logger(self)
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue='gdal')
        
    def _send_message(self, message):
        
        self.l.debug('send')
        self.channel.basic_publish(exchange='', routing_key='gdal', body=pickle.dumps(message))