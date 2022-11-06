from cobra.helper.logging import Logger
from cobra.tools.job import Job
from cobra.tools.job_engine import JobEngine
import time
import subprocess

class Osm2pgsqlJob(Job):

    def __init__(self, path_to_file, style_file = None, args=[]):

        super().__init__(args, job_type='osm2pgsql', job_queue='osm2pgsql')
        self.path_to_file = path_to_file

class Osm2pgsqlEngine(JobEngine):

    def __init__(self, queue):

        super().__init__(queue)

    def int_handle_job(self, job):

        #TODO: Check if postgis extenstion is enabled

        self.l.debug('int_handle_job')
        #  args =['osm2pgsql','-c','-S', f'/styles/{style}', f'--output-pgsql-schema={schema}', '--slim', path_to_pbf]
        args =['osm2pgsql','-c','-S', f'/download/default.style', '--slim', job.path_to_file]
        print(args)
        return_value = subprocess.run(args)
        print(return_value)
