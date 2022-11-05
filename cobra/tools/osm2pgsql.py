from cobra.helper.logging import Logger
from cobra.tools.job import Job
from cobra.tools.job_engine import JobEngine
import time

class Osm2pgsqlJob(Job):

    def __init__(self, path_to_file, style_file = None, args=[]):

        super().__init__(args, job_type='osm2pgsql')

class Osm2pgsqlEngine(JobEngine):

    def __init__(self, queue):

        super().__init__(queue)

    def int_handle_job(self, job):

        self.l.debut('int_handle_job')
