from cobra.helper.logging import Logger
import uuid

class Job:
    
    def __init__(self, args, job_type=None):
        
        self.args = args
        self.id = uuid.uuid1()
        self.job_type = job_type