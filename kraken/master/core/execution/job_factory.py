#!/usr/bin/env python
# encoding: utf-8

from .execution_job import CopyJobExecution, UploadJobExecution, MockJobExecution
from ....common.model.execution_type import ExecutionType

class JobFactory(object):
        
    def create_job(self, job):
        if (job.etype == ExecutionType.COPY):
            return CopyJobExecution(job.params)
        elif (job.etype == ExecutionType.UPLOAD):
            return UploadJobExecution(job.params)
        elif (job.etype == ExecutionType.MOCK):
            return MockJobExecution(job.params)
        else:
            raise UnknownJobTypeException("Unknown job type %s" % job.etype)

class UnknownJobTypeException(Exception):
    pass