#!/usr/bin/env python
# encoding: utf-8

from .execution_job import JobExecution

class JobFactory(object):
        
    def create_job(self, conf):
        return JobExecution(conf)