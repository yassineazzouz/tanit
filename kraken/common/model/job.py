#!/usr/bin/env python
# encoding: utf-8

class Job(object):

    def __init__( self,
                jid,
                src,
                dest,
                src_path,
                dest_path,
                include_pattern="*",
                min_size=0,
                preserve=True,
                force=True,
                checksum=True,
                files_only=False,
                part_size=65536,
                buffer_size=65536):
        self.jid = jid
        self.src = src
        self.dest = dest
        self.src_path = src_path
        self.dest_path = dest_path
        self.include_pattern = include_pattern
        self.min_size = min_size
        self.preserve = preserve
        self.force = force
        self.checksum = checksum
        self.files_only = files_only
        self.part_size = part_size
        self.buffer_size = buffer_size
        

class JobStatus(object):
    def __init__(self, jid, state, submission_time, start_time, finish_time, execution_time):
        self.jid = jid
        self.state = state
        self.submission_time = submission_time
        self.start_time = start_time
        self.finish_time = finish_time
        self.execution_time = execution_time
        
    def __str__(self):
        return "JobStatus { id: %s, state: %s, submission_time: %s, start_time: %s, finish_time: %s, execution_time: %s}" % (
            self.jid,
            self.state,
            self.submission_time,
            self.start_time,
            self.finish_time,
            self.execution_time)