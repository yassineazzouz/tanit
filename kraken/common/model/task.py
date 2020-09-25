#!/usr/bin/env python
# encoding: utf-8

class Task(object):

    def __init__( self,
                tid,
                etype,
                params):
        self.tid = tid
        self.etype = etype 
        self.params = params