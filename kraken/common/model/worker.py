#!/usr/bin/env python
# encoding: utf-8

class Worker(object):
        def __init__( self,
                      wid,
                      address,
                      port):
            self.wid = wid
            self.address = address
            self.port = port
            
class WorkerStatus(object):
        def __init__( self,
                      wid,
                      running,
                      pending,
                      available):
            self.wid = wid
            self.running = running
            self.pending = pending
            self.available = available