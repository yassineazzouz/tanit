#!/usr/bin/env python
# encoding: utf-8

from .client import MasterClient, MasterWorkerClient

class ClientFactory(object):
    
    def __init__(self, host = "localhost", port = 9090):
        self.host = host
        self.port = port
        
    def create_client(self, name):
        if name == 'master-worker':
            return MasterWorkerClient(self.host, self.port)

        elif name == 'master-client':
            return MasterClient(self.host, self.port)

        else:
            raise NoSuchClientException("No such client [ %s ]", name)

class NoSuchClientException(Exception):
    pass