#!/usr/bin/env python
# encoding: utf-8

class Task(object):

    def __init__( self,
                tid,
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
        self.tid = tid
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
