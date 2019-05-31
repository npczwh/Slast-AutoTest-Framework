#!/usr/bin/env python
# _*_ coding: utf-8 _*_


class Handler(object):
    def __init__(self, log):
        self.log = log
        self.msg = ''

    def execute(self):
        pass

    def get_message(self):
        return self.msg

