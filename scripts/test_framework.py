#!/usr/bin/env python
# _*_ coding: utf-8 _*_


class TestFramework(object):
    def __init__(self, config):
        self.config = config

    def get_name(self):
        return 'test'

    def run(self):
        print 'run with ' + self.config
        pass
