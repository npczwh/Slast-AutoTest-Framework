#!/usr/bin/env python
# _*_ coding: utf-8 _*_


class TestFramework(object):
    def __init__(self, config):
        self.config = config

    def run(self):
        print 'run with ' + self.config
        pass
