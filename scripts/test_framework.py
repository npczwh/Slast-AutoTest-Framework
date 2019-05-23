#!/usr/bin/env python
# _*_ coding: utf-8 _*_


class TestFramework(object):
    def __init__(self, config, test_level):
        self.__config = config
        self.__test_level = test_level

    def get_name(self):
        return 'test'

    def run(self):
        print 'run with ' + self.__config
        pass
