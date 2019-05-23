#!/usr/bin/env python
# _*_ coding: utf-8 _*_


class TestFramework(object):
    def __init__(self, config, test_level):
        self.__config = config
        self.__test_level = test_level
        self.__name = 'test'
        self.__msg = ''

    def get_name(self):
        return self.__name

    def get_message(self):
        return self.__msg

    def run(self):
        print 'run with ' + self.__config
        pass
