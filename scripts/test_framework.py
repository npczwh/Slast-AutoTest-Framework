#!/usr/bin/env python
# _*_ coding: utf-8 _*_

from func import *


class TestFramework(object):
    def __init__(self, path, test_level):
        self.__name = file_short_name(path)
        self.__config = path + '\\conf\\' + self.__name + '.conf'
        self.__test_level = test_level
        self.__msg = ''

    def get_name(self):
        return self.__name

    def get_message(self):
        return self.__msg

    def run(self):
        print 'run with ' + self.__config
        return False
