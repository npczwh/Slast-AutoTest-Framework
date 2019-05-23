#!/usr/bin/env python
# _*_ coding: utf-8 _*_

import ConfigParser
import os
from test_framework import TestFramework
from list_reader import ListReader
from func import *


class TestManager(object):
    def __init__(self, config):
        self.__config = config
        self.__list_file = None
        self.__start_at = None
        self.__tests = []

    def __parse_config(self):
        parser = ConfigParser.SafeConfigParser()
        parser.read(self.__config)
        base_path = file_base_dir(self.__config)
        filename = parser.get('base', 'module_config_list').strip()
        if not os.path.isfile(filename):
            filename = base_path + '\\' + filename
            if not os.path.isfile(filename):
                print 'config list file is not found.'
                return False
        self.__list_file = file_abs_name(filename)
        self.__start_at = parser.get('base', 'start_at').strip()
        return True

    def __prepare_test_list(self, configs):
        for config in configs:
            test = TestFramework(config)
            self.__tests.append(test)
        if self.__start_at:
            print 'start at'
            for test in self.__tests:
                if self.__start_at == test.get_name():
                    break
                else:
                    self.__tests.remove(test)
        if len(self.__tests):
            return True
        else:
            print 'the real test list is empty'
            return False

    def __prepare(self):
        if not self.__parse_config():
            return False
        reader = ListReader()
        if not reader.read(self.__list_file):
            print reader.get_message()
            return False
        configs = reader.get_list()
        if not self.__prepare_test_list(configs):
            return False
        return True

    def __start(self):
        for test in self.__tests:
            test.run()

    def execute(self):
        if not self.__prepare():
            pass
        self.__start()

