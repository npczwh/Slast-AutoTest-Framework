#!/usr/bin/env python
# _*_ coding: utf-8 _*_

import ConfigParser
from test_framework import TestFramework
from list_reader import ListReader
from func import *


class TestManager(object):
    def __init__(self, config):
        self.__config = config
        self.__list_file = None
        self.__start_at = None
        self.__test_level = None
        self.__suites = []
        self.__msg = ''

    def __parse_config(self):
        parser = ConfigParser.SafeConfigParser()
        parser.read(self.__config)
        base_path = file_base_dir(self.__config)
        filename = parser.get('base', 'suite_list').strip()
        if not os.path.isfile(filename):
            filename = base_path + '\\' + filename
            if not os.path.isfile(filename):
                self.__msg = 'suite list file is not found.'
                return False
        self.__list_file = file_abs_name(filename)
        self.__start_at = parser.get('base', 'start_at').strip()
        self.__test_level = parser.get('base', 'test_level').strip()
        return True

    def __prepare_test_list(self, suite_list):
        for suite_path in suite_list:
            suite = TestFramework(suite_path, self.__test_level)
            self.__suites.append(suite)
        if self.__start_at:
            for suite in self.__suites:
                if self.__start_at == suite.get_name():
                    break
                else:
                    self.__suites.remove(suite)
        if len(self.__suites):
            return True
        else:
            self.__msg = 'the real suite list is empty'
            return False

    def __prepare(self):
        if not self.__parse_config():
            return False
        reader = ListReader()
        if not reader.read(self.__list_file, False):
            self.__msg = reader.get_message()
            return False
        suite_list = reader.get_list()
        if not self.__prepare_test_list(suite_list):
            return False
        return True

    def __start(self):
        ret = True
        for suite in self.__suites:
            if not suite.run():
                self.__msg += '\n\tRun suite %s error: %s' % (suite.get_name(), suite.get_message())
                ret = False
        return ret

    def execute(self):
        if not self.__prepare():
            return False
        return self.__start()

    def get_message(self):
        return self.__msg
