#!/usr/bin/env python
# _*_ coding: utf-8 _*_

import ConfigParser
import os
from test_framework import TestFramework
from func import *


class TestManager(object):
    def __init__(self, config):
        self.__config = config
        self.__list_file = None
        self.__start_at = None
        self.__tests = None

    def __parse_config(self):
        parser = ConfigParser.SafeConfigParser()
        parser.read(self.__config)
        base_path = file_base_dir(self.__config)
        filename = parser.get('base', 'config_list_file').strip()
        if not os.path.isfile(filename):
            filename = base_path + '\\' + filename
            if not os.path.isfile(filename):
                print 'config list file not found.'
                return False
        self.__list_file = filename
        self.__start_at = parser.get('base', 'start_at').strip()
        return True

    def __prepare_test_list(self):
        if not self.__parse_config():
            return False
        if self.__start_at is None:
            print 'start at none'
        else:
            print self.__start_at
        return True

    def execute(self):
        if not self.__prepare_test_list():
            pass

