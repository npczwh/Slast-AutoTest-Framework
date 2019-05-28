#!/usr/bin/env python
# _*_ coding: utf-8 _*_

from test_executor import TestExecutor
from list_reader import ListReader


class TestExecutorFactory(object):
    def __init__(self):
        self.__whitelist = None
        self.__blacklist = None
        self.__begin_at = None
        self.__log = None
        self.__msg = ''

    def init(self, whitelist_name, blacklist_name, begin_at, log):
        reader = ListReader()
        if whitelist_name:
            if not reader.readitem(whitelist_name):
                self.__msg = reader.get_message()
                return False
            self.__whitelist = reader.get_list()
        if blacklist_name:
            if not reader.readitem(blacklist_name):
                self.__msg = reader.get_message()
                return False
            self.__blacklist = reader.get_list()
        self.__begin_at = begin_at
        self.__log = log
        return True

    def __filter(self, name):
        if self.__begin_at:
            if self.__begin_at == name:
                self.__begin_at = ''
            else:
                return False
        if self.__whitelist and name not in self.__whitelist:
            return False
        if self.__blacklist and name in self.__blacklist:
            return False
        return True

    def create_executor(self, name, type, context):
        if not self.__filter(name):
            return None

    def get_message(self):
        return self.__msg