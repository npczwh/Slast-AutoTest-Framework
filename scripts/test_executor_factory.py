#!/usr/bin/env python
# _*_ coding: utf-8 _*_

from test_executor import TestExecutor
from list_reader import ListReader
from func import *


class TestExecutorFactory(object):
    def __init__(self):
        self.__whitelist = None
        self.__blacklist = None
        self.__begin_at = None
        self.__path = None
        self.__log = None
        self.__msg = ''

    def init(self, whitelist_name, blacklist_name, begin_at, path, log):
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
        self.__path = path
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

    def create_executor(self, name, context, type):
        executor = None
        if not self.__filter(name):
            return None
        if type == 'xml':
            executor = TestExecutor(name, context['execute'], self.__path, self.__log)
            executor.set_extra_attr(context['prepare'], context['clear'], context['compare'])
        else:
            executor = TestExecutor(name, context, self.__path, self.__log)
            if executor.get_type(name) != executor.SQL:
                executor = None
        return executor

    def get_message(self):
        return self.__msg