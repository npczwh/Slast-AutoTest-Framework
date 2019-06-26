#!/usr/bin/env python
# _*_ coding: utf-8 _*_

from execute_step import ExecuteStep
from handler.api.list_reader import ListReader


class ExecuteStepFactory(object):
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

    def create_step(self, context, type):
        step = None
        if type == 'xml':
            step = ExecuteStep(self.__path, self.__log)
            step.set_extra_attr(context)
        else:
            step = ExecuteStep(self.__path, self.__log)
            step.set_execute_name(context)
            if step.get_type(step.get_name()) != step.SQL:
                return None
        if not step.get_name():
            return None
        if not self.__filter(step.get_name()):
            return None
        return step

    def get_message(self):
        return self.__msg
