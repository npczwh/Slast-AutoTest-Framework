#!/usr/bin/env python
# _*_ coding: utf-8 _*_


class TestExecutor(object):
    def __init__(self, name, execute, path, log):
        self.__msg = ''
        self.__name = name
        self.__prepare = None
        self.__execute = execute
        self.__clear = None
        self.__compare = None
        # todo: handle parallel write
        self.__path = path
        self.__log = log

    def set_extra_attr(self, prepare, clear, compare):
        self.__prepare = prepare
        self.__clear = clear
        self.__compare = compare

    def get_name(self):
        return self.__name

    def prepare(self):
        if self.__prepare:
            print 'prepare: %s' % self.__prepare
        return True

    def excute(self):
        if self.__execute:
            print 'execute: %s' % self.__execute
        return True

    def clear(self):
        if self.__clear:
            print 'clear: %s' % self.__clear
        return True

    def compare(self):
        if self.__compare:
            print 'compare: %s' % self.__compare
        return True

    def get_message(self):
        return self.__msg
