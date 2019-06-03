#!/usr/bin/env python
# _*_ coding: utf-8 _*_

from func import *
from sql_executor import SqlExecutor
from shell_executor import ShellExecutor
from perl_executor import PerlExecutor
from handler_executor import HandlerExecutor


class ExecuteStep(object):
    SQL = 1
    SHELL = 2
    PYTHON = 3
    PERL = 4
    HANDLER = 5
    UNSURPPORT = 6

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

    def get_type(self, name):
        suffix = file_suffix(name)
        if suffix == 'sql':
            return self.SQL
        elif suffix == 'sh':
            return self.SHELL
        elif suffix == 'py':
            return self.PYTHON
        elif suffix == 'pl':
            return self.PERL
        elif suffix == '':
            return self.HANDLER
        else:
            return self.UNSURPPORT

    def __create_executor(self, name):
        executor = None
        suffix = file_suffix(name)
        if suffix == 'sql':
            executor = SqlExecutor(name, self.__path, self.__log)
        elif suffix == 'sh':
            executor = ShellExecutor(name, self.__path, self.__log)
        elif suffix == 'py':
            pass
        elif suffix == 'pl':
            executor = PerlExecutor(name, self.__path, self.__log)
        elif suffix == '':
            executor = HandlerExecutor(name, self.__path, self.__log)
        if not executor:
            self.__msg = 'fail to create exeutor from name %s' % name
        return executor

    def prepare(self):
        if self.__prepare:
            print 'prepare: %s' % self.__prepare
        return True

    def excute(self):
        if self.__execute:
            executor = self.__create_executor(self.__execute)
            if not executor:
                return False
            if executor.execute():
                return True
            else:
                self.__msg += executor.get_message() + '\n'
                return False
        else:
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
