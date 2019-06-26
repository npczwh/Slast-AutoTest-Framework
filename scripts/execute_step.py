#!/usr/bin/env python
# _*_ coding: utf-8 _*_

from handler.api.func import *
from sql_executor import SqlExecutor
from shell_executor import ShellExecutor
from perl_executor import PerlExecutor
from python_executor import PythonExecutor
from handler_executor import HandlerExecutor


class ExecuteStep(object):
    SQL = 1
    SHELL = 2
    PYTHON = 3
    PERL = 4
    HANDLER = 5
    UNSURPPORT = 6

    HANDLER_ONLY = 1
    NO_HANDLER = 2
    ALL = 3

    DEFAULT_COMPARE_HANDLER = 'CompareFileHandler'

    def __init__(self, path, log):
        self.__msg = ''
        self.__name = None
        self.__suite_name = None
        self.__prepare = None
        self.__clear = None
        self.__execute = None
        self.__compare_handler = None
        self.__execute_handler = None
        self.__execute_config = None
        self.__path = path
        self.__log = log
        self.__res = True

    def set_extra_attr(self, context):
        if context.get('prepare', None):
            self.__prepare = context['prepare'].strip()
        if context.get('execute', None):
            self.__execute = context['execute'].strip()
            self.__name = file_short_name(self.__execute)
        else:
            if context.get('name', None):
                self.__name = context['name'].strip()
        if context.get('clear', None):
            self.__clear = context['clear'].strip()
        if context.get('compare_handler', None):
            self.__compare_handler = context['compare_handler'].strip()
        if context.get('execute_handler', None):
            self.__execute_handler = context['execute_handler'].strip()
        if context.get('execute_config', None):
            self.__execute_config = context['execute_config'].strip()
        if context.get('suite_name', None):
            self.__suite_name = context['suite_name'].strip()

    def set_execute_name(self, execute):
        self.__execute = execute
        self.__name = file_short_name(self.__execute)

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
        elif suffix == '' and name:
            return self.HANDLER
        else:
            return self.UNSURPPORT

    def __create_executor(self, name, type):
        executor = None
        suffix = file_suffix(name)
        if type == self.HANDLER_ONLY or type == self.ALL:
            if suffix == '' and name:
                executor = HandlerExecutor(name, self.__path, self.__log)
        if type == self.NO_HANDLER or type == self.ALL:
            if suffix == 'sql':
                executor = SqlExecutor(name, self.__path, self.__log)
            elif suffix == 'sh':
                executor = ShellExecutor(name, self.__path, self.__log)
            elif suffix == 'py':
                executor = PythonExecutor(name, self.__path, self.__log)
            elif suffix == 'pl':
                executor = PerlExecutor(name, self.__path, self.__log)
        if not executor:
            self.__msg = 'fail to create exeutor from name %s' % name
        return executor

    def prepare(self):
        executor = None
        if self.__prepare:
            executor = self.__create_executor(self.__prepare, self.ALL)
        if not executor:
            self.__log.debug('case %s: prepare empty ' % self.__name)
            return True
        self.__log.info('case %s: prepare ' % self.__name)
        if executor.execute():
            return True
        else:
            self.__msg += executor.get_message() + '\n'
            self.__res = False
            return False

    def excute(self):
        executor = None
        if self.__execute:
            executor = self.__create_executor(self.__execute, self.NO_HANDLER)
        elif self.__execute_handler:
            executor = self.__create_executor(self.__execute_handler, self.HANDLER_ONLY)
        else:
            self.__msg = 'execute and execute handler are both empty, fail to create exeutor '
        if not executor:
            self.__res = False
            return False

        if self.__execute_handler:
            if self.__execute_config:
                d = str_to_type(self.__execute_config)
            else:
                d = {}
            d['suite_name'] = self.__suite_name
            d['case_name'] = self.__name
            if d:
                executor.set_context(d)
            else:
                self.__msg = 'transform execute config (%s) to dict fail ' % self.__execute_config
                self.__res = False
                return False

        self.__log.info('case %s: execute ' % self.__name)
        if executor.execute():
            return True
        else:
            self.__msg += executor.get_message() + '\n'
            self.__res = False
            return False

    def clear(self):
        executor = None
        if self.__clear:
            executor = self.__create_executor(self.__clear, self.ALL)
        if not executor:
            self.__log.debug('case %s: clear empty ' % self.__name)
            return True
        self.__log.info('case %s: clear ' % self.__name)
        if executor.execute():
            return True
        else:
            self.__msg += executor.get_message() + '\n'
            self.__res = False
            return False

    def compare(self):
        if self.__compare_handler:
            executor = self.__create_executor(self.__compare_handler, self.HANDLER_ONLY)
        else:
            self.__log.debug('case %s: compare handler empty, use default handler %s '
                             % (self.__name, self.DEFAULT_COMPARE_HANDLER))
            executor = self.__create_executor(self.DEFAULT_COMPARE_HANDLER, self.HANDLER_ONLY)
        if not executor:
            self.__res = False
            return False
        executor.set_context(self.__name)
        self.__log.info('case %s: compare ' % self.__name)
        if executor.execute():
            return True
        else:
            self.__msg += executor.get_message() + '\n'
            self.__res = False
            return False

    def reset(self):
        self.__res = True
        self.__msg = ''

    def get_message(self):
        return self.__msg

    def get_res(self):
        return self.__res
