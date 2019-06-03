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

    DEFAULT_COMPARE_HANDLER = 'CompareFileHandler'

    def __init__(self, path, log):
        self.__msg = ''
        self.__name = None
        self.__prepare = None
        self.__clear = None
        self.__execute = None
        self.__compare_handler = None
        self.__execute_handler = None
        self.__execute_config = None
        # todo: handle parallel write
        self.__path = path
        self.__log = log

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
        elif suffix == '' and name:
            executor = HandlerExecutor(name, self.__path, self.__log)
        if not executor:
            self.__msg = 'fail to create exeutor from name %s' % name
        return executor

    def prepare(self):
        executor = None
        if self.__prepare:
            executor = self.__create_executor(self.__prepare)
        if not executor:
            self.__log.debug('case %s: prepare empty ' % self.__name)
            return True
        self.__log.info('case %s: prepare ' % self.__name)
        if executor.execute():
            return True
        else:
            self.__msg += executor.get_message() + '\n'
            return False

    def excute(self):
        executor = None
        if self.__execute:
            executor = self.__create_executor(self.__execute)
        elif self.__execute_handler:
            executor = self.__create_executor(self.__execute_handler)
        else:
            self.__msg = 'execute and execute handler are both empty, fail to create exeutor '
        if not executor:
            return False

        if self.__execute_handler and self.__execute_config:
            d = str_to_dict(self.__execute_config)
            if d:
                executor.set_context(d)
            else:
                self.__msg = 'transform execute config (%s) to dict fail ' % self.__execute_config
                return False

        self.__log.info('case %s: execute ' % self.__name)
        if executor.execute():
            return True
        else:
            self.__msg += executor.get_message() + '\n'
            return False

    def clear(self):
        executor = None
        if self.__clear:
            executor = self.__create_executor(self.__clear)
        if not executor:
            self.__log.debug('case %s: clear empty ' % self.__name)
            return True
        self.__log.info('case %s: clear ' % self.__name)
        if executor.execute():
            return True
        else:
            self.__msg += executor.get_message() + '\n'
            return False

    def compare(self):
        if self.__compare_handler:
            executor = self.__create_executor(self.__compare_handler)
        else:
            self.__log.debug('case %s: compare handler empty, use default handler %s '
                             % (self.__name, self.DEFAULT_COMPARE_HANDLER))
            executor = self.__create_executor(self.DEFAULT_COMPARE_HANDLER)
        if not executor:
            return False
        executor.set_context(self.__name)
        self.__log.info('case %s: compare ' % self.__name)
        if executor.execute():
            return True
        else:
            self.__msg += executor.get_message() + '\n'
            return False

    def reset(self):
        self.__msg = ''

    def get_message(self):
        return self.__msg
