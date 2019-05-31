#!/usr/bin/env python
# _*_ coding: utf-8 _*_

import commands
from abc import ABCMeta, abstractmethod


class Executor(object):
    __metaclass__ = ABCMeta

    def __init__(self, target, path, log):
        self.target = target
        self.path = path
        self.log = log
        self.__handler = None
        self.msg = ''

    @abstractmethod
    def execute(self):
        pass

    def execute_command(self, cmd, output):
        (status, output) = commands.getstatusoutput(cmd)
        if status != 0:
            self.msg += '%s \n' % status
            self.msg += '%s \n' % output
            self.msg += 'fail to execute command: %s ' % cmd
            return False
        return True

    def get_message(self):
        return self.msg