#!/usr/bin/env python
# _*_ coding: utf-8 _*_

import commands
from abc import ABCMeta, abstractmethod


class Executor(object):
    __metaclass__ = ABCMeta

    def __init__(self, target, path, log):
        self.target = target
        self.path = path
        self.context = None
        self.output = None
        self.log = log
        self.msg = ''

    @abstractmethod
    def execute(self):
        pass

    def set_context(self, context):
        self.context = context

    def execute_command(self, cmd):
        (status, self.output) = commands.getstatusoutput(cmd)
        if status != 0:
            self.msg += 'fail to execute command: %s \n' % cmd
            self.msg += 'status: %s \n' % status
            self.msg += 'output: %s \n' % self.output
            return False
        return True

    def get_target(self):
        return self.target

    def get_message(self):
        return self.msg
