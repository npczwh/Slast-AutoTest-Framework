#!/usr/bin/env python
# _*_ coding: utf-8 _*_

import subprocess
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
        cmds = "cd %s\n%s" % (self.path, cmd)
        self.output = []
        p = subprocess.Popen(cmds, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        while True:
            line = p.stdout.readline().strip('\n')
            if not line:
                break
            self.output.append(line)
            print line
        p.wait()
        p.stdout.close()
        if p.returncode:
            self.msg += 'fail to execute command: %s \n' % cmd
            self.msg += 'return code: %d \n' % p.returncode
            return False
        return True

    def get_target(self):
        return self.target

    def get_message(self):
        return self.msg
