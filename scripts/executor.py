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

    def execute_command(self, cmd, is_sql=False):
        cmds = "cd %s\n%s" % (self.path, cmd)
        lines = []
        p = subprocess.Popen(cmds, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        while True:
            line = p.stdout.readline()
            if not line:
                break
            if not is_sql:
                print line.strip('\n')
            lines.append(line)
        while True:
            line = p.stderr.readline()
            if not line:
                break
            if not is_sql:
                print line.strip('\n')
            lines.append(line)
        p.wait()
        p.stdout.close()
        p.stderr.close()
        self.output = ''
        for l in lines:
            self.output += l
        if p.returncode:
            self.msg += 'fail to execute command: %s \n' % cmd
            self.msg += 'return code: %d \n' % p.returncode
            return False
        return True

    def get_target(self):
        return self.target

    def get_message(self):
        return self.msg
