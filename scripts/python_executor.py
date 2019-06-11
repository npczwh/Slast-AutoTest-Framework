#!/usr/bin/env python
# _*_ coding: utf-8 _*_

from executor import Executor


class PythonExecutor(Executor):
    def __init__(self, target, path, log):
        super(PythonExecutor, self).__init__(target, path, log)

    def execute(self):
        python = self.path + '/scripts/' + self.target
        cmd = "python %s" % python
        self.msg = 'execute python: %s \n' % python
        if not self.execute_command(cmd):
            return False
        return True
