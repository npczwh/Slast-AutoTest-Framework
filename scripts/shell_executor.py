#!/usr/bin/env python
# _*_ coding: utf-8 _*_

from executor import Executor


class ShellExecutor(Executor):
    def __init__(self, target, path, log):
        super(ShellExecutor, self).__init__(target, path, log)

    def execute(self):
        shell = self.path + '\\scripts\\' + self.target
        cmd = "sh %s" % shell
        self.msg = 'execute shell: %s \n' % shell
        if not self.execute_command(cmd):
            return False
        print self.output
        return True
