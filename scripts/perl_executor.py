#!/usr/bin/env python
# _*_ coding: utf-8 _*_

from executor import Executor


class PerlExecutor(Executor):
    def __init__(self, target, path, log):
        super(PerlExecutor, self).__init__(target, path, log)

    def execute(self):
        perl = self.path + '/scripts/' + self.target
        cmd = "perl %s" % perl
        self.msg = 'execute perl: %s \n' % perl
        if not self.execute_command(cmd):
            return False
        return True
