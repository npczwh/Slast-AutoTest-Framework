#!/usr/bin/env python
# _*_ coding: utf-8 _*_

from executor import Executor


class SqlExecutor(Executor):
    def __init__(self, target, path, log):
        super(SqlExecutor, self).__init__(target, path, log)

    def execute(self):
        print 'execute sql: %s' % self.target
        return True
