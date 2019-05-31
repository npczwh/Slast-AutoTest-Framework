#!/usr/bin/env python
# _*_ coding: utf-8 _*_

from executor import Executor
from func import *


class SqlExecutor(Executor):
    def __init__(self, target, path, log):
        super(SqlExecutor, self).__init__(target, path, log)
        self.__host = '127.0.0.1'
        self.__user = 'gbase'
        self.__pwd = 'gbase20110531'

    def execute(self):
        sql = self.path + '\\case\\' + self.target
        cmd = "gccli -h%s -u%s -p%s -c -t -vv -f <%s" % (self.__host, self.__user, self.__pwd, sql)
        self.msg = 'execute sql: %s \n' % sql
        output = None
        if not self.execute_command(cmd, output):
            return False
        result_file = self.path + '\\case\\' + file_base_name(self.target) + '.result'
        write_file(result_file, 'w', output)
        return True
