#!/usr/bin/env python
# _*_ coding: utf-8 _*_

import sys
import subprocess
from handler import Handler
sys.path.append('..')
from func import *


class SqlExecutor(object):
    def __init__(self, sql, result_file, host='127.0.0.1', user='gbase', pwd='gbase20110531', db='gclusterdb'):
        self.__sql = sql
        self.__result_file = result_file
        self.__host = host
        self.__user = user
        self.__pwd = pwd
        self.__db = db
        self.__output = ''
        self.__msg = ''

    def execute(self):
        cmd = "gccli -h%s -u%s -p%s -D%s -c -t -vv -f <%s" % (self.__host, self.__user, self.__pwd, self.__db, self.__sql)
        if not self.__execute_command(cmd):
            return False
        write_file(self.__result_file, 'w', self.__output)
        return True

    def __execute_command(self, cmd):
        lines = []
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        while True:
            line = p.stdout.readline()
            if not line:
                break
            lines.append(line)
        while True:
            line = p.stderr.readline()
            if not line:
                break
            lines.append(line)
        p.wait()
        p.stdout.close()
        p.stderr.close()
        self.__output = ''
        for l in lines:
            self.__output += l
        if p.returncode:
            self.__msg += 'fail to execute command: %s \n' % cmd
            self.__msg += 'return code: %d \n' % p.returncode
            return False
        return True

    def get_message(self):
        return self.__msg


class SqlHandler(Handler):
    def __init__(self, path, log):
        super(SqlHandler, self).__init__(path, log)
        self.__host = '127.0.0.1'
        self.__user = 'gbase'
        self.__pwd = 'gbase20110531'
        self.__db = 'gclusterdb'

    def execute(self):
        if not self.context.get('case_name', None):
            self.msg = 'SqlHandler execute failed: case name not found'
            return True
        if self.context.get('sql', None):
            sql = self.path + '/case/' + self.context['sql'].strip()
        else:
            sql = self.path + '/case/' + self.context['case_name'].strip() + '.sql'
        result_file = self.path + '/result/' + self.context['case_name'].strip() + '.result'
        executor = SqlExecutor(sql, result_file, self.__host, self.__user, self.__pwd, self.__db)
        if executor.execute():
            return True
        else:
            self.msg = executor.get_message()
            return False
