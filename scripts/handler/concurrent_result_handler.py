#!/usr/bin/env python
# _*_ coding: utf-8 _*_

import sys
import commands
import os
from multiprocessing import Process
from multiprocessing import Manager
from handler import Handler
sys.path.append('..')
from func import *


class Worker(Process):
    def __init__(self, name, host, user, pwd, path, sql, id, res):
        super(Worker, self).__init__()
        self.__name = name
        self.__host = host
        self.__user = user
        self.__pwd = pwd
        self.__path = path
        self.__sql = sql
        self.__id = id
        self.__msg = ''
        self.__res = res
        self.__res.append(False)
        self.__res.append(self.__msg)
        self.__output = ''

    def run(self):
        cmd = "gccli -h%s -u%s -p%s -c -t -vv -f <%s" % (self.__host, self.__user, self.__pwd, self.__sql)
        if not self.__execute_command(cmd):
            self.__res[1] = self.__msg
            return
        result_file = self.__path + '/result/' + self.__name + '_' + str(self.__id) + '.result'
        write_file(result_file, 'w', self.__output)
        self.__res[0] = True

    def get_sql(self):
        return self.__sql

    def __execute_command(self, cmd):
        (status, self.__output) = commands.getstatusoutput(cmd)
        if status != 0:
            self.__msg += 'fail to execute command: %s \n' % cmd
            self.__msg += 'status: %s \n' % status
            self.__msg += 'output: %s \n' % self.__output
            return False
        return True


class ConcurrentHandler(Handler):
    TIMEOUT = 5000

    def __init__(self, path, log):
        super(ConcurrentHandler, self).__init__(path, log)
        self.__host = '127.0.0.1'
        self.__user = 'gbase'
        self.__pwd = 'gbase20110531'
        self.__res_list = []
        self.__workers = []
        self.__name = None
        self.__has_final = False

    def __create_workers(self):
        self.__name = self.context.get('name', None)
        sqls = self.context.get('concurrent', None)
        if not self.__name or not sqls:
            self.msg = 'ConcurrentHandler execute fail: name or concurrent is not found '
            return False
        sql_list = sqls.split(',')
        m = Manager()
        for i in range(len(sql_list)):
            l = m.list()
            self.__res_list.append(l)
            sql_list[i] = self.path + '/case/' + sql_list[i].strip()
            w = Worker(self.__name, self.__host, self.__user, self.__pwd, self.path, sql_list[i], i, l)
            self.__workers.append(w)
        return True

    def __dispatch(self, workers):
        res = True
        for w in workers:
            w.start()
        timeout = self.context.get('timeout', None)
        if not timeout:
            timeout = ConcurrentHandler.TIMEOUT
        timeout = float(timeout) / 1000.0
        for w in workers:
            w.join(timeout)
        for i in range(len(workers)):
            if workers[i].is_alive():
                workers[i].terminate()
                print i
                self.msg += 'ConcurrentHandler execute fail: sql(%s) can not finish in %d ms \n' \
                            % (workers[i].get_sql(), int(timeout * 1000))
                res = False
        return res

    @property
    def __execute_final(self):
        if not self.__has_final:
            return True
        m = Manager()
        l = m.list()
        self.__res_list.append(l)
        sql_name = self.path + '/case/' + self.context.get('final', None)
        i = len(self.__workers)
        w = Worker(self.__name, self.__host, self.__user, self.__pwd, self.path, sql_name, i, l)
        self.__workers.append(w)
        workers = [w]
        if self.__dispatch(workers):
            return True
        else:
            return False

    def __compose_result(self):
        buf = ''
        for i in range(len(self.__workers)):
            res_name = self.path + '/result/' + self.__name + '_' + str(i) + '.result'
            lines = read_lines(res_name)
            if self.__has_final and i == len(self.__workers) - 1:
                head = 'concurrent'
            else:
                head = 'final'
            buf += '--------   %s: %s   --------\n' % (head, self.__workers[i].get_sql())
            for l in lines:
                buf += l
            os.remove(res_name)
        name = self.path + '/result/' + self.__name + '.result'
        write_file(name, 'w', buf)

    def __finish(self):
        r = True
        for res in self.__res_list:
            if not res[0]:
                self.msg += res[1]
                r = False
        if not r:
            return False
        self.__compose_result()
        return True

    def execute(self):
        self.log.debug('ConcurrentHandler execute')
        if self.context.get('final', None):
            self.__has_final = True
        if not self.__create_workers():
            return False
        if not self.__dispatch(self.__workers):
            return False
        if not self.__execute_final:
            return False
        if not self.__finish():
            return False
        return True
