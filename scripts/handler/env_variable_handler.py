#!/usr/bin/env python
# _*_ coding: utf-8 _*_

from handler import Handler
from db_conn import *


# todo: use db_conn to check set variables result
class EnvVariableHandler(Handler):
    def __init__(self, path, log):
        super(EnvVariableHandler, self).__init__(path, log)
        self.__back_up = False
        self.__host = '127.0.0.1'
        self.__use = 'gbase'
        self.__pwd = 'gbase20110531'
        self.__config = {}
        self.__bak_config = {}

    def __parse_context(self):
        self.__config = {}
        self.__bak_config = {}
        for context in self.context:
            attr = context['attr']
            type = attr.get('type', None)
            var = attr.get('var', None)
            if not type or not var:
                self.msg = 'EnvVariableHandler execute failed: type/var not found '
                return False
            d = self.__config.get(type, None)
            if d:
                d[var] = context['value']
            else:
                d = {var:context['value']}
                self.__config[type] = d
        return True

    def __get_conn(self, type):
        connect = None
        if type == 'gcluster':
            connect = DBConn(self.__host, self.__use, self.__pwd)
        elif type == 'gbase':
            connect = DBConn(self.__host, self.__use, self.__pwd, port = 5050)
        return connect

    def __restore_vars(self, conn, type, var):
        sql = "show variables like '%s'" % var
        res = conn.GetSQLRes(sql)
        if not len(res):
            self.msg = '%s variable %s not exists ' % (type, var)
            return False
        value = res[0][1]
        d = self.__bak_config.get(type, None)
        if d:
            d[var] = value
        else:
            d = {var:value}
            self.__config[type] = d
        return True

    def __set_vars(self, conn, var, value):
        sql = "set global %s = %s" % (var, value)
        try:
            conn.ExecSQL(sql)
        except Exception as e:
            self.msg = str(e)
            return False
        return True

    def __action(self, config, is_execute):
        for type in config.keys():
            conn = self.__get_conn(type)
            vars = config.get(type)
            for var in vars.keys():
                if is_execute:
                    if not self.__restore_vars(conn, type, var):
                        return False
                if not self.__set_vars(conn, var, vars.get(var)):
                    return False
        return True

    def execute(self):
        self.log.debug('EnvVariableHandler execute')
        if not self.__parse_context():
            return False
        self.__back_up = True
        if not self.__action(self.__config, True):
            return False
        return True

    def clear(self):
        self.log.debug('EnvVariableHandler clear')
        if not self.__back_up:
            self.msg = 'EnvVariableHandler clear failed: no back up for clear '
            return False
        if not self.__action(self.__bak_config, False):
            return False
        return True
