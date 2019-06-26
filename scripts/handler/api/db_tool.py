#!/usr/bin/env python
# _*_ coding: utf-8 _*_

from db_conn import *


class DBTool(object):
    def __init__(self, host='127.0.0.1', user='gbase', pwd='gbase20110531', log=None):
        self.__host = host
        self.__user = user
        self.__pwd = pwd
        self.__log = log
        self.__msg = ''

    def clear_instance(self):
        conn = DBConn(self.__host, self.__user, self.__pwd)
        try:
            sql = 'show databases'
            res = conn.GetSQLRes(sql)
            for row in res:
                if self.is_sys_db(row[0]):
                    continue
                sql = 'drop database %s' % row[0]
                conn.ExecSQL(sql)
            sql = "select tbName from gbase.table_distribution where dbName='gclusterdb' and tbName not in " \
                  "('dual', 'rebalancing_status', 'nodedatamap')"
            res = conn.GetSQLRes(sql)
            for row in res:
                sql = 'drop table gclusterdb.%s' % row[0]
                conn.ExecSQL(sql)
        except Exception as e:
            self.__msg = str(e)
            return False
        return True

    @staticmethod
    def is_sys_db(name):
        if name == 'information_schema' or name == 'performance_schema' or name == 'gbase' \
                or name == 'gclusterdb' or name == 'gctmpdb':
            return True
        else:
            return False
