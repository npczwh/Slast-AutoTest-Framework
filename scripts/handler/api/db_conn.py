#!/usr/bin/env python
#coding:utf-8

from GBaseConnector import *


class DBError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return str(self.value)

class DBConn():
    def __init__(self, host, user, pwd, db = "", port = 5258):
        self.connStr = {'host':host,'port':port, 'user':user,'passwd':pwd, 'database':db, 'connection_timeout':9999999, 'charset':'utf8'}
        self.conn = GBaseConnection(**self.connStr)
        self.cursor = self.conn.cursor()

    def __del__(self):
        self.cursor.close()
        self.conn.close()

    def ExecSQL(self, sql):
        # success:  return (affectRow, warings)
        # false  :  throw Exception(errmsg)
        try:
            res = self.conn.query(sql)
            '''
            {'info_msg': 'Records: 1638400  Duplicates: 0  Warnings: 0', 'insert_id': 0, 'field_count': 0, 'warning_count': 0, 'server_status': 3, 'affected_rows': 1638400}
            '''
            return (res['affected_rows'], res['warning_count'])
        except GBaseError, err:
            raise DBError("GBaseError: " + str(err))
        except Exception, err:
            raise DBError(str(err))

    def ExecMany(self, sqlMode, data):
        try:
            res = self.cursor.executemany(sqlMode, data)
            return self.cursor.rowcount
        except GBaseError, err:
            raise DBError("GBaseError: " + str(err))
        except Exception, err:
            raise DBError(str(err))

    def GetTableRowCount(self, table, db = "", condition = None):
        try:
            if db == "":
                db = self.conn.get_database()
            cmd = "SELECT COUNT(*) FROM `%s`.`%s`" % (db, table)
            if condition is not None:
                cmd += " WHERE %s" % condition
            self.cursor.execute(cmd)
            return self.cursor.fetchone()[0]
        except GBaseError, err:
            raise DBError("GBaseError: " + str(err))
        except Exception, err:
            raise DBError(str(err))

    def GetTableColSum(self, table, col, db = ""):
        if db == "":
            db = self.conn.get_database()
        cmd = "select sum(%s) from %s.%s" % (col, db, table)
        self.cursor.execute(cmd)
        return self.cursor.fetchone()[0]

    def GetTableColsSum(self, table, cols, db = ""):
        rvSum = {}
        for col in cols:
            rvSum[col] = self.GetTableColSum(table, col, db)
        return rvSum

    def GetTableCols(self, table, db = ""):
        if db == "":
            db = self.conn.get_database()
        cmd = "desc %s.%s" % (db, table)
        self.cursor.execute(cmd)
        res = self.cursor.fetchall()
        colsName = []
        for r in res:
            colsName.append(r[0])
        return colsName

    def GetSQLRes(self, sql):
        try:
            self.cursor.execute(sql)
            if self.cursor._have_unread_result():
                return self.cursor.fetchall()
            else:
                return None
        except GBaseError, err:
            raise DBError("GBaseError: " + str(err))
        except Exception, err:
            raise DBError(str(err))

############################################## COMMON FUNCTIONS ###########################################################

if __name__ =='__main__':
    pass
