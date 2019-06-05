import weakref
import re
import itertools
import sys

global GBASELOGGER
from GBaseConnector import GBaseError
from GBaseConnector.GBaseUtils import GBaseConvert  
from GBaseConnector.GBaseConstants import (CharacterSet)

class GBaseCursor(object):
    '''
    ************************************************************
        Module      : GBaseCursor
        Function    : Create cursor object
        Corporation : General Data Technology CO,. Ltd.
        Team        : DMD, Interface Team
        Author      : wq
        Date        : 2013-7
        Version     : v1.0
        Description : Create cursor to operation GBase server. 
                      And execute SQL and fetch rows.
    ************************************************************
    '''
    def __init__(self, connection):
        '''
        ************************************************************
            Function    : __init__
            Arguments   : 1) connection (GBaseConnection) : connection object
            Return      : 
            Description : GBaseCursor construct function
        ************************************************************
        '''
        self._connection = None
        self._description = None
        self._rowcount = -1
        self.arraysize = 1
        self._stored_results = []
        self._nextrow = (None, None)
        self._warnings = None
        self._warning_count = 0
        self._executed = None
        self._executed_list = []
        self.convert = None

        self._split_rule = None
        self._sql_insert_values_rule = re.compile(
                r'VALUES\s*(\(\s*(?:%(?:\(.*\)|)s\s*(?:,|)\s*)+\))',
                re.I | re.M)
        self._sql_comment_rule = re.compile("\/\*.*\*\/")
        self._sql_insert_stmt_rule = re.compile(r'INSERT\s+INTO', re.I)
        self._exec_iterator = None
        
        self._call_list = None
        
        if connection is not None:
            try:
                self._connection = weakref.proxy(connection)
                self._setconvert()
                # self._connection._protocol
            except (AttributeError, TypeError):
                raise GBaseError.InterfaceError(errno=2048)

    @property
    def description(self):
        return self._description
    
    @property
    def rowcount(self):
        return self._rowcount
    
    def save_param_val(self):
        '''
            Function    : _save_param_val
            Return      : 
            Descripton  : if operation is a call statement and
                          return an out parameter. 
        '''
        if self._call_list is None:
            raise GBaseError.OperationalError("no call parameter value to save.")
        res = []
        for arg in self._call_list:
            getquery = "SELECT %s" % arg
            self.execute(getquery)
            res.append(self.fetchone()[0])
        return tuple(res)
    
    def callproc(self, procname, params=()):
        '''
        ************************************************************
            Function    : callproc
            Arguments   : 1) procname (string) : procedure name
                          2) params (list) : procedure arguments
            Return      : 
            Description : Call procedure
        ************************************************************
        '''        
        if not procname:
            GBASELOGGER.error("procname can not None.")
            raise GBaseError.InterfaceError("procname can not None.")
        
        if self._connection.unread_result:
            GBASELOGGER.error("Have unread result found.")
            raise GBaseError.InternalError("Have unread result found.")
        
        argfmt = "@_%s_arg%d_%d"
        call_list = []
        argnames = self._handle_parameters(params)
        for idx, arg in enumerate(argnames):
            argname = argfmt % (procname, idx + 1, id(argnames[idx]))
            call_list.append(argname)
            setquery = "SET %s=%%s" % argname
            self.execute(setquery, (arg,))
        self._call_list = call_list
        call = "CALL %s(%s)" % (procname, ','.join(call_list))
        self._executed = call
        call_res = self._connection.query_iterator(call)
        for res in call_res:
            self._handle_result(res)
            if self._connection.unread_result:
                yield self
    
    def close(self):
        '''
        ************************************************************
            Function    : close
            Arguments   : 
            Return      : 
            Description : close cursor object and release resources.
        ************************************************************
        '''
        if self._connection is None:
            return False
        
        self._reset_result()
        self._connection = None
        
        return True
    
    def execute(self, operation, params=None, multi_stmt=False):
        '''
        ************************************************************
            Function    : execute
            Arguments   : 1) operation (string) : sql 
                          2) params (dic) : parameter dictionary
                          3) multi_stmt : multiple sql switch
            Return      : if multi_stmt is True then function return iterator
            Description : execute DML/DDL
        ************************************************************
        '''
        if not operation:
            GBASELOGGER.error("operation can not None.")
            raise GBaseError.InterfaceError("operation can not None.")
        
        if self._connection.unread_result:
            GBASELOGGER.error("Have unread result found.")
            raise GBaseError.InternalError("Have unread result found.")
        
        self._reset_result()
        statement = ''
        
        try:
            if isinstance(operation, unicode):
                GBASELOGGER.debug("use unicode.")
                operation = operation.encode(self._connection.charset)
        except (UnicodeDecodeError, UnicodeEncodeError), e:
            GBASELOGGER.error(e)
            raise GBaseError.ProgrammingError(e)
        
        if params is not None:
            try:
                statement = operation % self._handle_parameters(params)
            except TypeError:
                GBASELOGGER.error(
                    "Wrong number of arguments during string formatting")
                raise GBaseError.ProgrammingError(
                    "Wrong number of arguments during string formatting")
        else:
            statement = operation
        
        GBASELOGGER.debug(statement)
        GBASELOGGER.sql(statement)
        
        # handle multiple statement
        if multi_stmt:
            GBASELOGGER.debug("Start execute iterator query. '%s'" % statement)
            self._executed = statement
            self._executed_list = []
            self._exec_iterator = self._execute_iterator(self._connection.query_iterator(statement)) 
            return self._exec_iterator 
        else:
            self._executed = statement
            try:
                GBASELOGGER.debug("Start execute query. '%s'" % statement)
                result = self._connection.query(statement)
                GBASELOGGER.debug("Start handle result. '%s'" % result)
                self._handle_result(result)
            except GBaseError.InterfaceError, err:
                if self._connection._have_next_result:
                    GBASELOGGER.error("Use multiSql=True when executing multiple statements")
                    raise GBaseError.InterfaceError(
                        "Use multiSql=True when executing multiple statements")
                raise
            return None
    
    def executemany(self, operation, seq_of_params):
        """
        ************************************************************
            Execute the given operation multiple times
            
            The executemany() method will execute the operation iterating
            over the list of parameters in seq_params.
            
            Example: Inserting 3 datas
            
            datas = [
                (1,'test data 1'),
                (2,'test data 2'),
                (3,'test data 3')
                ]
            sql = "INSERT INTO datas(id,name) VALUES (%d,'%s')"
            cursor.executemany(sql, datas)
            
            INSERT statements are optimized by batching the datas, that is
            using the GBase multiple rows syntax.
            
            Results are discarded. If they are needed, consider looping over
            data using the execute() method.
        ************************************************************
        """
        if not operation:
            return
        if self._have_unread_result():
            raise GBaseError.InternalError("Unread result found.")
        elif len(self._split_sql(operation)) > 1:
            raise GBaseError.InternalError(
                "executemany() does not support multiple statements")
        
        # Optimize INSERTs by batching them
        if self._match_insert_stmt(operation):
            fmt = self._get_sql_params(operation)
            values = []
            for params in seq_of_params:
                values.append(fmt % self._handle_parameters(params))
            operation = operation.replace(fmt, ','.join(values), 1)
            return self.execute(operation)
            
        try:
            rowcount = 0
            for params in seq_of_params:
                self.execute(operation, params)
                if self._rowcount and self._have_unread_result():
                    self.fetchall()
                rowcount += self._rowcount
        except (ValueError, TypeError), err:
            raise GBaseError.InterfaceError(
                "Failed executing the operation; %s" % err)
        except:
            # Raise whatever execute() raises
            raise
        self._rowcount = rowcount
    
    def fetchone(self):
        '''
        ************************************************************
            Function    : fetchone
            Arguments   : 
            Return      : row (dict)
            Description : Fetch one row. The function execute after self.execute(). 
        ************************************************************
        '''
        row = self._fetchrow()
        if row:
            return self._row_to_python(row)
        return None
    
    def fetchmany(self, size=1):
        '''
        ************************************************************
            Function    : fetchmany
            Arguments   : 1) size (int) : fetch row size
            Return      : rows (list)
            Description : Fetch more rows. The function execute after self.execute().
        ************************************************************
        '''
        GBASELOGGER.debug("Start fetch all data.")
        if not self._have_unread_result():
            GBASELOGGER.error("No resultset.")
            raise GBaseError.InterfaceError("No resultset.")
        
        if not isinstance(size, int) or size < 1:
            GBASELOGGER.error("Size '%s' is not valid." % size)
            raise GBaseError.ProgrammingError("Size '%s' is not valid." % size)
        
        results = []
        count = (size or self.arraysize)
        GBASELOGGER.debug("Fetch %s rows data." % count)
        while count > 0 and self._have_unread_result():
            count -= 1
            GBASELOGGER.debug("Start fetch data. count=%s" % count)
            row = self.fetchone()
            if row:
                results.append(row)
        return results
        
    def fetchall(self):
        '''
        ************************************************************
            Function    : fetchall
            Arguments   : 
            Return      : rows
            Description : Fetch all rows. The function execute after self.execute().
        ************************************************************
        '''
        GBASELOGGER.debug("Start fetch all data.")
        if not self._have_unread_result():
            GBASELOGGER.error("No result set can use.")
            raise GBaseError.InterfaceError("No result set can use.")
        
        results = []
        (r, e) = self._nextrow
        (rs, eof) = self._connection.get_rows()
        rows = []
        if r != None:
            rows = [r] + rs
            self._nextrow = (None, None)
        else:
            rows = rs
        self._rowcount = len(rows)
        GBASELOGGER.debug("Has %s rows data." % self._rowcount)
        for i in xrange(0, self._rowcount):
            results.append(self._row_to_python(rows[i]))
        self._handle_eof(eof)
        
        GBASELOGGER.debug("Fetched data. '%s'" % results)
        return results
    
    def has_rows(self):
        '''
        ************************************************************
            Function    : has_rows
            Arguments   : 
            Return      : True/False
            Description : Judgment is or not has row in resultset.
        ************************************************************
        '''
        if not self._description:
            return False
        return True

    def nextset(self, next_number=1):
        '''
        ************************************************************
            Function    : nextset
            Arguments   : 1) next_number (int) : Skip resultset number.
            Return      : 
            Description : If result include more resultsets. 
                          Then maybe use nextset skip number resultset.
        ************************************************************
        '''
        if not isinstance(next_number, int):
            GBASELOGGER.debug('Arguments is unvalid.')
            raise GBaseError.ProgrammingError('Arguments is unvalid.')
        
        if self._executed is None:
            raise GBaseError.ProgrammingError('Please use execute() first.')
            
        if next_number > len(self._split_sql(self._executed)):
            raise GBaseError.ProgrammingError(
                    'Cannot next %s resultsets.' % next_number)
            
        try:
            if self._exec_iterator:
                for i in xrange(0, next_number):
                    self._exec_iterator.next()
                    try:
                        self.fetchall()
                        if not self._have_unread_result():
                            raise

                        if not self._connection._have_next_result:
                            return None
                    except:
                        pass

                self._exec_iterator.next()
                return True
            else:
                raise
        except:
            raise GBaseError.ProgrammingError('No result set.')
            return None
    
    def setinputsizes(self, sizes):
        pass
    
    def setoutputsize(self, size, column=None):
        pass

    def _reset_result(self):
        '''
        ************************************************************
            Function    : _reset_result
            Arguments   : 
            Return      : 
            Description : Reset resultset relation member variables to initial.
        ************************************************************
        '''
        GBASELOGGER.debug("Reset all cursor internal variables.")
        self._rowcount = -1
        self._nextrow = (None, None)
        self._stored_results = []
        self._warnings = None
        self._warning_count = 0
        self._description = None
        self._executed = None
        self._executed_list = []
        self._call_list = None
        # self.reset()
        
    def _handle_parameters(self, params=()):
        """
        ************************************************************
            Function    : _handle_parameters
            Arguments   : 
            Return      : list
            Description : Process the parameters which were given when self.execute() was
                          called. It does following using the GBaseConnection converter:
                          * Convert Python types to GBase types
                          * Escapes characters required for GBase.
                          * Quote values when needed.
        ************************************************************
        """
        if isinstance(params, dict):
            GBASELOGGER.debug("parameter cannot be dict. %s" % str(params))
            raise GBaseError.ProgrammingError('parameter cannot be dict.')        
        try:
            res = params
            res = map(self.convert.to_gbase, res)
            res = map(self.convert.escape, res)
            res = map(self.convert.quote, res)
        except StandardError, e:
            raise GBaseError.ProgrammingError(
                "Failed processing format-parameters; %s" % e)
        else:
            GBASELOGGER.debug("Get res.'{%s}'" % res)
            return tuple(res)
    
    def _handle_result(self, result):
        """
        ************************************************************
            Function    : _handle_result
            Arguments   : 1) result (dict) : Handle resultset dictionary. 
            Return      : list
            Description : Handle resultset.
        ************************************************************
        """
        if not isinstance(result, dict):
            GBASELOGGER.error('Result not a dict type.')
            raise GBaseError.InterfaceError('Result not a dict type.')
        
        if 'columns' in result:
            GBASELOGGER.debug('Fetch columns info.')
            # Weak test, must be column/eof information
            self._description = result['columns']
            self._connection.unread_result = True
            # self._handle_resultset()
        elif 'affected_rows' in result:
            GBASELOGGER.debug('Handle no resultset.')
            # Weak test, must be an OK-packet
            self._connection.unread_result = False
            self._handle_noresultset(result)
        else:
            GBASELOGGER.error('Result error.')
            raise GBaseError.InterfaceError('Result error.')

    def _handle_noresultset(self, res):
        """
        ************************************************************
            Function    : _handle_noresultset
            Arguments   : 1) res (dict) :  
            Return      : 
            Description : Handles result of execute() when there is no result set. 
                          And get follow content: 
                          1) affected_rows
                          2) warning_count
                          3) fetch warnings and get content
        ************************************************************
        """
        try:
            GBASELOGGER.debug('Start handle no resultset.')
            self._rowcount = res['affected_rows']
            # self._last_insert_id = res['insert_id']
            self._warning_count = res['warning_count']
        except (KeyError, TypeError), err:
            GBASELOGGER.error("Failed handling non-resultset; %s" % err)
            raise GBaseError.ProgrammingError(
                "Failed handling non-resultset; %s" % err)
        
        if self._connection.get_warnings is True and self._warning_count:
            GBASELOGGER.debug("Start fetch warnings.")
            self._warnings = self._fetch_warnings()
    
    def _execute_iterator(self, query_iter):
        """
        ************************************************************
            Function    : _execute_iterator
            Arguments   : 1) query_iter (iter)  
            Return      : GBaseCursor objects 
            Description : Generator returns GBaseCursor objects for multiple statements
        ************************************************************
        """
        if not self._executed_list:
            GBASELOGGER.debug("Start split statements.")
            self._executed_list = self._split_sql(self._executed)
            
            GBASELOGGER.debug("Start split statements.'%s'" % self._executed_list)
            GBASELOGGER.sql(self._executed_list)
            
        for result, statement in itertools.izip(query_iter,
                                           iter(self._executed_list)):
            GBASELOGGER.debug("Start reset result.")
            self._reset_result()
            GBASELOGGER.debug("Start handle result.")
            self._handle_result(result)
            self._executed = statement
            yield self

    def _setconvert(self):
        """
        ************************************************************
            Function    : _setconvert
            Arguments   :   
            Return      :  
            Description : Set convert and use_unicode function. Get charset_name.
        ************************************************************
        """
        GBASELOGGER.debug("Start get characterset info and initial GBaseConvert.")
        charset_name = CharacterSet.get_info(self._connection._charset_id)[0]
        use_unicode = self._connection._use_unicode
        self.convert = GBaseConvert(charset_name, use_unicode)
        
    def _have_unread_result(self):
        """
        ************************************************************
            Function    : _have_unread_result
            Arguments   :   
            Return      :  
            Description : Check whether there is an unread result
        ************************************************************
        """
        try:
            return self._connection.unread_result
        except AttributeError:
            return False

    def _row_to_python(self, rowdata, desc=None):
        """
        ************************************************************
            Function    : _row_to_python
            Arguments   : 1) rowdata ()
                          2) desc ()
            Return      : 
            Description : Resultset row datatype to python datatype 
        ************************************************************
        """
        GBASELOGGER.debug("Start run to_python.")
        results = ()
        try:
            if not desc:
                desc = self._description
            for i, v in enumerate(rowdata):
                GBASELOGGER.debug("Use to_python convert rowdata.'%s'" % str(v))
                results += (self.convert.to_python(desc[i], v),)
                
        except StandardError, e:
            GBASELOGGER.error(
                "Failed converting row to Python types; %s" % e)
            raise GBaseError.InterfaceError(
                "Failed converting row to Python types; %s" % e)
        else:
            return results
        return None
    
    def _handle_eof(self, eof):
        """
        ************************************************************
            Function    : _handle_eof
            Arguments   : 1) eof (dict) : get eof packet from gbase server
            Return      : 
            Description : Handle eof packet content 
        ************************************************************
        """
        GBASELOGGER.debug("Start handle eof packet.")
        self._connection.unread_result = False
        self._nextrow = (None, None)
        self._warning_count = eof['warning_count']
        if self._connection.get_warnings is True and eof['warning_count']:
            GBASELOGGER.debug("Connection string get_warnings parameter is set. Start fetch warnings.")
            self._warnings = self._fetch_warnings()
            
    def _fetch_warnings(self):
        """
        ************************************************************
            Function    : _handle_eof
            Arguments   :
            Return      : results (dict)
            Description : Fetch warnings doing a SHOW WARNINGS. Can be called 
                          after getting the result.
                          Returns a result set or None when there were no warnings.
        ************************************************************
        """
        results = []
        try:
            GBASELOGGER.debug("Start fetch warnings.")
            GBASELOGGER.sql("SHOW WARNINGS")
            
            cursor = self._connection.cursor()
            cursor.execute("SHOW WARNINGS")
            results = cursor.fetchall()
            cursor.close()
        except StandardError, e:
            GBASELOGGER.error("Failed getting warnings; %s" % e)
            raise GBaseError.InterfaceError, GBaseError.InterfaceError(
                "Failed getting warnings; %s" % e), sys.exc_info()[2]
        
        if self._connection.raise_on_warnings is True:
            msg = '; '.join([ "(%s) %s" % (r[1], r[2]) for r in results])
            GBASELOGGER.debug(
                "Connection string raise_on_warnings is set True. Get msg '%s'." % msg)
            raise GBaseError.get_gbase_exception({'errno':results[0][1], 'errmsg':results[0][2], 'sqlstate':None})
        else:
            if len(results):
                return results

        return None
    
    def _fetchrow(self):
        '''
        ************************************************************
            Function    : _fetchrow
            Arguments   : 
            Return      : row (dict)
            Description : Fetch one row ( metadata ). 
        ************************************************************
        '''
        if not self._have_unread_result():
            return None
        row = None
        try:
            if self._nextrow == (None, None) :
                (row, eof) = self._connection.get_row()
            else:
                (row, eof) = self._nextrow
            
            if row:
                (nextrow, eof) = self._connection.get_row() 
                self._nextrow = (nextrow, eof)
                
                if eof is not None:
                    self._handle_eof(eof)
    
                if self._rowcount == -1:
                    self._rowcount = 1
                else:
                    self._rowcount += 1
                
            if eof:
                self._handle_eof(eof)
        except:
            raise
        else:
            return row
        
        return None
    
    def __iter__(self):
        '''
        ************************************************************
            Function    : __iter__
            Arguments   : 
            Return      : iter()
            Description : Iteration over the result set which calls self.fetchone()
                          and returns the next row. 
        ************************************************************
        '''
        return iter(self.fetchone, None)

    def _split_sql(self, operation):
        '''
        ************************************************************
            Function    : _split_sql
            Arguments   : 1) operation (string) : multiple sql 
            Return      : sql list
            Description : Split multiple sql to sql list.
        ************************************************************
        '''
        if self._split_rule is None:
            self._split_rule = re.compile(
                r''';(?=(?:[^"'`]*["'`][^"'`]*["'`])*[^"'`]*$)''')
        
        return self._split_rule.split(operation)

    def _get_sql_params(self, operation):
        '''
        ************************************************************
            Function    : _get_sql_params
            Arguments   : 1) operation (string) : multiple sql 
            Return      : Optimize INSERTs by batching them
            Description : 
        ************************************************************
        '''    
        sql = re.sub(self._sql_comment_rule, '', operation)
        res = re.search(self._sql_insert_values_rule, sql)
        fmt = res.group(1)
        return fmt

    def _match_insert_stmt(self, operation):
        '''
        ************************************************************
            Function    : _match_insert_stmt
            Arguments   : 1) operation (string) : multiple sql 
            Return      : a match object, or None if no match was found.
            Description : match multiple insert statement
        ************************************************************
        '''    
        return re.match(self._sql_insert_stmt_rule, operation)
    
