'''
Created on 2013-7-10

@author: lyh
'''

from GBaseConnector.errmsg import get_client_errmsg


_CUSTOM_ERROR_EXCEPTIONS = {}
def custom_error_exception(error=None, exception=None):  
    global _CUSTOM_ERROR_EXCEPTIONS

    if isinstance(error, dict) and not len(error):
        _CUSTOM_ERROR_EXCEPTIONS = {}
        return _CUSTOM_ERROR_EXCEPTIONS

    if not error and not exception:
        return _CUSTOM_ERROR_EXCEPTIONS

    if not isinstance(error, (int, dict)):
        raise ValueError(
            "The error argument should be either an integer or dictionary")

    if isinstance(error, int):
        error = { error: exception }

    for errno, exception in error.items():
        if not isinstance(errno, int):
            raise ValueError("error number should be an integer")
        try:
            if not issubclass(exception, Exception):
                raise TypeError
        except TypeError:
            raise ValueError("exception should be subclass of Exception")
        _CUSTOM_ERROR_EXCEPTIONS[errno] = exception

    return _CUSTOM_ERROR_EXCEPTIONS

class Error(StandardError):
    def __init__(self, errmsg = None, errno = None, sqlstate = None, values = None):
        self.msg = errmsg
        self.errno = errno or -1
        self.sqlstate = sqlstate
        
        if not self.msg and (2000 <= self.errno < 3000):
            errmsg = get_client_errmsg(self.errno)
            if values is not None:
                try:
                    errmsg = errmsg % values
                except TypeError, err:
                    errmsg = errmsg + " (Warning: %s)" % err
            self.msg = errmsg
        elif not self.msg:
            self.msg = 'Unknown error'
        
        if self.msg and self.errno != -1:
            if self.sqlstate:
                self.msg = '%d (%s): %s' % (self.errno, self.sqlstate,
                                            self.msg)
            else:
                self.msg = '%d: %s' % (self.errno, self.msg)
    
    def __str__(self):
        return self.msg

def get_gbase_exception(errinfo):
    """
    """
    errno = errinfo['errno']
    msg   = errinfo['errmsg']
    sqlstate = errinfo['sqlstate']
    try:
        return _CUSTOM_ERROR_EXCEPTIONS[errno](
            errmsg=msg, errno=errno, sqlstate=sqlstate)
    except KeyError:
        # Error was not mapped to particular exception
        pass

    if not sqlstate:
        return DatabaseError(errmsg=msg, errno=errno)

    try:
        return _SQLSTATE_CLASS_EXCEPTION[sqlstate[0:2]](
            errmsg=msg, errno=errno, sqlstate=sqlstate)
    except KeyError:
        # Return default InterfaceError
        return DatabaseError(errmsg=msg, errno=errno, sqlstate=sqlstate)

class Warning(StandardError):
    """Exception for important warnings"""
    pass

class InterfaceError(Error):
    """Exception for errors related to the interface"""
    pass

class DatabaseError(Error):
    """Exception for errors related to the database"""
    pass

class InternalError(DatabaseError):
    """Exception for errors internal database errors"""
    pass

class OperationalError(DatabaseError):
    """Exception for errors related to the database's operation"""
    pass

class ProgrammingError(DatabaseError):
    """Exception for errors programming errors"""
    pass

class IntegrityError(DatabaseError):
    """Exception for errors regarding relational integrity"""
    pass

class DataError(DatabaseError):
    """Exception for errors reporting problems with processed data"""
    pass

class NotSupportedError(DatabaseError):
    """Exception for errors when an unsupported database feature was used"""
    pass

_SQLSTATE_CLASS_EXCEPTION = {
    '02': DataError, # no data
    '07': DatabaseError, # dynamic SQL error
    '08': OperationalError, # connection exception
    '0A': NotSupportedError, # feature not supported
    '21': DataError, # cardinality violation
    '22': DataError, # data exception
    '23': IntegrityError, # integrity constraint violation
    '24': ProgrammingError, # invalid cursor state
    '25': ProgrammingError, # invalid transaction state
    '26': ProgrammingError, # invalid SQL statement name
    '27': ProgrammingError, # triggered data change violation
    '28': ProgrammingError, # invalid authorization specification
    '2A': ProgrammingError, # direct SQL syntax error or access rule violation
    '2B': DatabaseError, # dependent privilege descriptors still exist
    '2C': ProgrammingError, # invalid character set name
    '2D': DatabaseError, # invalid transaction termination
    '2E': DatabaseError, # invalid connection name
    '33': DatabaseError, # invalid SQL descriptor name
    '34': ProgrammingError, # invalid cursor name
    '35': ProgrammingError, # invalid condition number
    '37': ProgrammingError, # dynamic SQL syntax error or access rule violation
    '3C': ProgrammingError, # ambiguous cursor name
    '3D': ProgrammingError, # invalid catalog name
    '3F': ProgrammingError, # invalid schema name
    '40': InternalError, # transaction rollback
    '42': ProgrammingError, # syntax error or access rule violation
    '44': InternalError, # with check option violation
    'HZ': OperationalError, # remote database access
    'XA': IntegrityError,
    '0K': OperationalError,
    'HY': DatabaseError, # default when no SQLState provided by GBase server
}