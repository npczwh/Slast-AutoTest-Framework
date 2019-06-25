import time
import GBaseLogger
global GBASELOGGER

from GBaseConnector.GBaseCursor import GBaseCursor
from GBaseConnector.GBaseSocket import GBaseSocket
from GBaseConnector.GBaseProtocol import GBaseProtocol
from GBaseConnector.GBaseConstants import (ClientFlag, ServerCmd, flag_is_set,
                                           ServerFlag, CharacterSet)
from GBaseConnector import GBaseError

class GBaseConnection(object):
    '''
    ************************************************************
        Module      : GBaseConnection
        Function    : Create connection to GBase server
        Corporation : General Data Technology CO,. Ltd.
        Team        : DMD, Interface Team
        Author      : wq
        Date        : 2013-7
        Version     : v1.0
        Description : Create connection to GBase server. And execute 
                      SQL and fetch rows.
    ************************************************************
    '''
    def __init__(self, **kwargs):
        '''
        ************************************************************
            Function    : __init__
            Arguments   : 1) **kwargs (dict) : connection arguments
                                                { 'user': gbase,
                                                  'port': 5258  }
            Return      : 
            Description : GBaseConnection construct function
        ************************************************************
        '''
        self._host = '127.0.0.1'
        self._user = ''
        self._password = ''
        self._database = ''
        self._port = 5258
        self.charset = None
        self._use_unicode = True
        self._collation = None
        self._connection_timeout = 15
        self._autocommit = True
        self._time_zone = None
        self._sql_mode = None
        self._get_warnings = False
        self._raise_on_warnings = False
        
        self._client_flags = ClientFlag.get_default()
        self._charset_id = 33  # utf8
        
        self._socket = None
        self._protocol = None
        self._hello_res = None
        self._server_version = None
        self._have_next_result = False 
        self._unread_result = False
        
        # Initial log system
        GBaseLogger.InitLog(**kwargs)
        
        if len(kwargs) > 0:
            GBASELOGGER.debug('Open connection.')
            self.connect(**kwargs)
        
    def parsecfg(self, **kwargs):
        '''
        ************************************************************
            Function    : parsecfg
            Arguments   : 1) **kwargs (dict) : connection arguments
                                                { 'user': gbase,
                                                  'port': 5258  }
            Return      : 
            Description : Parse config arguments and get values.
        ************************************************************
        '''
        config = kwargs.copy()
        # dsn
        if 'dsn' in config:
            GBASELOGGER.error('Not support dsn key word.')
            raise GBaseError.NotSupportedError('Not support dsn key word.')
        
        # host
        if 'host' in config:
            self._host = config['host']
            del config['host']
            
        # user
        if 'user' in config:
            self._user = config['user']
            del config['user']

        # Compatible other key
        compatible_keys = [
            ('db', 'database'),
            ('passwd', 'password'),
            ('connect_timeout', 'connection_timeout'),
        ]
        
        for comp_key, full_key in compatible_keys:
            try:
                if full_key not in config:
                    config[full_key] = config[comp_key]
                del config[comp_key]
            except KeyError:
                pass  # Missing compat argument is OK

        # password
        if 'password' in config:
            self._password = config['password']
            del config['password']
            
        # port
        if 'port' in config: 
            try:
                self._port = int(config['port'])
                del config['port']
            except KeyError:
                pass # Missing port argument is OK
            except ValueError:
                raise ValueError(
                    "TCP/IP port number should be an integer")
            
        if 'connection_timeout' in config:
            self.connection_timeout = config['connection_timeout']
            del config['connection_timeout']
        
        if 'autocommit' in config:
            self.autocommit = config['autocommit']
            del config['autocommit']
            
        if 'sql_mode' in config:
            self.sql_mode = config['sql_mode']
            del config['sql_mode']
            
        if 'get_warnings' in config:
            self.get_warnings = config['get_warnings']
            del config['get_warnings']
        
        # Configure client flags
        try:
            default = ClientFlag.get_default()
            self.set_client_flags(config['client_flags'] or default)
            del config['client_flags']
        except KeyError:
            pass  # Missing client_flags-argument is OK
        
        # Configure character set and collation
        if ('charset' in config or 'collation' in config):
            try:
                self.charset = config['charset']
                del config['charset']
            except KeyError:
                self.charset = None
            try:
                collation = config['collation']
                del config['collation']
            except KeyError:
                collation = None
            self._charset_id = CharacterSet.get_charset_info(self.charset, collation)[0]
        
        if 'raise_on_warnings' in config:
            self.raise_on_warnings = config['raise_on_warnings']
            del config['raise_on_warnings']
        
        if 'use_unicode' in config:
            self.use_unicode = config["use_unicode"]
            del config['use_unicode']
            
        # Other configuration
        for k, v in config.items():
            # TRACE Configuration
            if k.startswith('trace'):
                continue
            
            try:
                self.DEFAULT_CONFIG[k]
            except KeyError:
                raise AttributeError("The argument is not support. '%s'" % k)
            
            attribute = '_' + k
            try:
                setattr(self, attribute, v.strip())
            except AttributeError:
                setattr(self, attribute, v)
    
    @property
    def port(self):
        '''
        ************************************************************
            Function    : port
            Arguments   : 
            Return      : int 
            Description : Return current port.
        ************************************************************
        '''
        return self._port
    
    @property
    def user(self):
        '''
        ************************************************************
            Function    : user
            Arguments   : 
            Return      : string 
            Description : Return current user.
        ************************************************************
        '''
        return self._user
    
    @property
    def host(self):
        '''
        ************************************************************
            Function    : host
            Arguments   : 
            Return      : string 
            Description : Return current host.
        ************************************************************
        '''
        return self._host
    
    def _get_use_unicode(self):
        '''
        ************************************************************
            Function    : _get_use_unicode
            Arguments   :
            Return      : bool
            Description : Get self._use_unicode property value
        ************************************************************
        '''
        return self._use_unicode
    def _set_use_unicode(self, value):
        '''
        ************************************************************
            Function    : _set_use_unicode
            Arguments   : 1) value (bool): property value
            Return      : 
            Description : Set self._use_unicode property value and check its
                          valid.
        ************************************************************
        '''
        if not isinstance(value, bool):
            GBASELOGGER.error("use_unicode '%s' is not valid." % value)
            raise ValueError("use_unicode '%s' is not valid." % value)
        self._use_unicode = value
    use_unicode = property(_get_use_unicode, _set_use_unicode) 
    
    def _get_connection_timeout(self):
        '''
        ************************************************************
            Function    : _get_connection_timeout
            Arguments   : 
            Return      : int
            Description : Get self._connection_timeout property value.
        ************************************************************
        '''
        return self._connection_timeout
    def _set_connection_timeout(self, value):
        '''
        ************************************************************
            Function    : _set_connection_timeout
            Arguments   : 1) value (int): property value
            Return      : 
            Description : Set self._connection_timeout property value and check its
                          valid.
        ************************************************************
        '''
        if not isinstance(value, int):
            GBASELOGGER.error("connection_timeout '%s' is not valid." % value)
            raise ValueError("connection_timeout '%s' is not valid." % value)
        if value <= 0: 
            value = self._connection_timeout
        self._connection_timeout = value
    connection_timeout = property(_get_connection_timeout, _set_connection_timeout)
    
    def _get_autocommit(self):
        '''
        ************************************************************
            Function    : _get_autocommit
            Arguments   : 
            Return      : bool
            Description : Get self._autocommit property value.
        ************************************************************
        '''
        return self._autocommit
    def _set_autocommit(self, value):
        '''
        ************************************************************
            Function    : _set_autocommit
            Arguments   : 1) value (bool): property value
            Return      : 
            Description : Get self._autocommit property value and check its
                          valid.
        ************************************************************
        '''
        if not isinstance(value, bool):
            GBASELOGGER.error("autocommit '%s' is not valid." % value)
            raise ValueError("autocommit '%s' is not valid." % value)
        self._autocommit = value
    autocommit = property(_get_autocommit, _set_autocommit)
    
    def _get_sql_mode(self):
        '''
        ************************************************************
            Function    : _get_sql_mode
            Arguments   : 
            Return      : dict
            Description : Get self._sql_mode property value.
        ************************************************************
        '''
        return self._query_info("SELECT @@session.sql_mode")[0]
    
    def _set_sql_mode(self, value):
        '''
        ************************************************************
            Function    : _set_sql_mode
            Arguments   : 1) value (bool): property value
            Return      : 
            Description : Get self._sql_mode property value and check its
                          valid.
        ************************************************************
        '''
        if isinstance(value, (list, tuple)):
            value = ','.join(value)
        self.query("SET @@session.sql_mode = '%s'" % value)
        self._sql_mode = value
        
    sql_mode = property(_get_sql_mode, _set_sql_mode)
    
    def _get_get_warnings(self):
        '''
        ************************************************************
            Function    : _get_get_warnings
            Arguments   : 
            Return      : bool
            Description : Get self._get_warnings property value.
        ************************************************************
        '''
        return self._get_warnings
    def _set_get_warnings(self, value):
        '''
        ************************************************************
            Function    : _set_get_warnings
            Arguments   : 1) value (bool): property value
            Return      : 
            Description : Get self._get_warnings property value and check its
                          valid.
        ************************************************************
        '''
        if not isinstance(value, bool):
            GBASELOGGER.error("get_warnings '%s' is not valid." % value)
            raise ValueError("get_warnings '%s' is not valid." % value)
        self._get_warnings = value
    get_warnings = property(_get_get_warnings, _set_get_warnings)
    
    def _get_raise_on_warnings(self):
        '''
        ************************************************************
            Function    : _get_raise_on_warnings
            Arguments   : 
            Return      : bool
            Description : Get self._raise_on_warnings property value.
        ************************************************************
        '''
        return self._raise_on_warnings
    def _set_raise_on_warnings(self, value):
        '''
        ************************************************************
            Function    : _set_raise_on_warnings
            Arguments   : 1) value (bool): property value
            Return      : 
            Description : Get self._raise_on_warnings property value and check its
                          valid.
        ************************************************************
        '''
        if not isinstance(value, bool):
            GBASELOGGER.error("raise_on_warnings '%s' is not valid." % value)
            raise ValueError("raise_on_warnings '%s' is not valid." % value)
        self._raise_on_warnings = value
    raise_on_warnings = property(_get_raise_on_warnings, _set_raise_on_warnings)
    
    def _get_unread_result(self):
        '''
        ************************************************************
            Function    : _get_unread_result
            Arguments   : 
            Return      : bool
            Description : Get self._unread_result property value.
        ************************************************************
        '''
        return self._unread_result
    def _set_unread_result(self, value):
        '''
        ************************************************************
            Function    : _set_raise_on_warnings
            Arguments   : 1) value (bool): property value
            Return      : 
            Description : Get self._unread_result property value and check its
                          valid.
        ************************************************************
        '''
        if not isinstance(value, bool):
            raise ValueError("unread_result '%s' is not valid." % value)
        self._unread_result = value
    unread_result = property(_get_unread_result, _set_unread_result)
    
    def set_time_zone(self, value):
        '''
        ************************************************************
            Function    : set_time_zone
            Arguments   : 1) value (string): time_zone value
            Return      : 
            Description : Set the time zone.
        ************************************************************
        '''
        self.query("SET @@session.time_zone = '%s'" % value)
        self._time_zone = value
    def get_time_zone(self):
        '''
        ************************************************************
            Function    : get_time_zone
            Arguments   : 
            Return      : dict
            Description : Get the current time zone
        ************************************************************
        '''
        return self._query_info("SELECT @@session.time_zone")[0]
    time_zone = property(get_time_zone, set_time_zone)
    
    def set_database(self, value):
        '''
        ************************************************************
            Function    : set_database
            Arguments   : 
            Return      : 
            Description : Set the current database
        ************************************************************
        '''
        self.query("USE %s" % value)
    def get_database(self):
        '''
        ************************************************************
            Function    : get_database
            Arguments   : 
            Return      : dict
            Description : Get the current database
        ************************************************************
        '''
        return self._query_info("SELECT DATABASE()")[0]
    database = property(get_database, set_database)
    
    def set_client_flags(self, flags):
        '''
        ************************************************************
            Function    : set_client_flags
            Arguments   : 1) flags (int): client flags value
            Return      : int
            Description : Set the client flags
                          set_client_flags([ClientFlag.FOUND_ROWS,-ClientFlag.LONG_FLAG])
        ************************************************************
        '''
        if isinstance(flags, int) and flags > 0:
            self._client_flags = flags
        elif isinstance(flags, (tuple, list)):
            for flag in flags:
                if flag < 0:
                    self._client_flags &= ~abs(flag)
                else:
                    self._client_flags |= flag
        else:
            GBASELOGGER.error("set_client_flags expect int (>0) or set")
            raise GBaseError.ProgrammingError("set_client_flags expect int (>0) or set")

        return self._client_flags
    
    def connect(self, **kwargs):
        '''
        ************************************************************
            Function    : connect
            Arguments   : 1) **kwargs (dict) : connection arguments
                                                { 'user': gbase,
                                                  'port': 5258  }
            Return      : 
            Description : connect to GBase server
        ************************************************************
        '''
        if len(kwargs) > 0:
            GBASELOGGER.debug('Parse connection string.')
            self.parsecfg(**kwargs)
        
        GBASELOGGER.debug('Get protocol object.')
        self._protocol = self._get_protocol()
        
        GBASELOGGER.debug('Close connection.')
        self.disconnect()
        
        GBASELOGGER.debug('Open connection.')
        self._open_connection()
        
    def _get_protocol(self):
        '''
        ************************************************************
            Function    : _get_protocol
            Arguments   : 
            Return      : instance of GBaseProtocol class
            Description : Create protocal object
        ************************************************************
        '''
        protocol = self._protocol
        if protocol is None:
            protocol = GBaseProtocol()
        
        return protocol
    
    def _get_connection(self):
        '''
        ************************************************************
            Function    : _get_connection
            Arguments   : 
            Return      : 
            Description : Only support TCP socket
        ************************************************************
        '''
        socket = None
        socket = GBaseSocket(host=self._host,
                             port=self._port,
                             timeout=self.connection_timeout)
        return socket
    
    def _open_connection(self):
        '''
        ************************************************************
            Function    : _open_connection
            Arguments   : 
            Return      : 
            Description : Use socket object open connection and make auth check
                          1) receive hello packet (shake hands)
                          2) auth check (user, pass)
                          3) set character
                          4) set auto commit
        ************************************************************
        '''
        # open socket
        GBASELOGGER.debug('Get socket object.')
        self._socket = self._get_connection()

        # hand shake
        GBASELOGGER.debug('Shake hand and send hello packet.')
        self._do_hello()
        
        # auth check
        GBASELOGGER.debug('Authorize and check user password.')
        if self._do_authcheck():
            # set charset
            GBASELOGGER.debug('Set character set and collation.')
            self.set_charset_collation(charset=self._charset_id)
            
            GBASELOGGER.debug("Set other initial info. Include "
                              "autocommit, time_zone, sql_mode, etc.")
            self._do_setotherinfo()
        else:
            GBASELOGGER.error('Authorize failure.')
            raise GBaseError.InternalError('Authorize failure.')
        
        # send and recv ( compressed protocol )
        # if self._client_flags & ClientFlag.COMPRESS:
        #    self._socket.recv = self._socket.recv_compressed
        #    self._socket.send = self._socket.send_compressed
    
    def _do_setotherinfo(self):
        '''
        ************************************************************
            Function    : _do_setotherinfo
            Arguments   : 
            Return      : 
            Description : Set other initial info. Include autocommit, 
                          time_zone, sql_mode, etc.
        ************************************************************
        '''
        # set auto commit
        autocommit_sql = 'SET AUTOCOMMIT=%s' % self.autocommit
        GBASELOGGER.debug(autocommit_sql)
        GBASELOGGER.sql(autocommit_sql)
        self._exec_query(autocommit_sql)
        
        # set time_zone and sql_mode
        if self._time_zone:
            GBASELOGGER.debug('Set _time_zone.')
            self.time_zone = self._time_zone
        if self._sql_mode:
            GBASELOGGER.debug('Set _sql_mode.')
            self.sql_mode = self._sql_mode

    def _do_hello(self):
        '''
        ************************************************************
            Function    : _do_hello
            Arguments   : 
            Return      : 
            Description : Receive hello packet (shake hands).
                          Will receive to hello packet when first 
                          connect to GBase server
        ************************************************************
        '''
        try:
            GBASELOGGER.debug('Receive hello package.')
            packet = self._socket.receive_packet()
        except:
            GBASELOGGER.error("Receive packet exception")
            raise GBaseError.InterfaceError("Receive packet exception.")
        
        if self._protocol.is_error(packet):
            res = self._protocol.parse_error_res(packet)
            GBASELOGGER.error("Packet is error. Errno=%s ErrMsg=%s SqlState=%s" % 
                              (res['errno'], res['errmsg'], res['sqlstate']))            
            raise GBaseError.get_gbase_exception(res)

        try:
            GBASELOGGER.debug('Parse hello package.')
            hello_res = self._protocol.parse_hello_res(packet)
        except:
            GBASELOGGER.error('Parse hello package error.')
            raise GBaseError.InterfaceError('Parse hello package error.')
        
        try:
            # get server version from package
            server_version = hello_res['server_version_original']
        except:
            GBASELOGGER.error('Hello package no server_version property.')
            raise GBaseError.InterfaceError('Hello package no server_version property.')
        
        GBASELOGGER.debug("Get hello package & server_version info "
                          "from hello package. "
                          "hello_res: '%s' server_version: '%s'" % (hello_res, server_version))
        self._hello_res = hello_res
        self._server_version = server_version
        
    def _do_authcheck(self):
        '''
        ************************************************************
            Function    : _do_authcheck
            Arguments   : 
            Return      : bool
            Description : Send authorize check packet (user, pass)
        ************************************************************
        '''
        # make auth packet
        try:
            GBASELOGGER.debug('Make auth packet.')
            packet = self._protocol.make_auth(self._user,
                                              self._password,
                                              self._hello_res['scramble'],
                                              self._database,
                                              self._charset_id,
                                              self._client_flags)
        except:
            GBASELOGGER.error('Make auth packet exception.')
            raise GBaseError.InterfaceError('Make auth packet exception.')
        
        # send auth package
        try:
            GBASELOGGER.debug("Send auth packet.")
            self._socket.send_data(packet)
        except:
            GBASELOGGER.error('Send auth packet exception.')
            raise GBaseError.InterfaceError('Send auth packet exception.')
        
        # receive server packet
        try:
            GBASELOGGER.debug("Receive auth packet.")
            packet = self._socket.receive_packet()        
        except:
            GBASELOGGER.error('Receive packet exception.')
            raise GBaseError.InterfaceError('Receive packet exception.')

        # judgement eof packet
        if self._protocol.is_eof(packet):
            raise GBaseError.NotSupportedError(
              "Old passwords mode is not supported.")
            
        if self._protocol.is_error(packet):
            res = self._protocol.parse_error_res(packet)
            GBASELOGGER.error("Packet is error. Errno=%s ErrMsg=%s SqlState=%s" % 
                              (res['errno'], res['errmsg'], res['sqlstate']))
            raise GBaseError.get_gbase_exception(res)
        
        try:
            if (not (self._client_flags & ClientFlag.CONNECT_WITH_DB)
                and self._database):
                GBASELOGGER.debug('Initial database.')
                self.initdb(self._database)
        except:
            GBASELOGGER.error('Initial database exception.')
            raise
        
        return True
    
    def initdb(self, db):
        '''
        ************************************************************
            Function    : initdb
            Arguments   : db(string) : database name
            Return      : dict
            Description : Send initial database packet
        ************************************************************
        '''
        packet = self._send(ServerCmd.INIT_DB, db)
        
        if self._protocol.is_error(packet):
            res = self._protocol.parse_error_res(packet)
            GBASELOGGER.error("Packet is error. Errno=%s ErrMsg=%s SqlState=%s" % 
                              (res['errno'], res['errmsg'], res['sqlstate']))            
            raise GBaseError.get_gbase_exception(res)

        return self._handle_ok(packet)
    
    def _handle_ok(self, packet):
        '''
        ************************************************************
            Function    : _handle_ok
            Arguments   : packet (string) : need handle packet
            Return      : dict
            Description : Handle ok packet.
        ************************************************************
        '''
        if self._protocol.is_ok(packet):
            GBASELOGGER.debug('Parse ok packet.')
            ok_packet = self._protocol.parse_ok_res(packet)
            
            GBASELOGGER.debug('Get server status from packet.')
            flag = ok_packet['server_status']
            
            GBASELOGGER.debug('Trigger next result.')
            self._trigger_next_result(flag)
            return ok_packet
        elif self._protocol.is_error(packet):
            GBASELOGGER.error("No receive ok packet or package is error.")
            raise GBaseError.InterfaceError("No receive ok packet or package is error.")
        
        GBASELOGGER.error('Packet is not OK packet')
        raise GBaseError.InterfaceError('Packet is not OK packet')
    
    def _handle_eof(self, packet):
        '''
        ************************************************************
            Function    : _handle_eof
            Arguments   : packet (string) : need handle packet
            Return      : list
            Description : Handle a GBase EOF packet
        ************************************************************
        '''
        if self._protocol.is_eof(packet):
            GBASELOGGER.debug('Parse EOF packet.')
            eof_packet = self._protocol.parse_eof(packet)
            
            GBASELOGGER.debug('Get status flag.')
            server_flag = eof_packet['server_flag']
            
            GBASELOGGER.debug('Trigger next result.')
            self._trigger_next_result(server_flag)
            return eof_packet
        elif self._protocol.is_error(packet):
            res = self._protocol.parse_error_res(packet)
            GBASELOGGER.error("Packet is error. Errno=%s ErrMsg=%s SqlState=%s" % 
                              (res['errno'], res['errmsg'], res['sqlstate']))            
            raise GBaseError.get_gbase_exception(res)

        GBASELOGGER.error('Expected EOF packet')
        raise GBaseError.InterfaceError('Expected EOF packet')
        
    def _handle_result(self, packet):
        '''
        ************************************************************
            Function    : _handle_result
            Arguments   : packet (string) : need handle packet
            Return      : dict
            Description : Get a GBase Result
        ************************************************************
        '''
        if not packet or len(packet) < 4:
            GBASELOGGER.error('GBase Server can not response')
            raise GBaseError.InterfaceError('GBase Server can not response')
        elif self._protocol.is_ok(packet):
            GBASELOGGER.debug("Packet is OK.")
            return self._handle_ok(packet)
        elif self._protocol.is_eof(packet):
            GBASELOGGER.error("Packet is EOF.")
            return self._handle_eof(packet)
        elif self._protocol.is_error(packet):
            res = self._protocol.parse_error_res(packet)
            GBASELOGGER.error("Packet is error. Errno=%s ErrMsg=%s SqlState=%s" % 
                              (res['errno'], res['errmsg'], res['sqlstate']))            
            raise GBaseError.get_gbase_exception(res)

        # We have a text result set
        GBASELOGGER.debug('Parse column count.')
        column_count = self._protocol.parse_column_count(packet)
        if not column_count or not isinstance(column_count, int):
            GBASELOGGER.error('Result set is not valid.')
            raise GBaseError.InterfaceError('Result set is not valid.')

        GBASELOGGER.debug("Loop receive column. count:'%s'" % column_count)
        columns = [None, ] * column_count
        for i in xrange(0, column_count):
            packet = self._socket.receive_packet()
            GBASELOGGER.debug("Receive column packet and parse packet.")            
            columns[i] = self._protocol.parse_column_res(packet)
        
        GBASELOGGER.debug("Recive EOF packet.")
        packet = self._socket.receive_packet()
        GBASELOGGER.debug("Handle EOF packet.")
        eof_packet = self._handle_eof(packet)
        
        GBASELOGGER.debug("Set unread_result=True.")
        self.unread_result = True
        return {'columns': columns, 'eof': eof_packet}
        
    def _send(self, command, argument=None):
        '''
        ************************************************************
            Function    : _handle_result
            Arguments   : 1) command (int)     : SQL command flag. ServerCmd.QUERY
                          2) argument (string) : SQL command. Select 1
            Return      : packets (string)
            Description : Send a command to the GBase server
        ************************************************************
        '''
        GBASELOGGER.debug("Send packet. command='%s', argument='%s'" % (command, argument))
        if self.unread_result:
            GBASELOGGER.error("Unread result found.")
            raise GBaseError.InternalError("Unread result found.")
        
        try:
            GBASELOGGER.debug("Make command.")
            packet = self._protocol.make_command(command, argument)
            GBASELOGGER.debug("Send packet.")
            self._socket.send_data(packet, 0)
        except AttributeError:
            GBASELOGGER.error("GBase Connection not available.")
            raise GBaseError.InternalError("GBase Connection not available.")
        
        GBASELOGGER.debug("Receive packet.")
        packet = self._socket.receive_packet()
        GBASELOGGER.debug("Return receive packet.")
        return packet

    def _trigger_next_result(self, flags):
        '''
        ************************************************************
            Function    : _trigger_next_result
            Arguments   : 1) flags (int) : Server Flag 
            Return      : 
            Description : Toggle whether there more results
                          This method checks the whether 
                          MORE_RESULTS_EXISTS is set in flags.
        ************************************************************
        '''
        GBASELOGGER.debug("Check server flag. '%s'" % flags)
        if flag_is_set(ServerFlag.MORE_RESULTS_EXISTS, flags):
            GBASELOGGER.debug("MORE_RESULTS_EXISTS flag exist and set _have_next_result=True.")
            self._have_next_result = True
        else:
            GBASELOGGER.debug("MORE_RESULTS_EXISTS flag not exist and set _have_next_result=False.")
            self._have_next_result = False

    def set_charset_collation(self, charset=None, collation=None):
        '''
        ************************************************************
            Function    : set_charset_collation
            Arguments   : 1) charset (string) : charset string
                          2) collation (string) : collation string
            Return      : 
            Description : Sets the character set and collation for the current connection
                          set_charset_collation('utf8','utf8_general_ci')
        ************************************************************
        '''
        if charset:
            if isinstance(charset, int):
                self._charset_id = charset
                (charset_name, collation_name) = CharacterSet.get_info(self._charset_id)
                GBASELOGGER.debug("Get CharacterSet '%s' and collation '%s'." % 
                                  (charset_name, collation_name))
            elif isinstance(charset, str):
                (self._charset_id, charset_name, collation_name) = \
                    CharacterSet.get_charset_info(charset, collation)
                GBASELOGGER.debug("Get _charset_id '%s' charset_name '%s' collation_name '%s'." %
                                  (self._charset_id, charset_name, collation_name))
            else:
                GBASELOGGER.error("charset should be integer, string or None")
                raise ValueError("charset should be integer, string or None")
        elif collation:
            (self._charset_id, charset_name, collation_name) = \
                    CharacterSet.get_charset_info(collation=collation)
            GBASELOGGER.debug("Get _charset_id '%s' charset_name '%s' collation_name '%s'." % 
                              (self._charset_id, charset_name, collation_name))
            
        query = "SET NAMES '%s' COLLATE '%s'" % (charset_name, collation_name)
        GBASELOGGER.debug("Start execute query: %s" % query)

        self._exec_query(query)
        # self.converter.set_charset(charset_name)
        
    def _exec_query(self, statement):
        '''
        ************************************************************
            Function    : _exec_query
            Arguments   : 1) statement (string) : SQL command
            Return      : 
            Description : Execute a query
        ************************************************************
        '''
        if self.unread_result is True:
            GBASELOGGER.error("Unread result found.")
            raise GBaseError.OperationalError("Unread result found.")
        
        GBASELOGGER.debug("Execute query. '%s'" % statement)
        self.query(statement)
    
    def query(self, statement):
        '''
        ************************************************************
            Function    : query
            Arguments   : 1) statement (string) : SQL command
            Return      : dict
            Description : Execute a query and return result
        ************************************************************
        '''
        GBASELOGGER.debug("Send query packet. SQL: '%s'" % statement)
        GBASELOGGER.sql(statement)
        packet = self._send(ServerCmd.QUERY, statement)
        GBASELOGGER.debug("Handle result.")
        result = self._handle_result(packet)
        
        if self._have_next_result:
            GBASELOGGER.sql('Use query_iter for statements with multiple queries.')
            raise GBaseError.OperationalError('Use query_iter for statements with multiple queries.')
        return result
    
    def query_iterator(self, statements):
        '''
        ************************************************************
            Function    : query_iterator
            Arguments   : 1) statements (string) : multiple sql string
            Return      : 
            Description : Send one or more statements to the GBase server
        ************************************************************
        '''
        # Handle the first query result
        GBASELOGGER.debug("Handle the first query result '%s'" % statements)
        packet = self._send(ServerCmd.QUERY, statements)
        yield self._handle_result(packet)
        
        # Handle next results
        while self._have_next_result:
            if self.unread_result:
                GBASELOGGER.error("Unread result found.")
                raise GBaseError.InternalError("Unread result found.")
            else:
                GBASELOGGER.debug("Handle receive packet.")
                packet = self._socket.receive_packet()
                result = self._handle_result(packet)
            yield result

    def quit(self):
        '''
        ************************************************************
            Function    : quit
            Arguments   : 
            Return      : string
            Description : Send quit packet to the GBase Server
        ************************************************************
        '''
        if self.unread_result:
            GBASELOGGER.error("Unread result found.")
            raise GBaseError.InternalError("Unread result found.")
        
        GBASELOGGER.debug("Make command packet.")
        packet = self._protocol.make_command(ServerCmd.QUIT)
        
        GBASELOGGER.debug("Send command packet.")
        self._socket.send_data(packet, 0)
        return packet
    
    def is_connected(self):
        '''
        ************************************************************
            Function    : is_connected
            Arguments   : 
            Return      : bool
            Description : Judgment current connect is or not connected
        ************************************************************
        '''
        try:
            GBASELOGGER.debug("Send ping command.")
            self.ping()
        except GBaseError.Error:
            GBASELOGGER.error("Send ping command error.")
            return False  # This method does not raise
        return True
    
    def ping(self, reconnect=False, attempts=1, delay=0):
        '''
        ************************************************************
            Function    : ping
            Arguments   : 1) reconnect (bool): Is or not re-connect GBase server
                          2) attempts (int): Try count
                          3) delay (int): Delay time when try re-connect
            Return      : 
            Description : Send ping command to GBase server
        ************************************************************
        '''
        try:
            packet = self._send(ServerCmd.PING)
            
            GBASELOGGER.debug("Send handle ok command.")
            self._handle_ok(packet)
        except:
            if reconnect:
                GBASELOGGER.debug("Send reconnect to GBase server.")
                self.reconnect(attempts=attempts, delay=delay)
            else:
                GBASELOGGER.error("Connection to GBase is not available.")
                raise GBaseError.InterfaceError("Connection to GBase is not available.")
    
    def reconnect(self, attempts=1, delay=0):
        '''
        ************************************************************
            Function    : ping
            Arguments   : 1) attempts (int): Try count
                          2) delay (int): Delay time when try re-connect
            Return      : 
            Description : Attempt to reconnect to the GBase server. 
                          Default only try once. 
        ************************************************************
        '''
        counter = 0
        while counter != attempts:
            counter = counter + 1
            try:
                GBASELOGGER.debug("Disconnect to GBase server.")
                self.disconnect()
                
                GBASELOGGER.debug("Connect to GBase server.")
                self.connect()
            except Exception, err:
                if counter == attempts:
                    GBASELOGGER.error("Can not reconnect to GBase after %s "
                                      "attempt(%s): " % (attempts,err))
                    raise GBaseError.InterfaceError("Can not reconnect to GBase after %s "
                                                     "attempt(%s): " % (attempts,err))
            if delay > 0:
                time.sleep(delay)
    
    def disconnect(self):
        '''
        ************************************************************
            Function    : disconnet
            Arguments   : 
            Return      : 
            Description : Disconnect from the GBase server. 
        ************************************************************
        '''
        if not self._socket:
            return

        try:
            GBASELOGGER.debug("Quit GBase server.")
            self.quit()
            
            GBASELOGGER.debug("Close socket tcp connection.")
            self._socket.close_tcp()
        except GBaseError.Error:
            pass  # Getting an exception would mean we are disconnected.
    close = disconnect

    def cursor(self):
        '''
        ************************************************************
            Function    : cursor
            Arguments   : 
            Return      : instance of GBaseCursor class
            Description : Returns a cursor object
        ************************************************************
        '''
        if not self.is_connected():
            GBASELOGGER.error("GBase Connection not available.")
            raise GBaseError.OperationalError("GBase Connection not available.")
        
        GBASELOGGER.debug("Return GBaseCursor object.")
        return GBaseCursor(self)
    
    def commit(self):
        '''
        ************************************************************
            Function    : commit
            Arguments   : 
            Return      : 
            Description : Commit current transaction
        ************************************************************
        '''
        GBASELOGGER.debug("Execute COMMIT command.")
        GBASELOGGER.sql("COMMIT")
        self._exec_query("COMMIT")

    def rollback(self):
        '''
        ************************************************************
            Function    : rollback
            Arguments   : 
            Return      : 
            Description : Rollback current transaction
        ************************************************************
        '''
        GBASELOGGER.debug("Execute ROLLBACK command.")
        GBASELOGGER.sql("ROLLBACK")
        self._exec_query("ROLLBACK")
    
    def _query_info(self, query):
        '''
        ************************************************************
            Function    : _query_info
            Arguments   : 
            Return      : dict
            Description : Send a query which only returns 1 row
        ************************************************************
        '''
        GBASELOGGER.sql("Get cursor")
        cursor = self.cursor()
        
        GBASELOGGER.sql("Execute query: '%s'" % query)
        GBASELOGGER.sql("Execute query: '%s'" % query)
        cursor.execute(query)
        
        GBASELOGGER.sql("Fetch one row.")
        return cursor.fetchone()
    
    def get_rows(self, count=None):
        '''
        ************************************************************
            Function    : get_rows
            Arguments   : 1) count (int) : Fetch how much rows
            Return      : a tuple with 2 elements: a list with all rows and
                          the EOF packet.
            Description : Get all rows returned by the GBase server
        ************************************************************
        '''
        if not self.unread_result:
            GBASELOGGER.error("No result set available.")
            raise GBaseError.InternalError("No result set available.")
        
        GBASELOGGER.debug("Read result set rows.")
        rows = self._protocol.read_result_set(self._socket, count)
        if rows[-1] is not None:
            GBASELOGGER.debug("Trigger next result and set unread_result=False.")
            self._trigger_next_result(rows[-1]['server_flag'])
            self.unread_result = False

        return rows
    
    def get_row(self):
        '''
        ************************************************************
            Function    : get_row
            Arguments   : 
            Return      : tuple
            Description : Get a rows returned by the GBase server
        ************************************************************
        '''
        GBASELOGGER.debug("Get rows.")
        (rows, eof) = self.get_rows(count=1)
        if len(rows):
            return (rows[0], eof)
        return (None, eof)

    def get_server_version(self):
        '''
        ************************************************************
            Function    : get_server_version
            Arguments   : 
            Return      : string
            Description : Get server version
        ************************************************************
        '''
        return self._server_version

    def get_server_info(self):
        '''
        ************************************************************
            Function    : get_server_info
            Arguments   : 
            Return      : string
            Description : Get server information
        ************************************************************
        '''
        try:
            return self._hello_res['server_version_original']
        except (TypeError, KeyError):
            return None
        
    DEFAULT_CONFIG = {
        'database': None,
        'user': '',
        'password': '',
        'host': '127.0.0.1',
        'port': 5258,
        'use_unicode': True,
        'charset': 'utf8',
        'collation': None,
        'autocommit': False,
        'time_zone': None,
        'sql_mode': None,
        'get_warnings': False,
        'raise_on_warnings': False,
        'connection_timeout': None,
        'client_flags': 0,
        'passwd': None,
        'db': None,
        'connect_timeout': None,
        'dsn': None
    }
