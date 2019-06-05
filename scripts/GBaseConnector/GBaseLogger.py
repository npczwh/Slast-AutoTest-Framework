import logging
import thread
import __builtin__

__builtin__.GBASELOGGER = None
__builtin__.LOCK = thread.allocate_lock()
def InitLog(**kwargs):
    global LOCK, GBASELOGGER
    try:
        LOCK.acquire()
        if GBASELOGGER is None:
            __builtin__.GBASELOGGER = GBaseLogger(**kwargs)
    finally:
        LOCK.release()

class GBaseLogger(object):
    '''
    ************************************************************
        Module      : GBaseLogger
        Function    : Record all system log and write to log file
        Corporation : General Data Technology CO,. Ltd.
        Team        : DMD, Interface Team
        Author      : wq, lyh
        Date        : 2013-7
        Version     : v1.0
        Description : Implement output message to log file. 
                      support Debug, Error, Sql mode log output.
                      
                      options:
                      trace: [True|False]
                      trace_mode: [debug|error|sql|all]
                      trace_file: [debug*c:\\dbg.log|error*c:\\err.log|sql*c:\\sql.log|all*c:\\all.log]
                      trace_maxbytes: 20971520
                      trace_backupCount: 2
    ************************************************************
        usage:
            1) config
            config = {'trace':True,'trace_mode':'debug','trace_file':'debug*c:\\dbg.log'}
            logger = Logging(**config)
            logger.debug('output debug message.')
            logger.close()
            
            2) other module reference:
            global GBASELOGGER
            GBASELOGGER.debug('debug error')
        examples:
            config = {'trace':True,'trace_mode':'debug'}
            config = {'trace':True,'trace_mode':'debug','trace_file':'debug*c:\\dbg.log'}
            config = {'trace':True,'trace_mode':'debug|error','trace_file':'debug*c:\\dbg.log|error*c:\\err.log'}
    
            config = {'trace':True,'trace_mode':'all'}
            config = {'trace':True,'trace_mode':'all','trace_file':'all*c:\\all.log'}
    '''
    def __init__(self, **kwargs):
        '''
        ************************************************************
            Function    : __init__
            Arguments   : 1) **kwargs (tuple) : connection arguments
                                                { 'user': gbase,
                                                  'port': 5258  }
            Return      : 
            Description : Logging construct function
        ************************************************************
        '''
        self._modes = ('debug', 'error', 'sql', 'all')
        # log file size default 20MB.
        # log backup file real 5 files. [filename].1, [filename].2... 
        self._file_mode = 'a'
        self._file_maxBytes = 20971520
        self._file_backupCount = 4
        # log file path
        self._all_file = None
        self._debug_file = None
        self._error_file = None
        self._sql_file = None
        # log switch
        self._all_mode = False
        self._debug_mode = False
        self._error_mode = False
        self._sql_mode = False
        # logger object
        self._no_logger = logging.getLogger('not use')
        self._all_logger = None
        self._debug_logger = None
        self._error_logger = None
        self._sql_logger = None
        # set log file default value 
        self._setlogfile('debug')
        self._setlogfile('error')
        self._setlogfile('sql')
        
        if len(kwargs) > 0:
            self.parsecfg(**kwargs)
        
        self._setlogger()

    def close(self):
        '''
        ************************************************************
            Function    : close
            Arguments   : 
            Return      : 
            Description : Close logging and release resource.
        ************************************************************
        '''
        if self._no_logger is not None:
            self._no_logger = None
            
        if self._all_logger is not None:
            handle = self._all_logger.handlers.pop()
            self._all_logger.removeHandler(handle)
            self._all_logger = None
            
        if self._debug_logger is not None:
            handle = self._debug_logger.handlers.pop()
            self._debug_logger.removeHandler(handle)
            self._debug_logger = None
            
        if self._error_logger is not None:
            handle = self._error_logger.handlers.pop()
            self._error_logger.removeHandler(handle)
            self._error_logger = None
        
        if self._sql_logger is not None:
            handle = self.sql_logger_handle.handlers.pop()
            self._sql_logger.removeHandler(handle)
            self._sql_logger = None

    def parsecfg(self, **kwargs):
        '''
        ************************************************************
            Function    : parsecfg
            Arguments   : 1) **kwargs (tuple) : connection arguments
                                                { 'user': gbase, 
                                                  'port': 5258  } 
            Return      : 
            Description : (internal) Parse connection config file to member
        ************************************************************
        '''
        config = kwargs.copy()

        # trace
        trace = False
        if 'trace' in config:
            trace = config['trace']
            if not isinstance(trace, bool):
                trace = False
            
        if isinstance(trace, bool):
            if trace is not True:
                return
        else:
            raise BaseException('Please set right trace value True/False.')
        
        # trace_mode 
        if 'trace_mode' in config:
            modes = config['trace_mode']
            mode_arr = modes.split('|')
            for i in range(0, len(mode_arr)):
                if mode_arr[i] not in self._modes:
                    continue
                self._setmode(mode_arr[i])

        # trace_file
        if 'trace_file' in config:
            trace_file = config['trace_file']
            files = trace_file.split('|')
            for i in range(0, len(files)):
                mode_file = files[i].split('*')
                if len(mode_file) <> 2:
                    raise BaseException('Please use right delimiter \'*\'.')
                try:
                    if mode_file[1] is not None:
                        import os
                        if os.path.isabs(mode_file[1]) == False:
                            raise BaseException('Path is not valid.')
                        
                        self._setlogfile(mode_file[0], mode_file[1])
                except:
                    continue
                
        # trace_maxbytes
        if 'trace_maxbytes' in config:
            maxbytes = config['trace_maxbytes']
            if isinstance(maxbytes, int): 
                if maxbytes > 0:
                    self._file_maxBytes = maxbytes
            
        # trace_backupCount
        if 'trace_backupCount' in config:
            count = config['trace_backupCount']
            if isinstance(count, int):
                if count > 0:
                    self._file_backupCount = count

    def _setmode(self, mode):
        '''
        ************************************************************
            Function    : _setmode
            Arguments   : 1) mode (string) : 'debug','error','sql','all' 
            Return      : 
            Description : (internal) Open log mode function
        ************************************************************
        '''
        try:
            attribute = '_' + mode + '_mode'
            setattr(self, attribute, True)
        except AttributeError:
            raise attribute + ' is not exists'
        
        if self._all_mode == True:
            self._debug_mode = True
            self._error_mode = True
            self._sql_mode = True
    
    def _setlogfile(self, mode, filepath=None):
        '''
        ************************************************************
            Function    : _setlogfile
            Arguments   : 1) mode (string) : 'debug','error','sql','all' 
                          2) filepath (string) : log file path
            Return      : 
            Description : (internal) Set mode log file
        ************************************************************
        '''
        if mode not in self._modes:
            raise BaseException('Please set right logger mode.')
        
        if filepath is None:
            import platform
            system = platform.system()
            if system == 'Windows':
                filepath = "c:\gbase_python_%s.log" % mode
            elif system == 'Linux':
                filepath = "/tmp/gbase_python_%s.log" % mode
            else:
                raise BaseException('The platform is not support.')
        try:
            attribute = '_' + mode + '_file'
            setattr(self, attribute, filepath)
        except AttributeError:
            raise attribute + ' is not exists.'
    
    def _setlogger(self):
        '''
        ************************************************************
            Function    : _setlogger
            Arguments   : 
            Return      : 
            Description : (internal) Set Debug/Error/Sql logger
        ************************************************************
        '''
        if self._all_file is not None:
            allow = False
            if self._all_mode is True:
                allow = True
            elif self._debug_mode is True:
                allow = True
            elif self._error_mode is True:
                allow = True
            elif self._sql_mode is True:
                allow = True

            if allow is True:
                self._all_logger = self._getlogger('all')
        else:
            if self._debug_mode == True:
                self._debug_logger = self._getlogger('debug')
            if self._error_mode == True:
                self._error_logger = self._getlogger('error')
            if self._sql_mode == True:
                self._sql_logger = self._getlogger('sql')
        
    def _get_error_func(self):
        '''
        ************************************************************
            Function    : _get_error_func
            Arguments   :  
            Return      : logging.getLogger().error function pointer
            Description : Connection string need set error or all level.
                          { trace: True, 
                            trace_mode: 'error|all', 
                            trace_file: 'error*c:\\error.log' }
        ************************************************************
        '''
        if self._all_file is not None and self._error_mode is True:
            if self._all_logger is not None:
                return self._all_logger.error
        else:
            if self._error_mode is True or self._all_mode is True:
                if self._error_logger is not None:
                    return self._error_logger.error
                    
        return self._no_logger.debug
    
    def _get_debug_func(self):
        '''
        ************************************************************
            Function    : _get_debug_func
            Arguments   :
            Return      : logging.getLogger().debug function pointer 
            Description : Connection string need set debug or all level.
                          { trace: True,
                            trace_mode: 'debug|all',
                            trace_file: 'debug*c:\\debug.log' }
        ************************************************************
        '''
        if self._all_file is not None and self._debug_mode is True:
            if self._all_logger is not None:
                return self._all_logger.debug
        else:
            if self._debug_mode is True or self._all_mode is True:
                if self._debug_logger is not None:
                    return self._debug_logger.debug
                
        return self._no_logger.debug
                    
    def _get_sql_func(self):
        '''
        ************************************************************
            Function    : _get_sql_func
            Arguments   : 
            Return      : logging.getLogger().info function pointer
            Description : Connection string need set sql or all level.
                          { trace: True,
                            trace_mode: 'sql|all',
                            trace_file: 'sql*c:\\sql.log' }
        ************************************************************
        '''
        if self._all_file is not None and self._sql_mode is True:
            if self._all_logger is not None:
                return self._all_logger.info
        else:
            if self._sql_mode is True or self._all_mode is True:
                if self._sql_logger is not None:
                    return self._sql_logger.info
        
        return self._no_logger.debug
    
    debug = property(_get_debug_func)
    error = property(_get_error_func)
    sql = property(_get_sql_func)

    def _getlogger(self, mode):
        '''
        ************************************************************
            Function    : _getlogger
            Arguments   : 1) mode (string) : 'debug','error','sql','all'
            Return      : 
            Description : (internal) Get logger for mode
        ************************************************************
        '''
        from logging.handlers import RotatingFileHandler
        if mode not in self._modes:
            raise BaseException('Please set right logger mode.')
        
        logfile = getattr(self, '_' + mode + '_file')
        logger = logging.getLogger(mode)
        logger.setLevel(logging.DEBUG)

        handler = RotatingFileHandler(logfile,
                                      self._file_mode,
                                      self._file_maxBytes,
                                      self._file_backupCount)        
        fmt = ''
        datefmt = '%Y-%m-%d %H:%M:%S'
        if mode == 'debug' or mode == 'error' or mode == 'all':
            fmt = '[%(asctime)s] (%(funcName)s) File %(filename)s , line %(lineno)d. Thread %(threadName)s,%(thread)d <%(levelname)s> %(message)s'
        elif mode == 'sql':
            fmt = '[%(asctime)s] Thread %(threadName)s,%(thread)d %(message)s'
        
        formatter = logging.Formatter(fmt, datefmt)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
        return logger
