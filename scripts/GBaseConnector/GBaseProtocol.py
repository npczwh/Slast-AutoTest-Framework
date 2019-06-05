'''
Created on 2013-7-9

@author: lyh
'''

try:
    from hashlib import sha1
except ImportError:
    from sha import new as sha1
from GBaseUtils import *
from GBaseConstants import FieldFlag
import GBaseError

class GBaseProtocol(object):
    '''
        GBase communication protocol
    '''        
    def make_auth(self, user, passwd, seed, dbname = None, charset = 33, 
                  client_flags = 0, max_allowed_packet = 1073741824):
        '''
           Function    : make_auth
           Arguments   : 1) user
                         2) seed
                         3) paasswd = None
                         4) dbname  = None                       
                         5) charset = 33  utf8
                         6) client_flags = 0
                         7) max_allowed_packet = 1073741824
           Return      : auth pack [string]
           Description : create an authentication pack
        '''
        if not seed:
            raise GBaseError.ProgrammingError('Seed missing')

        auth = self._make_auth(user, passwd, seed, dbname)
        return int4store(client_flags) +\
               int4store(max_allowed_packet) +\
               int1store(charset) +\
               '\x00' * 23 + auth[0] + auth[1] + auth[2]
    
    def make_command(self, command, param = None):
        '''
           Function    : make_command
           Arguments   : 1) user
                         2) seed
                         3) paasswd = None
                         4) dbname  = None                       
                         5) charset = 33  utf8
                         6) client_flags = 0
                         7) max_allowed_packet = 1073741824
           Return      : auth pack [string]
           Description : create an command pack
        '''
        data = int1store(command)
        if param is not None:
            data += str(param)
        return data
    
    def parse_hello_res(self, packet):
        '''
           Function    : parse_hello_res
           Arguments   : packet   gbase server's hello packet 
           Return      : some message come from gbase server
           Description : parse gbase server's hello message
        '''
        res = {}
        (packet, res['protocol']) = read_int(packet[4:], 1)
        (packet, res['server_version_original']) = read_str(packet, end='\x00')
        (packet, res['server_threadid']) = read_int(packet, 4)
        (packet, res['scramble']) = read_bytes(packet, 8)
        packet = packet[1:] # Filler 1 * \x00
        (packet, res['capabilities']) = read_int(packet, 2)
        (packet, res['charset']) = read_int(packet, 1)
        (packet, res['server_status']) = read_int(packet, 2)
        packet = packet[13:] # Filler 13 * \x00
        (packet, scramble_next) = read_bytes(packet, 12)
        res['scramble'] += scramble_next
        return res
    
    def parse_ok_res(self, packet):
        '''
           Function    : parse_ok_res
           Arguments   : packet   gbase server's ok packet 
           Return      : ok message [dict]
           Description : parse gbase server's hello message
        '''
        if not self.is_ok(packet):
            raise GBaseError.InterfaceError(errmsg = "is not ok packet.")
        res = {}
        try:
            (packet, res['field_count']) = read_int(packet[4:], 1) 
            (packet, res['affected_rows']) = read_lc_int(packet)
            (packet, res['insert_id']) = read_lc_int(packet)
            (packet, res['server_status']) = read_int(packet, 2)
            (packet, res['warning_count']) = read_int(packet, 2)
            if packet:
                (packet, res['info_msg']) = read_lc_str(packet)
        except ValueError:
            raise GBaseError.InterfaceError("Failed parsing OK packet.")
        return res
    
    def parse_error_res(self, packet):
        '''
           Function    : parse_error_res
           Arguments   : packet   gbase server's error packet 
           Return      : error infomation [dict]
           Description : parse gbase server's error message
        '''
        res = {}
            
        if not self.is_error(packet):
            raise ValueError("Packet is not an error packet")
        try:
            packet = packet[5:]
            (packet, res['errno']) = read_int(packet, 2)
            if packet[0] != '#': # NO SQLState                
                res['errmsg'] = packet
                res['sqlstate'] = None
            else:
                (packet, res['sqlstate']) = read_bytes(packet[1:], 5)
                res['errmsg'] = packet
        except Exception, err:
            raise GBaseError.InterfaceError("Failed getting Error information (%r)" % err)        
        return res
    
    def parse_column_res(self, packet):
        '''
           Function    : parse_column_res
           Arguments   : packet   gbase server's column packet 
           Return      : column message [list]
           Description : parse gbase server's column message
        '''
        res = {}
        (packet, res['catalog']) = read_lc_str(packet[4:])
        (packet, res['db']) = read_lc_str(packet)
        (packet, res['table']) = read_lc_str(packet)
        (packet, res['org_table']) = read_lc_str(packet)
        (packet, res['name']) = read_lc_str(packet)
        (packet, res['org_name']) = read_lc_str(packet)
        packet = packet[1:] # filler 1 * \x00
        (packet, res['charset']) = read_int(packet, 2)
        (packet, res['length']) = read_int(packet, 4)
        (packet, res['type']) = read_int(packet, 1)
        (packet, res['flags']) = read_int(packet, 2)
        (packet, res['decimal']) = read_int(packet, 1)
        packet = packet[2:] # filler 2 * \x00

        return (
            res['name'],
            res['type'],
            None, # display_size
            None, # internal_size
            None, # precision
            None, # scale
            ~res['flags'] & FieldFlag.NOT_NULL, # null_ok
            res['flags'],
            )
    
    def parse_column_count(self, packet):
        '''
           Function    : parse_column_count
           Arguments   : packet   gbase server's column count packet 
           Return      : column count [number]
           Description : parse gbase server's column message
        '''
        return read_lc_int(packet[4:])[1]
    
    def parse_eof(self, packet):
        '''
           Function    : parse_eof
           Arguments   : packet   gbase server's eof packet 
           Return      : eof message [list]
           Description : parse gbase server's eof message
        '''
        res = {}
        if not self.is_eof(packet):
            raise ValueError("Packet is not an eof packet")
        try:
            packet = packet[5:]
            (packet, res['warning_count']) = read_int(packet, 2)
            (packet, res['server_flag']) = read_int(packet, 2)
        except ValueError:
            raise GBaseError.InterfaceError("Failed parsing eof packet.")
        return res
    
    def read_result_set(self, socket, count = 1):
        '''
           Function    : read_result_set
           Arguments   : socket   gbase socket 
           Return      : a tuple with 2 elements: a list with all rows and
                         the EOF packet.
           Description : parse gbase server's result set
        '''
        rows = []
        eof = None
        rowdata = None
        i = 0
        while True:
            if eof is not None:
                break
            if i == count:
                break
            packet = socket.receive_packet()
            if packet[0:3] == '\xff\xff\xff':
                data = packet[4:]
                packet = socket.receive_packet()
                while packet[0:3] == '\xff\xff\xff':
                    data += packet[4:]
                    packet = socket.receive_packet()
                if self.is_eof(packet):
                    eof = self.parse_eof(packet)
                else:
                    data += packet[4:]
                rowdata = read_lc_str_list(data)
            elif self.is_eof(packet):
                eof = self.parse_eof(packet)
                rowdata = None
            else:
                eof = None
                rowdata = read_lc_str_list(packet[4:])
            if eof is None and rowdata is not None:
                rows.append(rowdata)
            i += 1
        return (rows, eof)
    
    def is_error(self, packet):
        '''
           Function    : is_error
           Arguments   : packet   gbase server's packet 
           Return      : true or false
           Description : return true if packet is error packet
        '''
        if packet[4] == '\xff':
            return True
        return False
    
    def is_ok(self, packet):
        '''
           Function    : is_ok
           Arguments   : packet   gbase server's packet 
           Return      : true or false
           Description : return true if packet is ok packet
        '''
        if packet[4] == '\x00':
            return True
        return False
    
    def is_eof(self, packet):
        '''
           Function    : is_eof
           Arguments   : packet   gbase server's packet 
           Return      : true or false
           Description : return true if packet is eof packet
        '''
        if packet[4] == '\xfe':
            return True
        return False
    
    def _make_passwd(self, passwd, seed):
        hash4 = None
        try:
            hash1 = sha1(passwd).digest()
            hash2 = sha1(hash1).digest()
            hash3 = sha1(seed + hash2).digest()
            xored = [ unpack_int(h1) ^ unpack_int(h3) for (h1,h3) in zip(hash1, hash3) ]
            hash4 = struct.pack('20B', *xored)
        except Exception, err:
            raise GBaseError.InterfaceError('Failed make password; %s' % err)
        return hash4
    
    def _make_auth(self, user, passwd, seed, dbname = None):
        '''
           Function    : _make_auth
           Arguments   : user   gbase server's packet
                         passwd
                         dbname                         
                         seed
           Return      : [user, passwd, db]
           Description : return true if packet is eof packet
        '''
        if user is not None and len(user) > 0:
            if isinstance(user, unicode):
                user = user.encode('utf8')
            _username = user + '\x00'
        else:
            raise GBaseError.OperationalError('user cannot be None or empty.')
        
        if passwd is not None and len(passwd) > 0:
            if isinstance(passwd, unicode):
                passwd = passwd.encode('utf8')
            _password = int1store(20) + self._make_passwd(passwd, seed)
        else:
            _password = ''
            #raise GBaseError.OperationalError('passwd cannot be None or empty.')
        
        if dbname is not None and len(dbname) > 0:
            if isinstance(dbname, unicode):
                dbname = dbname.encode('utf8')
            _dbname = dbname + '\x00'
        else:
            _dbname = '\x00'
        
        return (_username, _password, _dbname)
