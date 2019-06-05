'''
Created on 2013-7-17

@author: lyh
'''
import struct
import datetime
import time
from decimal import Decimal
from GBaseConnector.GBaseConstants import FieldType, FieldFlag

class BaseTypeConvert(object):
    def __init__(self, charset='utf8', use_unicode=True):
        self.python_types = None
        self.gbase_types = None
        self.set_charset(charset)
        self.set_unicode(use_unicode)
        
    def set_charset(self, charset):
        if charset is not None:
            self.charset = charset
        else:
            # default to utf8
            self.charset = 'utf8'
    
    def set_unicode(self, value=True):
        self.use_unicode = value
    
    def to_gbase(self, value):
        return value
    
    def to_python(self, vtype, value):
        return value
    
    def escape(self, buf):
        return buf
    
    def quote(self, buf):
        return str(buf)

class GBaseConvert(BaseTypeConvert):
    def __init__(self, charset=None, use_unicode=True):
        BaseTypeConvert.__init__(self, charset, use_unicode)
    
    def escape(self, value):
        """
        Escapes special characters as they are expected to by when GBase
        receives them.
        Returns the value if not a string, or the escaped string.
        """
        if value is None:
            return value
        elif isinstance(value, (int,float,long,Decimal)):
            return value
        res = value
        res = res.replace('\\','\\\\')
        res = res.replace('\n','\\n')
        res = res.replace('\r','\\r')
        res = res.replace('\047','\134\047') # single quotes
        res = res.replace('\042','\134\042') # double quotes
        res = res.replace('\032','\134\032') # for Win32
        return res
    
    def quote(self, buf):
        """
        Quote the parameters for commands. General rules:
          o numbers are returns as str type (because operation expect it)
          o None is returned as str('NULL')
          o String are quoted with single quotes '<string>'
        
        Returns a string.
        """
        if isinstance(buf, (int,float,long,Decimal)):
            return str(buf)
        elif isinstance(buf, type(None)):
            return "NULL"
        else:
            # Anything else would be a string
            return "'%s'" % buf 
    
    def to_gbase(self, value):
        type_name = value.__class__.__name__.lower()
        return getattr(self, "_%s_to_gbase" % str(type_name))(value)
    
    def _int_to_gbase(self, value):
        return int(value)
    
    def _long_to_gbase(self, value):
        return long(value)
    
    def _float_to_gbase(self, value):
        return float(value)
    
    def _str_to_gbase(self, value):
        return str(value)

    def _unicode_to_gbase(self, value):
        """
        Encodes value, a Python unicode string, to whatever the
        character set for this converter is set too.
        """
        return value.encode(self.charset)
    
    def _bool_to_gbase(self, value):
        if value:
            return 1
        else:
            return 0
        
    def _nonetype_to_gbase(self, value):
        """
        This would return what None would be in GBase, but instead we
        leave it None and return it right away. The actual conversion
        from None to NULL happens in the quoting functionality.
        
        Return None.
        """
        return None
        
    def _datetime_to_gbase(self, value):
        """
        Converts a datetime instance to a string suitable for GBase.
        The returned string has format: %Y-%m-%d %H:%M:%S[.%f]
        
        If the instance isn't a datetime.datetime type, it return None.
        
        Returns a string.
        """
        if value.microsecond:
            return '%d-%02d-%02d %02d:%02d:%02d.%06d' % (
                value.year, value.month, value.day,
                value.hour, value.minute, value.second,
                value.microsecond)
        return '%d-%02d-%02d %02d:%02d:%02d' % (
            value.year, value.month, value.day,
            value.hour, value.minute, value.second)
    
    def _date_to_gbase(self, value):
        """
        Converts a date instance to a string suitable for GBase.
        The returned string has format: %Y-%m-%d
        
        If the instance isn't a datetime.date type, it return None.
        
        Returns a string.
        """
        return '%d-%02d-%02d' % (value.year, value.month, value.day)
    
    def _time_to_gbase(self, value):
        """
        Converts a time instance to a string suitable for GBase.
        The returned string has format: %H:%M:%S[.%f]
        
        If the instance isn't a datetime.time type, it return None.
        
        Returns a string or None when not valid.
        """
        if value.microsecond:
            return value.strftime('%H:%M:%S.%%06d') % value.microsecond
        return value.strftime('%H:%M:%S')
    
    def _struct_time_to_gbase(self, value):
        """
        Converts a time.struct_time sequence to a string suitable
        for GBase.
        The returned string has format: %Y-%m-%d %H:%M:%S
        
        Returns a string or None when not valid.
        """
        return time.strftime('%Y-%m-%d %H:%M:%S', value)
        
    def _timedelta_to_gbase(self, value):
        """
        Converts a timedelta instance to a string suitable for GBase.
        The returned string has format: %H:%M:%S

        Returns a string.
        """
        (hours, r) = divmod(value.seconds, 3600)
        (mins, secs) = divmod(r, 60)
        hours = hours + (value.days * 24)
        if value.microseconds:
            return '%02d:%02d:%02d.%06d' % (hours, mins, secs,
                                            value.microseconds)
        return '%02d:%02d:%02d' % (hours, mins, secs)
    
    def _decimal_to_gbase(self, value):
        """
        Converts a decimal.Decimal instance to a string suitable for
        GBase.
        
        Returns a string or None when not valid.
        """
        if isinstance(value, Decimal):
            return str(value)
        
        return None
         
    def to_python(self, flddsc, value):
        """
        Converts a given value coming from GBase to a certain type in Python.
        The flddsc contains additional information for the field in the
        table. It's an element from GBaseCursor.description.
        
        Returns a mixed value.
        """
        res = value
        
        if value == '\x00' and flddsc[1] != FieldType.BIT:
            # Don't go further when we hit a NULL value
            return None
        if value is None:
            return None
            
        type_name = FieldType.get_info(flddsc[1])
        try:
            return getattr(self, '_%s_to_python' % type_name)(value, flddsc)
        except KeyError:
            # If one type is not defined, we just return the value as str
            return str(value)
        except ValueError, e:
            raise ValueError, "%s (field %s)" % (e, flddsc[0])
        except TypeError, e:
            raise TypeError, "%s (field %s)" % (e, flddsc[0])
        except:
            raise
    
    def _FLOAT_to_python(self, v, desc=None):
        """
        Returns v as float type.
        """
        return float(v)
    _DOUBLE_to_python = _FLOAT_to_python
    
    def _INT_to_python(self, v, desc=None):
        """
        Returns v as int type.
        """
        return int(v)
    _TINY_to_python = _INT_to_python
    _SHORT_to_python = _INT_to_python
    _INT24_to_python = _INT_to_python
    
    def _LONG_to_python(self, v, desc=None):
        """
        Returns v as long type.
        """
        return int(v)
    _LONGLONG_to_python = _LONG_to_python
    
    def _DECIMAL_to_python(self, v, desc=None):
        """
        Returns v as a decimal.Decimal.
        """
        return Decimal(v)
    _NEWDECIMAL_to_python = _DECIMAL_to_python
        
    def _str(self, v, desc=None):
        """
        Returns v as str type.
        """
        return str(v)
    
    def _BIT_to_python(self, v, dsc=None):
        """Returns BIT columntype as integer"""
        s = v
        if len(s) < 8:
            s = '\x00'*(8-len(s)) + s
        return struct.unpack('>Q', s)[0]
    
    def _DATE_to_python(self, v, dsc=None):
        """
        Returns DATE column type as datetime.date type.
        """
        pv = None
        try:
            pv = datetime.date(*[ int(s) for s in v.split('-')])
        except ValueError:
            return None
        else:
            return pv
    _NEWDATE_to_python = _DATE_to_python
            
    def _TIME_to_python(self, v, dsc=None):
        """
        Returns TIME column type as datetime.time type.
        """
        pv = None
        try:
            (hms, fs) = v.split('.')
            fs = int(fs.ljust(6, '0'))
        except ValueError:
            hms = v
            fs = 0
        try:
            (h, m, s) = [ int(d) for d in hms.split(':')]
            pv = datetime.timedelta(hours=h, minutes=m, seconds=s,
                                    microseconds=fs)
        except ValueError, err:
            raise ValueError(
                "Could not convert %s to python datetime.timedelta" % v)
        else:
            return pv
            
    def _DATETIME_to_python(self, v, dsc=None):
        """
        Returns DATETIME column type as datetime.datetime type.
        """
        pv = None
        try:
            (sd, st) = v.split(' ')
            if len(st) > 8:
                (hms, fs) = st.split('.')
                fs = int(fs.ljust(6, '0'))
            else:
                hms = st
                fs = 0
            dt = [ int(v) for v in sd.split('-') ] +\
                 [ int(v) for v in hms.split(':') ] + [fs,]
            pv = datetime.datetime(*dt)
        except ValueError:
            pv = None
        
        return pv
    _TIMESTAMP_to_python = _DATETIME_to_python
    
    def _YEAR_to_python(self, v, desc=None):
        """Returns YEAR column type as integer"""
        try:
            year = int(v)
        except ValueError:
            raise ValueError("Failed converting YEAR to int (%s)" % v)
        
        return year

    def _SET_to_python(self, v, dsc=None):
        """Returns SET column typs as set
        
        Actually, GBase protocol sees a SET as a string type field. So this
        code isn't called directly, but used by STRING_to_python() method.
        
        Returns SET column type as a set.
        """
        pv = None
        try:
            pv = set(v.split(','))
        except ValueError:
            raise ValueError, "Could not convert SET %s to a set." % v
        return pv

    def _STRING_to_python(self, v, dsc=None):
        """
        Note that a SET is a string too, but using the FieldFlag we can see
        whether we have to split it.
        
        Returns string typed columns as string type.
        """
        if dsc is not None:
            # Check if we deal with a SET
            if dsc[7] & FieldFlag.SET:
                return self._SET_to_python(v, dsc)
            if dsc[7] & FieldFlag.BINARY:
                return v
        
        if self.use_unicode:
            try:
                return unicode(v, self.charset)
            except:
                raise
        return str(v)
    _VAR_STRING_to_python = _STRING_to_python

    def _BLOB_to_python(self, v, dsc=None):
        if dsc is not None:
            if dsc[7] & FieldFlag.BINARY:
                return v
        
        return self._STRING_to_python(v, dsc)
    _LONG_BLOB_to_python = _BLOB_to_python
    _MEDIUM_BLOB_to_python = _BLOB_to_python
    _TINY_BLOB_to_python = _BLOB_to_python


def unpack_int(data):
    try:
        if isinstance(data, int):
            return data
        data_len = len(data)
        if data_len == 1:
            return int(ord(data))
        if data_len <= 4:
            tdata = data + '\x00' * (4 - data_len)
            return struct.unpack('<I', tdata)[0]
        else:
            tdata = data + '\x00' * (8 - data_len)
            return struct.unpack('<Q', tdata)[0]
    except:
        raise

def read_bytes(data, size):
    res = data[0:size]
    return (data[size:], res)    

def read_int(data, size = None):            
    try:
        tdata = data[0:size]
        tres = unpack_int(tdata)
    except:
        raise
    return (data[size:], tres)

def read_str(data, end = None, size = None):  
    if end is not None:
        try:
            idx = data.index(end)
        except (ValueError), err:
            raise ValueError("end byte not precent in buffer. (%s) " % str(err))
        return (data[idx+1:], data[0:idx])
    elif size is not None:
        return read_bytes(data,size)
    else:
        raise ValueError("read_str needs either end or size")

def int1store(i):  
    if i < 0 or i > 255:
        raise ValueError('int1store requires 0 <= i <= 255')
    else:
        return struct.pack('<B',i)

def int2store(i):
    if i < 0 or i > 65535:
        raise ValueError('int2store requires 0 <= i <= 65535')
    else:
        return struct.pack('<H',i)

def int3store(i):
    if i < 0 or i > 16777215:
        raise ValueError('int3store requires 0 <= i <= 16777215')
    else:
        return struct.pack('<I',i)[0:3]

def int4store(i):
    if i < 0 or i > 4294967295L:
        raise ValueError('int4store requires 0 <= i <= 4294967295')
    else:
        return struct.pack('<I',i)

def int8store(i):
    if i < 0 or i > 18446744073709551616L:
        raise ValueError('int4store requires 0 <= i <= 2^64')
    else:
        return struct.pack('<Q',i)

def intstore(i):
    if i < 0 or i > 18446744073709551616:
        raise ValueError('intstore requires 0 <= i <= 2^64')
        
    if i <= 255:
        fs = int1store
    elif i <= 65535:
        fs = int2store
    elif i <= 16777215:
        fs = int3store
    elif i <= 4294967295L:
        fs = int4store
    else:
        fs = int8store
        
    return fs(i)

def read_lc_int(buff):
    if len(buff) == 0:
        raise ValueError("Empty buffer.")
    
    (buff,s) = read_int(buff,1)
    if s == 251:
        return (buff,None)
    elif s == 252:
        (buff,i) = read_int(buff,2)
    elif s == 253:
        (buff,i) = read_int(buff,3)
    elif s == 254:
        (buff,i) = read_int(buff,8)
    else:
        i = s
    
    return (buff, int(i))

def read_lc_str(buf):
    if buf[0] == '\xfb':
        return (buf[1:], None)
        
    l = lsize = 0
    fst = ord(buf[0])
    
    if fst <= 250:
        l = fst
        return (buf[1+l:], buf[1:l+1])
    elif fst == 252:
        lsize = 2
    elif fst == 253:
        lsize = 3
    if fst == 254:
        lsize = 8
    
    l = unpack_int(buf[1:lsize+1])
    return (buf[lsize+l+1:], buf[lsize+1:l+lsize+1])

def read_lc_str_list(buf):
    """
    """
    strlst = []    
    while buf:
        if buf[0] == '\xfb':
            strlst.append(None)
            buf = buf[1:]
            continue

        l = lsize = 0
        fst = ord(buf[0])

        if fst <= 250:
            l = fst
            strlst.append(buf[1:l+1])
            buf = buf[1+l:]
            continue
        elif fst == 252:
            lsize = 2
        elif fst == 253:
            lsize = 3
        if fst == 254:
            lsize = 8

        l = unpack_int(buf[1:lsize+1])
        strlst.append(buf[lsize+1:l+lsize+1])
        buf = buf[lsize+l+1:]
    return tuple(strlst)