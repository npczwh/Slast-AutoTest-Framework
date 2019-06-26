'''
Created on 2013-7-9

@author: lyh
'''

import socket
import struct

import GBaseConnector.GBaseError
from GBaseConnector.GBaseConstants import MAX_PACKET_LENGTH, GBASE_PACKET_HEADER_LEN


class GBaseSocket(object):
    '''
        GBase socket communicating with GBase Server
    '''
    def __init__(self, host='127.0.0.1', port=5258, timeout=None):
        '''
        Constructor
        '''
        self._host = host
        self._port = port
        self._timeout = timeout
        self._gbase_socket = None
        self._packet_number = -1
        
        self.open_tcp()
    
    def send_data(self, data, pknr = None):
        '''
           Function    : send_packet
           Arguments   : data string    
           Return      : 
           Description : send data to gbase server
        '''
        if pknr is None:
            self.next_packet_number()
        else:
            self._packet_number = pknr
        packets = self._prepare_packet(data, self._packet_number)
        try:
            for packet in packets:
                self._gbase_socket.sendall(packet)
        except Exception, err:
            raise GBaseConnector.GBaseError.OperationalError(errmsg = str(err))
    
    def receive_packet(self):
        '''
           Function    : recive_packet
           Arguments   :     
           Return      : packets [string]
           Description : receive packets from gbase server
        '''
        packets = ''
        try:
            # read gbase packet's header
            packets = self._gbase_socket.recv(1)
            while len(packets) < GBASE_PACKET_HEADER_LEN:
                buff = self._gbase_socket.recv(1)
                if buff is None:
                    raise GBaseConnector.GBaseError.OperationalError(error = 2013)
                packets += buff

            #read the rest data
            [packet_len, self._packet_number] = self._parse_packet_header(packets)
            packets += self._get_packet_data(packet_len)
            return packets
        except Exception, err:
            raise GBaseConnector.GBaseError.OperationalError(errmsg = str(err))
    
    def open_tcp(self):
        '''
           Function    : open_tcp
           Arguments   :     
           Return      : 
           Description : create a tcp socket
        '''
        addrinfo = None
        try:
            addrinfos = socket.getaddrinfo(self._host, self._port, 0, socket.SOCK_STREAM)
            for info in addrinfos:
                if info[0] == socket.AF_INET6:
                    addrinfo = info
                    break
                elif info[0] == socket.AF_INET:
                    addrinfo = info
                    break   
            (family, socktype, proto, canonname, sockaddr) = addrinfo             
        except socket.gaierror, err:
            raise GBaseConnector.GBaseError.InterfaceError(errno=2003, values=(self._host, err[1]))
        
        try:
            self._gbase_socket = socket.socket(family, socktype, proto)
            self._gbase_socket.settimeout(self._timeout)
            self._gbase_socket.connect(sockaddr)
        except socket.gaierror, err:
            raise GBaseConnector.GBaseError.InterfaceError(errno=2003, values=(self._host, err[1]))
        except socket.error, err:
            try:
                msg = err.errno
                if msg is None:
                    msg = str(err)
            except AttributeError:
                msg = str(err)
            raise GBaseConnector.GBaseError.InterfaceError(
                errno=2003, values=(self._host, msg))
        except StandardError, err:
            raise GBaseConnector.GBaseError.InterfaceError(errmsg ='%s' % err)
        except:
            raise
    
    def close_tcp(self):
        '''
           Function    : close_tcp
           Arguments   :     
           Return      : 
           Description : close a tcp socket
        '''
        try:
            self._gbase_socket.close()
        except (socket.error, AttributeError):
            pass
    
    def _prepare_packet(self, data, packet_number):
        '''
           Function    : _prepare_packet
           Arguments   :     
           Return      : 
           Description : pack data into gbase packet
        '''        
        packets = []
        buflen = len(data)
        maxpktlen = MAX_PACKET_LENGTH
        while buflen > maxpktlen:
            packets.append('\xff\xff\xff' + struct.pack('<B', packet_number)
                        + data[:maxpktlen])
            data = data[maxpktlen:]
            buflen = len(data)
            packet_number = packet_number + 1
        packets.append(struct.pack('<I', buflen)[0:3]
                    + struct.pack('<B', packet_number) + data)
        return packets
    
    def next_packet_number(self):
        '''
           Function    : next_packet_number
           Arguments   :     
           Return      : 
           Description : next packet 's series number
        '''
        self._packet_number = self._packet_number + 1
        return self._packet_number

    def is_connected(self):
        '''
           Function    : is_connected
           Arguments   : 
           Return      : true or false
           Description : return true if socket is connected
        '''
        pass
    
    def _parse_packet_header(self, packets):
        '''
           Function    : _parse_packet_header
           Arguments   : packets  gbase packet's header
           Return      : [packet_len, packet_series_number] list
           Description : parse gbase packet's header, 
                         return the length of gbase packet and the packet's series number. 
        '''
        if len(packets) < GBASE_PACKET_HEADER_LEN:
            raise GBaseConnector.GBaseError.OperationalError(error = 2013)
        
        packet_number = ord(packets[3])
        packet_len = struct.unpack("<I", packets[0:3] + '\x00')[0] + GBASE_PACKET_HEADER_LEN
        return [packet_len, packet_number]
    
    def _get_packet_data(self, packet_len):
        '''
           Function    : _get_packet_data
           Arguments   : packet_len        the length of packet
           Return      : data string
           Description : get the data of packet 
        '''
        data = ''
        rest_len = packet_len - GBASE_PACKET_HEADER_LEN
        while rest_len > 0:
            buff = self._gbase_socket.recv(rest_len)
            if not buff:
                raise GBaseConnector.GBaseError.InterfaceError(errno=2013)
            data += buff
            rest_len = rest_len - len(buff)
        return data

if __name__ == '__main__':
    pass
