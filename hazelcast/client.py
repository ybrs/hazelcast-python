import sys
import socket
import time
import os
import re

try:
    import cPickle as pickle
except ImportError:
    import pickle

_DEAD_RETRY = 30  # number of seconds before retrying a dead server.
_SOCKET_TIMEOUT = 3  #  number of seconds before sockets timeout.

class _ConnectionDeadError(Exception):
    pass

class HCHost(object):
    ''' this is the server, node '''

    def __init__(self, host, debug=0, dead_retry=_DEAD_RETRY,
                 socket_timeout=_SOCKET_TIMEOUT):
        self.dead_retry = dead_retry
        self.socket_timeout = socket_timeout
        self.debug = debug
        if isinstance(host, tuple):
            host, self.weight = host
        else:
            self.weight = 1

        #  parse the connection string
        m = re.match(r'^(?P<proto>unix):(?P<path>.*)$', host)
        if not m:
            m = re.match(r'^(?P<proto>inet):'
                    r'(?P<host>[^:]+)(:(?P<port>[0-9]+))?$', host)
        if not m: m = re.match(r'^(?P<host>[^:]+)(:(?P<port>[0-9]+))?$', host)
        if not m:
            raise ValueError('Unable to parse connection string: "%s"' % host)

        hostData = m.groupdict()
        if hostData.get('proto') == 'unix':
            self.family = socket.AF_UNIX
            self.address = hostData['path']
        else:
            self.family = socket.AF_INET
            self.ip = hostData['host']
            self.port = int(hostData.get('port', 11211))
            self.address = ( self.ip, self.port )

        self.deaduntil = 0
        self.socket = None

        self.buffer = ''

    def debuglog(self, str):
        if self.debug:
            sys.stderr.write("HazelCastClient: %s\n" % str)

    def _check_dead(self):
        if self.deaduntil and self.deaduntil > time.time():
            return 1
        self.deaduntil = 0
        return 0

    def connect(self):
        if self._get_socket():
            return 1
        return 0

    def mark_dead(self, reason):
        self.debuglog("HazelCastClient: %s: %s.  Marking dead." % (self, reason))
        self.deaduntil = time.time() + self.dead_retry
        self.close_socket()

    def _get_socket(self):
        if self._check_dead():
            return None
        if self.socket:
            return self.socket
        s = socket.socket(self.family, socket.SOCK_STREAM)
        if hasattr(s, 'settimeout'): s.settimeout(self.socket_timeout)
        try:
            s.connect(self.address)
        except socket.timeout, msg:
            self.mark_dead("connect: %s" % msg)
            return None
        except socket.error, msg:
            if isinstance(msg, tuple): msg = msg[1]
            self.mark_dead("connect: %s" % msg[1])
            return None
        self.socket = s
        self.buffer = ''
        return s

    def close_socket(self):
        if self.socket:
            self.socket.close()
            self.socket = None

    def send_cmd(self, cmd):
        self.socket.sendall(cmd + '\r\n')

    def send_cmds(self, cmds):
        """ cmds already has trailing \r\n's applied """
        self.socket.sendall(cmds)

    def readline(self):
        buf = self.buffer
        recv = self.socket.recv
        while True:
            index = buf.find('\r\n')
            if index >= 0:
                break
            data = recv(4096)
            if not data:
                # connection close, let's kill it and raise
                self.close_socket()
                raise _ConnectionDeadError()

            buf += data
        self.buffer = buf[index+2:]
        return buf[:index]

    def read_response(self):
        header = self.readline()
        if self.debug:
            print "**** server ****", header
        pos = header.find('#')
        if pos == -1:
            if ('OK' in header):
                return header
            else:
                print "[", header, "]"
                raise Exception(header)
        else:
            argscnt = int(header[pos+1:])            
            if not argscnt:
                return header
            arglengths = self.readline().split(' ')        
            ret = []
            for i in arglengths:
                ret.append(self.recv(int(i)))
            # just read the last \r\n
            self.readline()
            return ret

    def expect(self, text):
        line = self.readline()
        if line != text:
            self.debuglog("while expecting '%s', got unexpected response '%s'"
                    % (text, line))
        return line

    def recv(self, rlen):
        self_socket_recv = self.socket.recv
        buf = self.buffer
        while len(buf) < rlen:
            foo = self_socket_recv(max(rlen - len(buf), 4096))
            buf += foo
            if not foo:
                raise _Error( 'Read %d bytes, expecting %d, '
                        'read returned 0 length bytes' % ( len(buf), rlen ))
        self.buffer = buf[rlen:]
        return buf[:rlen]

    def auth(self, user='dev', passwd='dev-pass'):
        self.socket.send('P01\r\n')        
        self.socket.sendall('AUTH %s %s #0\r\n' % (user, passwd))
        return self.readline()

    def cmd(self, command, commandargs, *args):
        '''
        hc protocol documentation is at, https://github.com/hazelcast/hazelcast/blob/clientprotocol/hazelcast-documentation/src/main/docbook/manual/content/client/ClientProtocol.xml
        MPUT myMap 0 #2 <- command commandargs #<numberof data args>
        5 7 <- args lengths
        myKeymyValue <- args data
        '''
        buf = '%s %s #%s\r\n' % (command, commandargs, len(args))        
        if not args:
            self.socket.sendall(buf)
            if self.debug:
                print "*** client ***"
                print buf
                print "*** // client ***"
            return self.read_response()
        lengths = []
        for i in args:
            lengths.append(str(len(i)))
        buf += '%s\r\n' % (' '.join(lengths))                        
        for i in args:
            buf += i
        if self.debug:
            print "*** client ***"
            print buf
            print "*** // client ***"
        self.socket.sendall(buf)
        return self.read_response()

    def __str__(self):
        d = ''
        if self.deaduntil:
            d = " (dead until %d)" % self.deaduntil

        if self.family == socket.AF_INET:
            return "inet:%s:%d%s" % (self.address[0], self.address[1], d)
        else:
            return "unix:%s%s" % (self.address, d)


class HazelCast(object):
    ''' this is for the cluster '''
    def __init__(self, hosts, user='dev', passwd='dev-pass', debug=0, dead_retry=_DEAD_RETRY,
                 socket_timeout=_SOCKET_TIMEOUT):        
        self.nodes = []
        for host in hosts:
            node = HCHost(host,debug=0, dead_retry=_DEAD_RETRY, socket_timeout=_SOCKET_TIMEOUT)
            self.nodes.append(node)
            node.connect()
            node.auth(user=user, passwd=passwd)

    def cmd(self, command, commandargs, *args):
        ''' TODO: first node is the best for now, but should fallback to second node if the first node is dead etc.. '''
        return self.nodes[0].cmd(command, commandargs, *args)

    def members(self):
        ''' returns cluster members         
        hc.members()
        >>> ['192.168.2.130:5702', '192.168.2.130:5701']
        '''
        return self.cmd('MEMBERS', '').split('OK ')[1].split(' ')
    
    def ping(self):
        ''' pings cluster
        hc.ping()
        >>> True
        '''
        return self.cmd('PING', '')

    ''' map commands
    MGET, MGETALL, MPUT, MTRYPUT, MSET, MPUTTRANSIENT, MPUTANDUNLOCK, MREMOVE, MREMOVEITEM,
    MCONTAINSKEY, MCONTAINSVALUE, ADDLISTENER, EVENT, REMOVELISTENER, KEYSET, ENTRYSET, MGETENTRY, MLOCK, MISKEYLOCKED,
    MUNLOCK, MLOCKMAP, MUNLOCKMAP, MFORCEUNLOCK, MPUTALL, MPUTIFABSENT, MREMOVEIFSAME, MREPLACEIFNOTNULL, MREPLACEIFSAME,
    MTRYLOCKANDGET, MFLUSH, MEVICT, MADDLISTENER, MREMOVELISTENER
    '''

    def mput(self, mapname, key, value):
        '''
            Associates the specified value with the specified key in the map.
            If the map previously contained a mapping for this key, the old value is replaced by the specified value.
            The operation will return the old value.        
        '''
        return self.cmd('MPUT', mapname, key, value)

    def mget(self, mapname, key):
        '''
            Returns the value to which this map maps the specified key. Returns null if the map contains no mapping for
            this key        
        '''
        return self.cmd('MGET', mapname, key)

    def mgetall(self, mapname, keys):
        '''
        Returns the entries for the given keys.
        @param keys array
        @return map ok key values
        '''        
        l = self.cmd('MGETALL', mapname, *keys)
        ret = {}
        for i in range(0, len(l), 2):
            ret[l[i]] = l[i+1]
        return ret

        
def hazelcast(hosts=None, user='dev', passwd='dev-pass', debug=0, dead_retry=_DEAD_RETRY,
                 socket_timeout=_SOCKET_TIMEOUT):  
    if not hosts:
        hosts=['127.0.0.1:5701']
    return HazelCast(hosts, user='dev', passwd='dev-pass', debug=0, dead_retry=_DEAD_RETRY,
                 socket_timeout=_SOCKET_TIMEOUT)

if __name__ == "__main__":
    hc = HazelCast(hosts=['127.0.0.1:5701'])
    print hc.members()
    # print hc.ping()
    print hc.mput('mymap', 'foo', 'bar')
    print hc.mput('mymap', 'foo2', 'bar2')
    print hc.mget('mymap', 'foo')    
    print hc.mgetall('mymap', ['foo', 'foo3', 'foo2'])

    # node = HCHost(host='127.0.0.1:5701', debug=0)
    # node.connect()
    # node.auth()    
    # print node.cmd('MPUT', 'mymapx', 'foo', 'bar')
    # print node.cmd('MGETALL', 'mymapx', 'foo')
    # print node.cmd('LADD', 'mylist', 'foo')
    # print node.cmd('NEWID', 'foo')
    # print node.cmd('MEMBERS', '')
    # node.connect()
    # node.send_cmd('auth')    
    # print node.readline()