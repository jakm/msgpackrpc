# -*- coding: utf8 -*-

import msgpack
import socket
import threading
from collections import deque
from contextlib import contextmanager


MSGTYPE_REQUEST = 0
MSGTYPE_RESPONSE = 1
MSGTYPE_NOTIFICATION = 2
BUFSIZE = 1024


class MsgpackRPCError(Exception):
    pass


class ConnectionError(MsgpackRPCError):
    pass


class ConnectionTimeout(ConnectionError):
    pass


class SocketTimeout(ConnectionError):
    pass


class ProtocolError(MsgpackRPCError):
    pass


class SerializationError(MsgpackRPCError):
    pass


class ResponseError(MsgpackRPCError):
    pass


class Connection(object):
    def __init__(self, host, port, connect_timeout=None, socket_timeout=None,
                 encoding='utf-8'):
        self.host = host
        self.port = port
        self.connect_timeout = connect_timeout
        self.socket_timeout = socket_timeout

        self.packer = msgpack.Packer(encoding=encoding)
        self.unpacker = msgpack.Unpacker(encoding=encoding,
                                         unicode_errors='strict')

        self._socket = None
        self._next_msgid = 0

    def connect(self):
        if self.is_connected():
            raise ConnectionError('Already connected')

        try:
            self._socket = socket.create_connection((self.host, self.port),
                                                   self.connect_timeout)
        except socket.error as e:
            if isinstance(e, socket.timeout):
                raise ConnectionTimeout(str(e))
            else:
                raise ConnectionError(str(e))

        self._socket.settimeout(self.socket_timeout)

        return self

    def close(self):
        if not self.is_connected():
            return

        try:
            self._socket.close()
        except Exception:
            pass
        finally:
            self._socket = None

    def is_connected(self):
        return self._socket is not None

    def call(self, method, params):
        if not self.is_connected():
            raise ConnectionError('Not connected')

        msgid = self._get_next_msgid()
        request = (MSGTYPE_REQUEST, msgid, method, params)

        self._write_message(request)
        response = self._read_message()

        try:
            msgtype, recvd_msgid, error, result = response
        except ValueError:
            raise ProtocolError('Invalid response: "%s"' % response)

        if msgid != recvd_msgid:
            raise ProtocolError("Invalid response: received msgid doesn't match")

        if error is not None:
            raise ResponseError(error)

        return result

    def notify(self, method, params):
        raise NotImplementedError('Not yet')

    def _get_next_msgid(self):
        self._next_msgid += 1
        return self._next_msgid

    def _write_message(self, message):
        try:
            data = self.packer.pack(message)
        except Exception as e:
            raise SerializationError('Serialization failed: %s' % e)

        length = len(data)
        sent = 0
        err = None

        try:
            while sent < length:
                sent += self._socket.send(data[sent:])
        except socket.timeout as e:
            err = ConnectionTimeout(str(e))
        except socket.error as e:
            err = ConnectionError(str(e))

        if err != None:
            self.close()
            raise err

    def _read_message(self):
        err = None

        try:
            while True:
                data = self._socket.recv(BUFSIZE)
                self.unpacker.feed(data)
                for message in self.unpacker:
                    return message
        except socket.timeout as e:
            err = ConnectionTimeout(str(e))
        except socket.error as e:
            err = ConnectionError(str(e))

        if err != None:
            self.close()
            raise err


class ConnectionPool(object):
    def __init__(self, host, port, connect_timeout=None, socket_timeout=None,
                 encoding='utf-8', size=10):
        self.host = host
        self.port = port
        self.connect_timeout = connect_timeout
        self.socket_timeout = socket_timeout
        self.encoding = encoding
        self.size = size

        self._lock = threading.RLock()
        self._cond = threading.Condition(self._lock)
        self._connections = deque()

    def _new_connection(self):
        return Connection(self.host, self.port, self.connect_timeout,
                          self.socket_timeout, self.encoding)

    def connect(self):
        with self._lock:
            err = None

            for _ in range(8): # FIXME: attempts!!!!
                try:
                    for _ in range(self.size - len(self._connections)):
                        conn = self._new_connection()
                        self.put(conn.connect())
                except ConnectionError as e:
                    err = e

                if len(self._connections) == self.size:
                    break
            else:
                print err
                self.close() # FIXME: tady se to zamyka podruhe
                assert err is not None
                raise err

    def close(self):
        with self._lock:
            while len(self._connections):
                conn = self._connections.pop()
                conn.close()

    def get(self):
        with self._lock:
            if len(self._connections) > 0:
                return self._connections.pop()

            conn = self._new_connection()
            return conn.connect() # FIXME: attempts !!!

    def put(self, conn):
        with self._lock: # FIXME: tady se to zamyka podruhe
            if len(self._connections) < self.size:
                if conn.is_connected():
                    self._connections.appendleft(conn)
            else:
                conn.close()

    @contextmanager
    def get_connection(self):
        conn = self.get()
        try:
            yield conn
        finally:
            self.put(conn)
