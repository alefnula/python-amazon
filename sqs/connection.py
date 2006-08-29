import re
import sha
import hmac
import time
import base64
import socket
import urllib
import httplib

import sqs
from sqs.parsers import parseError


DEFAULT_CONTENT_TYPE = 'text/plain'
PORTS_BY_SECURITY = { True: 443, False: 80 }

class SQSConnection(object):
    """SQS Connection object.
    You shoud never use this object directly. User SQSService instead.
    """
    def __init__(self, pub_key, priv_key, host=sqs.DEFAULT_HOST, port=None, secure=True, debug=0):
        self._pub_key = pub_key
        self._priv_key = priv_key
        self._host = host
        if not port:
            self._port = PORTS_BY_SECURITY[secure]
        else:
            self._port = port
        self._secure = secure
        if (secure):
            self._conn = httplib.HTTPSConnection("%s:%d" % (self._host, self._port))
        else:
            self._conn = httplib.HTTPConnection("%s:%d" % (self._host, self._port))
        self._set_debug(debug)


    def _set_debug(self, debug):
        self._debug = debug
        self._conn.set_debuglevel(debug)


    def clone(self):
        """C.clone() -> new connection to sqs"""
        return SQSConnection(self._pub_key, self._priv_key, self._host, self._port, self._secure, self._debug)


    def _auth_header_value(self, method, path, headers):
        path = path.split('?')[0]
        # ...unless there is an acl parameter
        if re.search("[&?]acl($|=|&)", path):
            path += "?acl"
        auth_parts = [method,
                      headers.get("Content-MD5", ""),
                      headers.get("Content-Type", DEFAULT_CONTENT_TYPE),
                      headers.get("Date", time.strftime("%a, %d %b %Y %X GMT", time.gmtime())),
                      path]
        auth_str = "\n".join(auth_parts)
        auth_str = base64.encodestring(
            hmac.new(self._priv_key, auth_str, sha).digest()).strip()
        return "AWS %s:%s" % (self._pub_key, auth_str)


    def _headers(self, method, path, length=None, headers=None):
        if not headers:
            headers = {}
        if not headers.has_key('Date'):
            headers["Date"] = time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime())
        if not headers.has_key('AWS-Version'):
            headers['AWS-Version'] = sqs.VERSION
        if not headers.has_key('Content-Type'):
            headers['Content-Type'] = DEFAULT_CONTENT_TYPE
        if not headers.has_key('Content-MD5'):
            headers['Content-MD5'] = ''
        if not headers.has_key('Content-Length'):
            if length is not None:
                headers['Content-Length'] = length
            else:
                headers['Content-Length'] = 0
        headers["Authorization"] = self._auth_header_value(method, path, headers)
        return headers

    def _params(self, params):
        p = ''
        if params:
            p = '?' + urllib.urlencode(params)
        return p

    def _path(self, queue=None, message=None):
        if queue is None:
            return "/"
        queue = queue
        if message is None:
            return queue
        return queue + "/" + message

    def _io_len(self, io):
        if hasattr(io, "len"):
            return io.len
        o_pos = io.tell()
        io.seek(0, 2)
        length = io.tell() - o_pos
        io.seek(o_pos, 0)
        return length


    def __getattr__(self, attr):
        method = attr.upper()
        def f(queue=None, message=None, send_io=None, params=None, headers=None):
            path = self._path(queue, message)
            length = None
            if isinstance(headers, dict) and headers.has_key("Content-Length"):
                length = headers["Content-Length"]
            elif send_io is not None:
                length = self._io_len(send_io)

            headers = self._headers(method, path, length=length, headers=headers)

            def do_conn():
                self._conn.putrequest(method, path + self._params(params))
                for k,v in headers.items():
                    self._conn.putheader(k, v)
                self._conn.endheaders()

            retry = False
            try:
                do_conn()
            except socket.error, e:
                 # if broken pipe (timed out/server closed connection)
                 # open new connection and try again
                if e[0] == 32:
                    retry = True
            if retry:
                do_conn()
                
            if send_io is not None:
                data = send_io.read(httplib.MAXAMOUNT)
                while len(data) > 0:
                    self._conn.send(data)
                    data = send_io.read(httplib.MAXAMOUNT)
                send_io.read() # seems to be needed to finish the response
            try:
                r = self._conn.getresponse()
            except httplib.ResponseNotReady, e:
                e.args += ('You are probably overlapping SQS ops',)
                raise e
            if r.status < 200 or r.status > 299:
                raise parseError(r.read())
            return r
        return f
