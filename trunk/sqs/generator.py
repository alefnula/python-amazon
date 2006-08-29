import sqs
import re
import sha
import hmac
import time
import base64
import urllib


DEFAULT_CONTENT_TYPE = 'text/plain'
PORTS_BY_SECURITY = { True: 443, False: 80 }

DEFAULT_EXPIRES_IN = 60

class SQSGenerator:
    """
    Generator class
    
    Objects of this class are used for generating authenticated URLs for accessing
    Amazon's SQS service.
    """
    def __init__(self, pub_key, priv_key, host=DEFAULT_HOST, port=None, secure=True):
        self._pub_key = pub_key
        self._priv_key = priv_key
        self._host = host
        if not port:
            self._port = PORTS_BY_SECURITY[secure]
        else:
            self._port = port
        if (secure):
            self.protocol = 'https'
        else:
            self.protocol = 'http'
        self._secure = secure
        self.server_name = "%s:%d" % (self._host, self._port)
        self._expires_in = DEFAULT_EXPIRES_IN
        self._expires = None


    def set_expires_in(self, expires_in):
        """
        Set relative expiration time from the url creation moment.
        
        @param expires_in: Relative expiration time
        @type  expires_in: int
        """
        self._expires_in = expires_in
        self._expires = None


    def set_expires(self, expires):
        """
        Set absolute expiration time.
        
        @param expires: Absolute expiration time
        @type  expires: time.time()
        """
        self._expires = expires
        self._expires_in = None


    def _auth_header_value(self, method, path, headers):
        stripped_path = path.split('?')[0]
        # ...unless there is an acl parameter
        if re.search("[&?]acl($|=|&)", path):
            stipped_path += "?acl"
        auth_parts = [method,
                      headers.get("Content-MD5", ""),
                      headers.get("Content-Type", DEFAULT_CONTENT_TYPE),
                      headers.get("Date", time.strftime("%a, %d %b %Y %X GMT", time.gmtime())),
                      stipped_path]
        auth_str = "\n".join(auth_parts)
        auth_str = base64.encodestring(
            hmac.new(self._priv_key, auth_str, sha).digest()).strip()
        return urllib.quote_plus(auth_str)


    def _headers(self, method, path, length=None, headers=None, expires=None):
        if not headers:
            headers = {}
        if not headers.has_key('Date'):
            headers["Date"] = str(expires)
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
        return headers


    def _params(self, params, acl=False):
        p = ''
        if params:
            if acl:
                arg_div = '&'
            else:
                arg_div = '?'
            p = arg_div + urllib.urlencode(params)
        return p

    def _path(self, queue=None, message=None, acl=False):
        if queue is None:
            path = "/"
        else:
            path = '/' + queue
            if message is not None:
                path += '/' + message
        if acl:
            path += '?acl'
        return path



    def _io_len(self, io):
        if hasattr(io, "len"):
            return io.len
        o_pos = io.tell()
        io.seek(0, 2)
        length = io.tell() - o_pos
        io.seek(o_pos, 0)
        return length

    def _generate(self, queue=None, message=None, send_io=None, params=None, headers=None, acl=False):
        expires = 0
        if self._expires_in != None:
            expires = int(time.time() + self._expires_in)
        elif self._expires != None:
            expires = int(self._expires)
        path = self._path(queue, message, acl)
        length = None
        if isinstance(headers, dict) and headers.has_key("Content-Length"):
            length = headers["Content-Length"]
        elif send_io is not None:
            length = self._io_len(send_io)
        headers = self._headers(method, path, length=length, headers=headers, expires=expires)
        signature = self._auth_header_value(method, path, headers)
        path += self._params(params, acl)
        if '?' in path:
            arg_div = '&'
        else:
            arg_div = '?'
        query_part = "Signature=%s&Expires=%d&AWSAccessKeyId=%s&Version=%s&Timestamp=%s" % \
                     (signature, expires, self._pub_key, sqs.VERSION,
                      time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime()))

        return self.protocol + '://' + self.server_name + path + arg_div + query_part
