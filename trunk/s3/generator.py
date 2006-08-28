import s3
import time
import urllib

class S3Generator:
    """
    Generator class
    
    Objects of this class are used for generating authenticated URLs for accessing
    Amazon's S3 service
    """
    DEFAULT_EXPIRES_IN = 60

    def __init__(self, pub_key, priv_key, secure=True, host=s3.DEFAULT_HOST, port=None):
        self._pub_key = pub_key
        self._priv_key = priv_key
        self._host = host
        if not port:
            self._port = PORTS_BY_SECURITY[secure]
        else:
            self._port = port
        self._secure = secure

        if not port:
            port = PORTS_BY_SECURITY[is_secure]

        if (secure):
            self.protocol = 'https'
        else:
            self.protocol = 'http'

        self.server_name = "%s:%d" % (host, port)
        self.__expires_in = RestGenerator.DEFAULT_EXPIRES_IN
        self.__expires = None

    def set_expires_in(self, expires_in):
        self.__expires_in = expires_in
        self.__expires = None

    def set_expires(self, expires):
        self.__expires = expires
        self.__expires_in = None

    def create_bucket(self, bucket, headers={}):
        return self.generate_url('PUT', bucket, headers)

    def list_bucket(self, bucket, options={}, headers={}):
        path = bucket
        if options:
            path += '?' + '&'.join(["%s=%s" % (param, urllib.quote_plus(options[param])) for param in options])

        return self.generate_url('GET', path, headers)

    def delete_bucket(self, bucket, headers={}):
        return self.generate_url('DELETE', bucket, headers)

    def put(self, bucket, key, object, headers={}):
        headers.update(object.get_meta_for_headers())
        return self.generate_url(
                'PUT',
                '%s/%s' % (bucket, urllib.quote_plus(key)), headers)

    def get(self, bucket, key, headers={}):
        return self.generate_url('GET', '%s/%s' % (bucket, urllib.quote_plus(key)), headers)

    def delete(self, bucket, key, headers={}):
        return self.generate_url('DELETE', '%s/%s' % (bucket, urllib.quote_plus(key)), headers)

    def get_bucket_acl(self, bucket, headers={}):
        return self.get_acl(bucket, '', headers)

    def get_acl(self, bucket, key='', headers={}):
        return self.generate_url('GET', '%s/%s?acl' % (bucket, urllib.quote_plus(key)), headers)

    def put_bucket_acl(self, bucket, acl_xml_document, headers={}):
        return self.put_acl(bucket, '', acl_xml_document, headers)

    # don't really care what the doc is here.
    def put_acl(self, bucket, key, acl_xml_document, headers={}):
        return self.generate_url('PUT', '%s/%s?acl' % (bucket, urllib.quote_plus(key)), headers)

    def list_all_my_buckets(self, headers={}):
        return self.generate_url('GET', '', headers)

    def make_bare_url(self, bucket, key=''):
        return self.protocol + '://' + self.server_name + '/' + bucket + '/' + key

    def _auth_header_value(self, method, path, headers):
        xamzs = [k for k in headers.keys() if k.startswith("x-amz-")]
        xamzs.sort()
        auth_parts = [method,
                      headers.get("Content-MD5", ""),
                      headers.get("Content-Type", ""),
                      headers.get("Date", ""),]
        auth_parts.extend([k + ":" + headers[k].strip() for k in xamzs])
        # hmmm fali mi ona perverzija za ?acl i ?torrent
        auth_parts.append(path)
        auth_str = "\n".join(auth_parts)
        auth_str = base64.encodestring(
            hmac.new(self._priv_key, auth_str, sha).digest()).strip()
        return "AWS %s:%s" % (self._pub_key, auth_str)

    def _headers(self, method, path, length=None, headers=None):
        if not headers:
            headers = {}
        headers["Date"] = strftime("%a, %d %b %Y %H:%M:%S GMT", gmtime())
        if length is not None:
            headers["Content-Length"] = length
        return urllib.quote_plus(self._auth_header_value(method, path, headers))
##        headers["Authorization"] = self._auth_header_value(method, path, headers)
##        return headers

    def _params(self, params):
        p = ''
        if params:
            p = '?' + urllib.urlencode(params)
        return p

    def _path(self, bucket=None, obj=None):
        if bucket is None:
            return "/"
        bucket = "/" + bucket
        if obj is None:
            return bucket
        return bucket + "/" + urllib.quote(obj)

    def _io_len(self, io):
        if hasattr(io, "len"):
            return io.len
        o_pos = io.tell()
        io.seek(0, 2)
        length = io.tell() - o_pos
        io.seek(o_pos, 0)
        return length

    def generate_url(self, method, path, headers):
        expires = 0
        if self.__expires_in != None:
            expires = int(time.time() + self.__expires_in)
        elif self.__expires != None:
            expires = int(self.__expires)
        else:
            raise "Invalid expires state"

        encoded_canonical = self._headers(method, path, headers=headers)

        if '?' in path:
            arg_div = '&'
        else:
            arg_div = '?'

        query_part = "Signature=%s&Expires=%d&AWSAccessKeyId=%s" % (encoded_canonical, expires, self._pub_key)

        return self.protocol + '://' + self.server_name + '/' + path  + arg_div + query_part


