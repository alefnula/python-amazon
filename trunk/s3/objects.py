from StringIO import StringIO

class S3Object(object):
    def __init__(self, key, data, metadata):
        self.key = key
        self.data = data
        self.metadata = {}
        for key in metadata:
            self.metadata[key.lower()] = metadata[key]



class S3Bucket(object):
    """S3Bucket(name, s3c) -> a new S3 Bucket connection"""
    def __init__(self, name, s3c):
        self._name = name
        self._s3c = s3c
        class S3ConnectionBucket:
            def __getattr__(self, attr):
                def f(obj=None, send_io=None, params=None,
                      headers=None, *args):
                    return getattr(s3c, attr)(name, obj, send_io=send_io,
                                              params=params, headers=headers,
                                              *args)
                return f
        self._s3cb = S3ConnectionBucket()

    def __str__(self):
        return self._name
    
    def __repr__(self):
        return self._name

    def list(self, **params):
        """B.list(**params) -- list bucket objects, returns full datastructure

        accepted params: prefix, marker, max-keys, delimiter
        """
##        return xml_decode(self._s3cb.get(params=params))

    def keys(self, **params):
        """B.keys(**params) -- list bucket's objects, returns flat list of object keys
        
        accepted params: prefix, marker, max-keys, delimiter
        """
        print self.list(**params)
        bl = self.list(**params)['ListBucketResult']
        keys = None
        if not bl.has_key('Contents'):
            keys = []
        elif isinstance(bl['Contents'], list):
            keys = [k['Key'] for k in bl['Contents']]
        else:
            keys = [bl['Contents']['Key'],]

        if bl.has_key('CommonPrefixes'):
            cp = bl['CommonPrefixes']
            if isinstance(cp, list):
                keys.extend([p['Prefix'] for p in cp])
            elif isinstance(cp, dict):
                keys.append(cp['Prefix'])

        return keys

    def has_key(self, k):
        """B.has_key(key) -- Bool, does key exists in bucket"""
        return self.list(prefix=k)['ListBucketResult'].has_key('Contents')

    def head(self, k, **headers):
        """B.head(key, **headers) -- get an object's headers from bucket

        Returns an HTTPResponse, but there should be no data to read().
        
        accepted headers: if_modified_since, if_unmodified_since, if_match,
                          if_none_match
        """
        return dict(self._s3cb.head(k, headers=headers).getheaders())


    def get(self, k, **headers):
        """B.get(key, **headers) -- get an object from bucket

        Returns an io object (HTTPResponse) so data can be read().
        
        accepted headers: If-Modified-Since, If-Unmodified-Since, If-Match,
                          If-None-Match, Range
        """
        r = self._s3cb.get(k, headers=headers)
        metadata = {}
        for header in r.getheaders():
            if header[0].startswith('x-amz-meta-'):
                metadata[header[0][11:]] = header[1]
        data = r.read()
        return S3Object(k, data, metadata)

    def put(self, s3object, **headers):
        """B.put(key, v, **headers) -- put an object into bucket

        v can be either an io object, or a string

        accepted headers: Cache-Control, Content-Type, Content-Length,
                          Content-MD5, Content-Disposition, Content-Encoding,
                          Expires
        """
        data = s3object.data
        if isinstance(data, str) or isinstance(data, unicode):
            data = StringIO(data)
        for key in s3object.metadata:
            headers['x-amz-meta-'+key] = s3object.metadata[key]
        self._s3cb.put(s3object.key, send_io=data, headers=headers)

    def delete(self, ks):
        """B.delete(keys) -- delete a key or keys from bucket"""
        if not isinstance(ks, list):
            ks = [ks,]
        for k in ks:
            self._s3cb.delete(k)

    def clone(self):
        """B.clone() -> new connection to the same bucket"""
        return S3Bucket(self._name, self._s3c.clone())

    def __getitem__(self, k):
        """B.__getitem__(key) -- get an object's data from s3

        the full data is returned, so no read() is needed"""
        return self.get(k).read()
        
    def __setitem__(self, k, v):
        """B.__setitem__(key, val) -- put an object into bucket"""
        self.put(k, v)

    def __delitem__(self, k):
        """B.__delitem__(key) -- delete a key from bucket"""
        self.delete(k)


