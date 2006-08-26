from s3.connection import S3Connection
from s3.objects import S3Bucket
from s3.parsers import parseListBuckets, parseGetBucket, parseGetBucketNames

class S3Service:
    """S3 Service object"""
    
    def __init__(self, pub_key, priv_key):
        self._s3_conn = S3Connection(pub_key, priv_key)

    def get(self, name):
        """
        Get bucket with the exact name
        
        @param name: The name of the bucket
        @type name:  string
        @return:     Bucket if exists, else None
        @rtype:      S3Bucket or None
        """
        response = self._s3_conn.clone().get()
        return parseGetBucket(name, response.read(), self._s3_conn)

    def list(self):
        """
        List buckets:
        
        @return:       List of buckets associated with the authenticated user
        @rtype:        list
        """
        response = self._s3_conn.clone().get()
        return parseListBuckets(response.read(), self._s3_conn)

    def create(self, name):
        """
        Create a bucket
        
        @param name: Name for the new bucket
        @type name:  string
        @return:     Returns the newly created bucket
        @rtype:      S3Bucket
        """
        response = self._s3_conn.clone().put(name)
##        response.read()
        return S3Bucket(name, self._s3_conn)


    def delete(self, name):
        """
        Deletes a bucket
        
        @param name: Name of the queue that should be deleted
        @type  name: string
        """
        self._s3_conn.clone().delete(name)


    def keys(self):
        """S.keys() -- list buckets, returns just flat list of bucket names"""
        response = self._s3_conn.clone().get()
        return parseGetBucketNames(response.read())
        
    values = list

    def __getitem__(self, key):
        """S.__getitem__(key) -> a bucket instance"""
        return self.get(key)

    def __delitem__(self, key):
        """S.__delitem__(key) -- delete a bucket"""
        self.delete(key)
