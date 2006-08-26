try:
    import cElementTree as et
except:
    from elementtree import ElementTree as et

from s3.errors import S3Error

import s3

xmlns = 'http://s3.amazonaws.com/doc/' + s3.VERSION + '/'

def parseError(xml):
    '''
    Parse the response XML if error occured, and creates an SQSError exception.
    
    @param xml: The XML error response
    @type  xml: string
    @return:    Returns the S3Error exception
    @rtype:     S3Error
    '''
    root = et.fromstring(xml)
    code = root.find('Code').text
    message = root.find('Message').text
    resource_node = root.find('Resource')
    if resource_node:
        resource = resource_node.text
    else:
        resource = ''
    return S3Error(code, message, resource)


def parseGetBucket(name, xml, connection):
    """
    Parse the response XML for geting a bucket
    
    @param name:       The name of the bucket
    @type  name:       string
    @param xml:        The XML response
    @type  xml:        string
    @param connection: S3Connection to the server
    @type  connection: S3Connection
    @return:           Bucket if exist else None
    @rtype:            S3Bucket or None
    """
    root = et.fromstring(xml)
    names = []
    buckets = root.find('{%s}Buckets' % xmlns)
    for bucket in buckets:
        names.append(bucket.find('{%s}Name' % xmlns).text)
    if name in names:
        return s3.S3Bucket(name, connection)
    else:
        return None

def parseGetBucketNames(xml):
    """
    Parse the response XML for geting a list of bucket names
    
    @param xml:        The XML response
    @type  xml:        string
    @return:           list of bucket names
    @rtype:            list
    """
    root = et.fromstring(xml)
    names = []
    buckets = root.find('{%s}Buckets' % xmlns)
    for bucket in buckets:
        names.append(bucket.find('{%s}Name' % xmlns).text)
    return names


def parseListBuckets(xml, connection):
    '''Parse the response XML for listing buckets
    
    @param xml:        The XML response
    @type  xml:        string
    @param connection: S3Connection to the server
    @type  connection: S3Connection
    @return:           List of buckets
    @rtype:            list
    '''
    root = et.fromstring(xml)
    buckets_list = []
    buckets = root.find('{%s}Buckets' % xmlns)
    for bucket in buckets:
        buckets_list.append(s3.S3Bucket(bucket.find('{%s}Name' % xmlns).text, connection))
    return buckets_list
        
