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

class SQSGenerator(object):
    """
    Generator class
    
    Objects of this class are used for generating authenticated URLs for accessing
    Amazon's SQS service.
    """
    def __init__(self, pub_key, priv_key, host=sqs.DEFAULT_HOST, port=None, secure=True):
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


    def _auth_header_value(self, action, timestamp):
        auth_str = "%s%s" % (action, timestamp)
        auth_str = base64.encodestring(
            hmac.new(self._priv_key, auth_str, sha).digest()).strip()
        return urllib.quote_plus(auth_str)
        

##    def _headers(self, headers=None, length=None):
##        if not headers:
##            headers = {}
##        if not headers.has_key('Date'):
##            headers["Date"] = time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime())
##        if not headers.has_key('AWS-Version'):
##            headers['AWS-Version'] = sqs.VERSION
##        if not headers.has_key('Content-Type'):
##            headers['Content-Type'] = DEFAULT_CONTENT_TYPE
##        if not headers.has_key('Content-MD5'):
##            headers['Content-MD5'] = ''
##        if not headers.has_key('Content-Length'):
##            if length is not None:
##                headers['Content-Length'] = length
##            else:
##                headers['Content-Length'] = 0
##        return headers


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


##    def _io_len(self, io):
##        if hasattr(io, "len"):
##            return io.len
##        o_pos = io.tell()
##        io.seek(0, 2)
##        length = io.tell() - o_pos
##        io.seek(o_pos, 0)
##        return length


    def _generate(self, method, queue=None, message=None, send_io=None, params=None, headers=None, acl=False):
        expires = 0
        if self._expires_in != None:
            expires = int(time.time() + self._expires_in)
        elif self._expires != None:
            expires = int(self._expires)
        expires_str = time.strftime("%Y-%m-%dT%XZ", time.gmtime(expires))
        path = self._path(queue, message, acl)
##        length = None
##        if isinstance(headers, dict) and headers.has_key("Content-Length"):
##            length = headers["Content-Length"]
##        elif send_io is not None:
##            length = self._io_len(send_io)
##        headers = self._headers(headers=headers, length=length)
        signature = self._auth_header_value(params['Action'], expires_str)
        path += self._params(params, acl)
        if '?' in path:
            arg_div = '&'
        else:
            arg_div = '?'
        query_part = "Expires=%s&AWSAccessKeyId=%s&Version=%s&Signature=%s" % \
                     (expires_str, self._pub_key, sqs.VERSION, signature)

        return self.protocol + '://' + self.server_name + path + arg_div + query_part


    def create_queue(self, name, timeout=None):
        """
        Create a queue.
        
        @param name:    The name to use for the Queue created.
                        The Queue name must be unique for all queues created by
                        the given Access Key ID.
        @type  name:    string
        @param timeout: Default visibility timeout for this Queue.
                        If this parameter is not included, the default value is
                        set to 30 seconds
        @type  timeout: int
        @return:        Authenticated URL for creating a new Queue.
        @rtype:         string
        """
        params = {
            'Action'    : 'CreateQueue',
            'QueueName' : name
        }
        if timeout:
            params['DefaultVisibilityTimeout'] = timeout
        return self._generate('GET', params=params)


    def list_queues(self, prefix=None):
        """
        List all queues or queues with a certan prefix.
        
        @param prefix: This parameter can be used to filter results returned.
                       When specified, only queues with queue names beginning
                       with the specified string are returned.
        @type  prefix: string
        @return:       Authenticated URL for listing Queues.
        @rtype:        string
        """
        params = { 'Action'    : 'ListQueues' }
        if prefix:
            params['QueueNamePrefix'] = prefix
        return self._generate('GET', params=params)


    def delete_queue(self, queue_url):
        """
        Delete a queue.
        
        @param queue_url: Queue url
        @type  queue_url: string
        @return:          Authenticated URL for deleting queue
        @rtype:           string
        """
        params = { 'Action' : 'DeleteQueue' }
        return self._generate('GET', queue=queue_url, params=params)


    def send_message(self, queue_url, message):
        """
        Save a message into Queue.
        
        @param queue_url: URL for the Queue in which the message should be saved
        @type  queue_url: string
        @param message:   Message body
        @type  message:   string
        @return:          Authenticated URL for saving message into Queue
        @rtype:           string
        """
        params = {
            'Action' : 'SendMessage',
            'MessageBody' : urllib.quote(message)
        }
        return self._generate('GET', queue=queue_url, params=params)

    def receave_message(self, queue_url, number=None, timeout=None):
        """
        Get message(s) from Queue.
        
        @param queue_url: URL for the Queue from which the message should be
                          relatived
        @type  queue_url: string
        @param number:    Maximum number of messages to return.
                          If the number of messages in the queue is less than
                          value specified by NumberOfMessages, then the number
                          of messages returned is up to the number of messages
                          in the queue. Not necessarily all the messages in the
                          queue will be returned. If no value is provided, the
                          default value of 1 is used.
        @type  number:    int
        @param timeout:   The duration, in seconds, that the messages are
                          visible in the queue. If no value is specified, the
                          default value for the queue is used
        @type  timeout:   int
        @return:          Authenticated URL for retreaving messages from Queue
        @rtype:           string
        """
        params = { 'Action' : 'ReceiveMessage' }
        if number: params['NumberOfMessages'] = number
        if timeout: params['VisibilityTimeout'] = timeout
        return self._generate('GET', queue=queue_url, params=params)
        

    def delete_message(self, queue_url, message_id):
        """
        Delete a message from Queue.
        
        @param queue_url:  URL for the Queue from which the message should be deleted
        @type  queue_url:  string
        @param message_id: The ID of the message to delete
        @type  message_id: string
        @return:           Authenticated URL for deleting message from Queue
        @rtype:            string
        """
        params = {
            'Action' : 'DeleteMessage',
            'MessageId' : message_id
        }
        return self._generate('GET', queue=queue_url, params=params)


    def peek_message(self, queue_url, message_id):
        """
        Returns a preview of the message specified in the MessageId parameter.
        
        The message is returned regardless of the VisibilityTimeout state on the
        queue. The visibility state is not modified when PeekMessage is used,
        thereby not affecting which messages get returned from a subsequent
        ReceiveMessage request.
        
        @param queue_url:  URL for the Queue from which the message should be peeked
        @type  queue_url:  string
        @param message_id: The ID of the message to retreave
        @type  message_id: string
        @return:           Authenticated URL for retreaveing a message
        @rtype:            string
        """
        params = {
            'Action' : 'PeekMessage',
            'MessageId' : message_id
        }
        return self._generate('GET', queue=queue_url, params=params)

    def set_timeout(self):
        pass


    def get_timeout(self):
        pass


    def add_grant(self):
        pass


    def get_grant(self):
        pass


    def del_grant(self):
        pass

