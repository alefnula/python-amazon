import xml
import urlparse
from sqs.errors import SQSError
from sqs.parsers import parseTimeout, parseMessageCreate, parseMessageRead, parseMessagesRead
from StringIO import StringIO


class SQSMessage(object):
    def __init__(self, body='', id=None, queue=None):
        self._body = body
        self.id = id
        self.queue = queue

    def __len__(self):
        return len(self._body)
    
    def __str__(self):
        return self._body

    def __repr__(self):
        return self._body

    def _getBody(self):
        return self._body

    def _setBody(self, body):
        self._body = body

    body = property(_getBody, _setBody)



class SQSQueue:
    def __init__(self, url, sqs_conn):
        self._sqs_conn = sqs_conn
        self.url = url
        self.id = urlparse.urlparse(self.url)[2] 
        self.name = self.id.split('/')[2]

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.name

    def clone(self):
        """Clones the connection to the queue
        
        @return: New connection to the same queue
        @rtype:  SQSQueue
        """
        return SQSQueue(self.url, self._sqs_conn.clone())


##    def get_timeout(self):
##        '''Get the visibility timeout for the queue
##        
##        @return: Visibility timeout
##        @rtype:  int
##        '''
##        response = self._sqs_conn.clone().get()
##        print respo
##        return parseTimeout(response.read())


    def read(self, visibility_timeout=None):
        '''Returns a single message or None if queue is empty'''
        params = {}
        if visibility_timeout != None:
            params = {'VisibilityTimeout' : visibility_timeout}
        queue = self.id + '/front'
        response = self._sqs_conn.clone().get(queue=queue, params=params)
        return parseMessageRead(response.read())


    def write(self, message):
        '''Add a single message to the queue'''
        message.queue = self
        headers = {'Content-Length':len(message)}
        queue = self.id + '/back'
        response = self._sqs_conn.clone().put(queue=queue, send_io=StringIO(message.body), headers=headers)
        message.id = parseMessageCreate(response.read())
        return message


    def get_messages(self, number, timeout=None):
        '''Get a variable number of messages
        
        @param number:  Number of messages to get
        @type  number:  int
        @param timeout: Visibility timeout for the message
        @type  timeout: int
        @return:        List of messages
        @rtype:         list
        '''
        queue = self.id + '/front'
        params = {'NumberOfMessages' : number}
        if timeout:
            params['VisibilityTimeout'] = timeout
        response = self._sqs_conn.clone().get(queue=queue, params=params)
        return parseMessagesRead(response.read())


    def delete(self, message):
        '''Delete the message from the queue
        
        @param message: Message to delete
        @type  message: SQSMessage
        '''
        if hasattr(message, 'id'):
            self._sqs_conn.clone().delete(queue=self.id, message=message.id)
