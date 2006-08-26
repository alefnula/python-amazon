from sqs.connection import SQSConnection
from sqs.objects import SQSQueue
from sqs.parsers import parseQueueList, parseQueueCreation

class SQSService(object):
    """SQS Service object"""
    def __init__(self, pub_key, priv_key):
        self._sqs_conn = SQSConnection(pub_key, priv_key)

    def get(self, name):
        """
        Get queue with the exact name
        
        @param name: The name of the queue
        @type name:  string
        @return:     Queue if exists, else None
        @rtype:      SQSQueue or None
        """
        list = self.list(name)
        for queue in list:
            if queue.name == name:
                return queue
        return None


    def list(self, prefix=''):
        """
        List queues:
        
        @param prefix: Queue name prefix.
        @type prefix:  string
        @return:       List of queues that begin with prefix
        @rtype:        list
        """
        if prefix:
            params = {'QueueNamePrefix' : prefix }
        else:
            params = {}
        response = self._sqs_conn.clone().get(params=params)
        return parseQueueList(response.read(), self._sqs_conn)


    def create(self, name):
        """
        Create a queue
        
        @param name: Name for the new queue
        @type name:  string
        @return:     Returns the newly created queue
        @rtype:      SQSQueue
        """
        response = self._sqs_conn.clone().post(params={'QueueName' : name})
        return parseQueueCreation(response.read(), self._sqs_conn)


    def delete(self, name):
        """
        Deletes a queue
        
        @param name: Name of the queue that should be deleted
        @type  name: string
        """
        q = self.get(name)
        self._sqs_conn.clone().delete(queue=q.id)
