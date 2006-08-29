from sqs.errors import SQSError
import sqs

try:
    import cElementTree as et
except:
    from elementtree import ElementTree as et

xmlns = "http://queue.amazonaws.com/doc/" + sqs.VERSION + '/'

def parseError(xml):
    '''
    Parse the response XML if error occured, and creates an SQSError exception.
    
    @param xml: The XML error response
    @type  xml: string
    @return:    Returns the SQSError exception
    @rtype:     SQSError
    '''
    print "Dosao sam u parseError:\n", xml
    root = et.fromstring(xml)
    code = root.find('Errors/Error/Code').text
    body = root.find('Errors/Error/Message').text
    if code == 'MissingParameter':
        missing_parameter = root.find('MissingParameterName')
        if missing_parameter: reason = missing_parameter.text
        else: reason = ''
    elif code == 'AccessFailure':
        service_name = root.find('ServiceName')
        if service_name: reason = service_name.text
        else: reason = ''
    else:
        reason = ''
    return SQSError(code, reason, body)


def parseQueueList(xml, connection):
    '''Parse the response XML for listing queues
    
    @param xml:        The XML response
    @type  xml:        string
    @param connection: SQSConnection to the server
    @type  connection: SQSConnection
    @return:           List of queues
    @rtype:            list
    '''
    queues = []
    root = et.fromstring(xml)
    for node in root.findall('{%s}QueueUrl' % xmlns):
        queues.append(sqs.SQSQueue(node.text, connection))
    return queues


def parseQueueCreation(xml, connection):
    '''Parse the response XML returned on queue creation
    
    @param xml:        The XML response
    @type  xml:        string
    @param connection: SQSConnection to the server
    @type  connection: SQSConnection
    @return:           Newly created queue
    @rtype:            SQSQueue
    '''
    root = et.fromstring(xml)
    return sqs.SQSQueue(root.find('{%s}QueueUrl' % xmlns).text, connection)


def parseTimeout(xml):
    pass


def parseMessageCreate(xml):
    root = et.fromstring(xml)
    return root.find('{%s}MessageId' % xmlns)


def parseMessageRead(xml):
    root = et.fromstring(xml)
    message = root.find('{%s}Message' % xmlns)
    if message != None:
        id = message.find('{%s}MessageId' % xmlns).text
        body = message.find('{%s}MessageBody' % xmlns).text
        return sqs.SQSMessage(id=id, body=body)
    return None


def parseMessagesRead(xml):
    messages = []
    root = et.fromstring(xml)
    for message in root.findall('{%s}Message' % xmlns):
        id = message.find('{%s}MessageId' % xmlns).text
        body = message.find('{%s}MessageBody' % xmlns).text
        messages.append(sqs.SQSMessage(id=id, body=body))
    return messages

##class GetTimeoutHandler(xml.sax.ContentHandler):
##    def __init__(self):
##        self.timeout = None
##        self.status = None
##        self.request_id = None
##        self.curr_text = ''
##
##    def endElement(self, name):
##        if name == 'VisibilityTimeout':
##            self.timeout = int(self.curr_text)
##        elif name == 'StatusCode':
##            self.status = self.curr_text
##        elif name == 'RequestId':
##            self.request_id = self.curr_text
##
##        self.curr_text = ''
##
##    def characters(self, content):
##        self.curr_text += content
##
##
##class GetMessageHandler(xml.sax.ContentHandler):
##    def __init__(self, queue, messageClass):
##        self.queue = queue
##        self.message = messageClass()
##        self.message.queue = queue
##        self.id = None
##        self.request_id = None
##        self.curr_text = ''
##
##    def endElement(self, name):
##        if name == 'MessageId':
##            self.message.id = self.curr_text
##        elif name == 'MessageBody':
##            self.message.set_body(self.curr_text)
##        elif name == 'StatusCode':
##            self.status = self.curr_text
##        elif name == 'RequestId':
##            self.request_id = self.curr_text
##        self.curr_text = ''
##
##    def characters(self, content):
##        self.curr_text += content
##
##
##
##class GetMessagesHandler(xml.sax.ContentHandler):
##    def __init__(self, queue, messageClass):
##        self.queue = queue
##        self.messages = []
##        self.current_message = None
##        self.id = None
##        self.request_id = None
##        self.curr_text = ''
##        self._messageClass = messageClass
##
##    def startElement(self, name, attrs):
##        if name == 'Message':
##            self.current_message = self._messageClass()
##            self.current_message.queue = self.queue
##            self.messages.append(self.current_message)
##
##    def endElement(self, name):
##        if name == 'MessageId':
##            self.current_message.id = self.curr_text
##        elif name == 'MessageBody':
##            self.current_message.body = self.curr_text
##        elif name == 'StatusCode':
##            self.status = self.curr_text
##        elif name == 'RequestId':
##            self.request_id = self.curr_text
##        self.curr_text = ''
##
##    def characters(self, content):
##        self.curr_text += content
