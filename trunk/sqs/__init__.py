VERSION = '2006-04-01'

from sqs.connection import SQSConnection
from sqs.errors import SQSError
from sqs.objects import SQSQueue, SQSMessage
from sqs.service import SQSService

Service = SQSService
