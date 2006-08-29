VERSION = '2006-04-01'
DEFAULT_HOST = "queue.amazonaws.com"

from sqs.connection import SQSConnection
from sqs.errors import SQSError
from sqs.objects import SQSQueue, SQSMessage
from sqs.service import SQSService
from sqs.generator import SQSGenerator

Service = SQSService
