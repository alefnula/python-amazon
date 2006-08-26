class SQSError(Exception):
    def __init__(self, status, reason, body=None):
        Exception.__init__(self)
        self.status = status
        self.reason = reason
        self.body = body

    def __repr__(self):
        return '%s: %s\n%s' % (self.status, self.reason, self.body)

    def __str__(self):
        return '%s: %s\n%s' % (self.status, self.reason, self.body)
