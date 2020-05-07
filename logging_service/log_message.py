import datetime

class LogMessage:
    def __init__(self, author, topic, content=None, type='log', debug=True):
        self.author = author
        self.topic = topic
        self.content = content
        self.type = type
        self.debug = debug

    def __str__(self):
        return f'{datetime.datetime.now().strftime("%H:%M:%S.%f")}\t\tAUTHOR:{self.author}\t\tTOPIC:{self.topic}\t\tTYPE:{self.type}\t\t{"DEBUG" if self.debug else "PRODUCTION"}\t\t{f"CONTENT:{self.content}" if self.content is not None else ""}'