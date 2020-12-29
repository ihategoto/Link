class App(object):
    def __init__(self, env, start_r):
        self.env = env
        self.start = start_r

    def __iter__(self):
        response_headers = [('Content-type', 'text/plain')]
        self.start('200 OK', response_headers)
        yield bytes('Hello world!\n', 'latin-1')
