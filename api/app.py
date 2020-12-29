from ..modbus_handler.handler import Handler, InvalidRegister
import urllib

class App(object):
    def __init__(self, env, start_r):
        self.env = env
        self.start = start_r

    def __iter__(self):
        if self.env['REQUEST_METHOD'] != "GET":
            self.start("405 Method Not Allowed", [('Content-Type','text/plain')])
            yield bytes("GET only method allowed!", "latin-1")
        data = urllib.parse.parse_qs(self.env['QUERY_STRING'])
        print(repr(data), flush=True)
        