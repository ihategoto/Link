from .modbus_handler.handler import Handler, InvalidRegister
import urllib

class App(object):
    def __init__(self, env, start_r):
        self.env = env
        self.start = start_r

    def __iter__(self):
        if self.env['REQUEST_METHOD'] != "GET":
            self.start("405 Method Not Allowed", [('Content-Type','text/plain')])
            yield bytes("GET only method allowed!\n", "latin-1")
        data = urllib.parse.parse_qs(self.env['QUERY_STRING'])
        if "slave" not in data or "sensor" not in data or "value" not in data:
            self.start("400 Bad Request", [('Content-Type','text/plain')])
            yield bytes("One ore more parameters are missing!\n", "latin-1")
        try:
            Handler.write(data["slave"][0], data["sensor"][0], data["value"][0])
        except Exception:
            self.start("500 Internal Server Error", [('Content-Type','text/plain')])
            yield bytes("Error while writing on sensor!\n", "latin-1")  
        self.start("200 OK", [('Content-Type','text/plain')])
        yield bytes("OK.\n", "latin-1")
        