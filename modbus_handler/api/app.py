def app(environ, start_fn):
    start_fn('200 OK', [('Content-Type', 'text/plain')])
    return [bytes("Hello world!\n", "utf-8")]
