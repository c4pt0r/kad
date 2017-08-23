import jsonrpclib
from jsonrpclib.SimpleJSONRPCServer import SimpleJSONRPCServer

class StoppableRPCServer:

    stopped = False

    def __init__(self, ip, port, funcs):
        server = SimpleJSONRPCServer((ip, port), logRequests = False)
        for func in funcs:
            server.register_function(func)

        def _stop():
            pass

        server.register_function(_stop)
        self.server = server
        self.ip = ip
        self.port = port

    def serve(self):
        while not self.stopped:
            self.server.handle_request()

    def force_stop(self):
        # hacky and stupid. -_-|||
        self.stopped = True
        get_client(self.ip, self.port)._stop()


def get_client(ip, port):
    remote = jsonrpclib.Server('http://'+ ip +':' + str(port))
    return remote

