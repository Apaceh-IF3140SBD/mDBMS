import socketserver
from abc import abstractmethod

"""
    This is a base class for creating a custom server handler.

    Instructions:
    1. Extend the `ServerHandler` class and implement the `handle` method.
       The `handle` method will define how the server processes requests from clients.
       
    2. Create an instance of `ServerRunner` and call its `run` method.
       Pass the extended class (not an instance) of `ServerHandler` as the parameter to `run`.

    Example Usage:
    - Define your handler by extending `ServerHandler`.
    - Implement the `handle` method to process incoming requests.
    - Use `ServerRunner().run(YourCustomHandlerClass)` to start the server.
    - See ServerExample.py
"""
class ServerHandler(socketserver.BaseRequestHandler):
    
    HOST = 'localhost'
    PORT = 65432
    
    @abstractmethod
    def handle(self):
        """
        Define the behavior of the server when a request is received.
        This method must be implemented in subclasses.
        """
        pass

class ServerRunner:
    def run(self, handler: ServerHandler):
        """
        Start the server with the specified handler class.
        
        :param handler: A subclass of ServerHandler that implements the `handle` method.
        """
        host, port = ServerHandler.HOST, ServerHandler.PORT
        with socketserver.ThreadingTCPServer((host, port), handler) as server:
            print(f"Server running on {host}:{port}")
            server.serve_forever()
