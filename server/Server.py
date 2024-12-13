import socketserver
import os
from abc import abstractmethod
from concurrencyControl.CCWrapper import ConcurrencyControlWrapper
from storageManager.core.BufferManager import BufferManager
from storageManager.utils.SchemaManager import SchemaManager
from failureRecovery.core.FailureRecovery import FailureRecovery
from storageManager.core.StorageEngine import StorageEngine
from storageManager.functions.DataRetrieval import DataRetrieval


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


# def display_schemas(bin_dir):
#     """
#     Display all schemas in the `bin` directory.
#     """
#     for file_name in os.listdir(bin_dir):
#         if file_name.endswith("_schema.bin"):
#             schema_path = os.path.join(bin_dir, file_name)
#             try:
#                 schema = SchemaManager.load_schema(schema_path)
#                 print(f"Schema for table '{schema.table_name}':")
#                 for column, col_type in schema.columns.items():
#                     print(f"  - {column}: {col_type}")
#                 print("\n")
#             except Exception as e:
#                 print(f"Failed to load schema from {file_name}: {e}")


# def display_blocks(bin_dir, buffer_manager):
#     """
#     Display all data blocks in the `bin` directory.
#     """
#     for file_name in os.listdir(bin_dir):
#         if "_block_" in file_name and file_name.endswith(".bin"):
#             table_name, block_id = file_name.split("_block_")
#             block_id = int(block_id.split(".")[0])
#             try:
#                 block = buffer_manager.load_block_from_disk(table_name, block_id)
#                 print(f"Block {block_id} for table '{table_name}':")
#                 for row_id, row in block.rows.items():
#                     print(f"  Row {row_id}: {row}")
#                 print("\n")
#             except Exception as e:
#                 print(f"Failed to read block {block_id} for table {table_name}: {e}")




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
        bin_dir = os.path.join(os.path.dirname(__file__), "../bin")
        schemas = {}
        buffer_manager = BufferManager(buffer_size=5, schemas=schemas)
        storage_engine = StorageEngine(buffer_manager)

        storage_engine.load_all_table()
        print("Loaded schemas:")
        # display_schemas(bin_dir)

        print("Existing data blocks:")
        # display_blocks(bin_dir, buffer_manager)

        failure_recovery = FailureRecovery(storage_engine)
        concurrency_control = ConcurrencyControlWrapper(algorithm="Timestamp")

        # Start the server
        host, port = "localhost", 65432
        with CustomServer((host, port), handler, storage_engine, failure_recovery, concurrency_control) as server:
            print(f"Server running on {host}:{port}")
            server.serve_forever()


class CustomServer(socketserver.ThreadingTCPServer):
    def __init__(self, server_address, RequestHandlerClass, storage_engine: StorageEngine, failure_recovery: FailureRecovery, concurrency_control: ConcurrencyControlWrapper):
        super().__init__(server_address, RequestHandlerClass)
        self.storage_engine = storage_engine
        self.failure_recovery = failure_recovery
        self.concurrency_control = concurrency_control