from datetime import datetime
import random
# from classes.ConcurrencyControl import ConcurrencyControl
from queryOptimizer.classes.OptimizationEngine import OptimizationEngine
from queryProcessor.classes.TreeHandler import TreeHandler
from queryProcessor.classes.QueryTree import QueryTree
from queryProcessor.classes.ExecutionResult import ExecutionResult
from storageManager.core.StorageEngine import StorageEngine
from concurrencyControl.CCWrapper import ConcurrencyControlWrapper
from storageManager.core.BufferManager import BufferManager
from failureRecovery.core.FailureRecovery import FailureRecovery
from storageManager.core.TableSchema import TableSchema
from storageManager.functions.DataRetrieval import DataRetrieval
from storageManager.functions.DataWrite import DataWrite
from storageManager.functions.DataDeletion import DataDeletion
from storageManager.functions.Condition import Condition
from server.Server import ServerHandler, CustomServer
# from dataclasses import dataclass, asdict
import json



class QueryProcessor(ServerHandler):
    def __init__(self, request, client_address, server: CustomServer):
        self.storage_engine = server.storage_engine
        self.failure_recovery = server.failure_recovery
        self.transaction_statuss = False
        self.concurrency_control = server.concurrency_control
        self.transaction_id = -1
        super().__init__(request, client_address, server)

    def handle(self):
        print(f"Connection established with {self.client_address}")

        try:
            data = self.request.recv(1024).decode('utf-8').strip()
            print(f"Received request: {data}")

            result = self.execute_query(data)

            response_json = json.dumps(result)
            self.request.sendall(response_json.encode('utf-8'))


            # person_data = fruit_mapping.get(data)
            # if person_data:
            #     response = asdict(person_data)
            # else:
            #     response = {"error": "Unknown person"}

            # response_json = json.dumps(response)
            # self.request.sendall(response_json.encode('utf-8'))

        except Exception as e:
            message = "Exception raised"
            print(f"Error handling client {self.client_address}: {e}")
            response_json = json.dumps(message)
            self.request.sendall(response_json.encode('utf-8'))
        finally:
            print(f"Connection closed with {self.client_address}")

            

    def execute_query(self, query: str):
        """
        Execute a parsed query.

        query_tree: dict
            Example query_tree format:
            {
                "command": "SELECT", # or "INSERT", "UPDATE", "DELETE", "CREATE"
                "table": "students",
                "attributes": ["StudentID", "FullName"], # For SELECT, UPDATE
                "values": [1, "Alice", 3.8], # For INSERT, UPDATE
                "conditions": [{"column": "GPA", "operation": ">", "operand": 3.5}] # For SELECT, UPDATE, DELETE
            }
        """

        try:
            # if query.lower()  == "begin transaction;":
            #     if not self.transaction_statuss:
            #         self.transaction_id = self.concurrency_control.begin_transaction()
            #         self.transaction_statuss = True
            #         print("START TRANSACTION\n")
            #     else:
            #         print("WARNING:  there is already a transaction in progress\n START TRANSACTION\n")
            # else:        
            #     if query.lower() == "commit;":
            #         if self.transaction_statuss:
            #             self.concurrency_control.end_transaction(self.transaction_id)
            #             self.transaction_statuss = False
            #             self.transaction_id = -1
            #             print("COMMIT\n")
            #         else:
            #             print("WARNING:  there is no transaction in progress\nCOMMIT\n")
            #     else:
            #         if self.transaction_statuss:                
            #             query_optimizer = OptimizationEngine(storageEngine=StorageEngine)
            #             parsed_query = query_optimizer.parse_query(query)
            #             optimized_query = query_optimizer.optimize_query(parsed_query)
            #             tree_handler = TreeHandler(self.storage_engine)
            #             result = tree_handler.proccess_node(optimized_query, self.transaction_id)
            #             return result
            #         else:
                        self.transaction_id = self.concurrency_control.begin_transaction()
            #             self.transaction_statuss = True
                        query_optimizer = OptimizationEngine(self.storage_engine, {})
                        # parsed_query = query_optimizer.parse_query(query)
                        optimized_query = query_optimizer.optimize_query(query)
                        print("ADASDASD", optimized_query)
                        print("After optimization")
                        tree_handler = TreeHandler(self.storage_engine)
                        result = tree_handler.process_node(optimized_query, self.transaction_id)
                        self.concurrency_control.end_transaction(self.transaction_id)
                        return result
                
        except Exception as e:
            # Code to handle the exception
            print(f"An error occurred: {e}")
            raise e

        

        
