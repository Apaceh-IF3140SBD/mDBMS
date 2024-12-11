from datetime import datetime
# from classes.ConcurrencyControl import ConcurrencyControl
from ccManager.integration.CCWrapper import ConcurrencyControlWrapper
from classes.QueryOptimizer import QueryOptimizer
from classes.TreeHandler import TreeHandler
from classes.QueryTree import QueryTree
from classes.ExecutionResult import ExecutionResult
from storage.core.StorageEngine import StorageEngine
from storage.core.TableSchema import TableSchema
from storage.functions.DataRetrieval import DataRetrieval
from storage.functions.DataWrite import DataWrite
from storage.functions.DataDeletion import DataDeletion
from storage.functions.Condition import Condition


class QueryProcessor:
    def __init__(self, storage_engine: StorageEngine):
        self.storage_engine = storage_engine
        self.transaction_status = False
        self.concurrency_control = ConcurrencyControlWrapper(algorithm='Timestamp')
        self.transaction_id = -1

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
            if query.lower()  == "begin transaction;":
                if not self.transaction_status:
                    self.transaction_id = self.concurrency_control.begin_transaction()
                    self.transaction_status = True
                    print("START TRANSACTION\n")
                else:
                    print("WARNING:  there is already a transaction in progress\n START TRANSACTION\n")
            else:        
                if query.lower() == "commit;":
                    if self.transaction_status:
                        self.concurrency_control.end_transaction(self.transaction_id)
                        self.transaction_status = False
                        self.transaction_id = -1
                        print("COMMIT\n")
                    else:
                        print("WARNING:  there is no transaction in progress\nCOMMIT\n")
                else:
                    if self.transaction_status:                
                        query_optimizer = QueryOptimizer()
                        parsed_query = query_optimizer.parse_query(query)
                        optimized_query = query_optimizer.optimize_query(parsed_query)
                        tree_handler = TreeHandler(self.storage_engine)
                        tree_handler.proccess_node(optimized_query, self.transaction_id)
                    else:
                        self.transaction_id = self.concurrency_control.begin_transaction()
                        self.transaction_status = True
                        query_optimizer = QueryOptimizer()
                        parsed_query = query_optimizer.parse_query(query)
                        optimized_query = query_optimizer.optimize_query(parsed_query)
                        tree_handler = TreeHandler(self.storage_engine)
                        tree_handler.proccess_node(optimized_query, self.transaction_id)
                        self.concurrency_control.end_transaction(self.transaction_id)
                
        except Exception as e:
            # Code to handle the exception
            print(f"An error occurred: {e}")
