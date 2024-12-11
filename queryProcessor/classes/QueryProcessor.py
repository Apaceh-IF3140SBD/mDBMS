from datetime import datetime
import random
# from classes.ConcurrencyControl import ConcurrencyControl
from concurrencyControl.CCWrapper import ConcurrencyControlWrapper
from queryOptimizer.classes.OptimizationEngine import OptimizationEngine
from queryProcessor.classes.TreeHandler import TreeHandler
from queryProcessor.classes.QueryTree import QueryTree
from queryProcessor.classes.ExecutionResult import ExecutionResult
from storageManager.core.StorageEngine import StorageEngine
from storageManager.core.BufferManager import BufferManager
from storageManager.core.TableSchema import TableSchema
from storageManager.functions.DataRetrieval import DataRetrieval
from storageManager.functions.DataWrite import DataWrite
from storageManager.functions.DataDeletion import DataDeletion
from storageManager.functions.Condition import Condition
from failureRecovery.core.FailureRecovery import FailureRecovery
from server.Server import ServerHandler
# from dataclasses import dataclass, asdict
import json

student_schema = TableSchema(
    table_name="students",
    columns={
        "StudentID": "int",
        # "FullName": "varchar(50)",
        "FullName": "char(50)",
        "Nickname": "varchar(50)",
        "GPA": "float"
    }
)

student_course_schema = TableSchema(
    table_name="students_courses_relation",
    columns={
        "StudentId": "int",
        "Year": "int",
        "CourseId": "int"
    }
)

course_schema = TableSchema(
    table_name="courses",
    columns={
        "CourseID": "int",
        "Year": "int",
        "CourseName": "varchar(50)",
        "CourseDescription": "varchar(65535)"
    }
)


def generate_name():
    first_names = ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve', 'Frank', 'Grace', 'Hank', 'Ivy', 'Jack']
    last_names = ['Smith', 'Johnson', 'Williams', 'Jones', 'Brown', 'Davis', 'Miller', 'Wilson', 'Moore', 'Taylor']
    return f"{random.choice(first_names)} {random.choice(last_names)}"

def generate_course_name():
    subjects = ['Math', 'Science', 'History', 'Art', 'Music', 'Literature', 'Physics', 'Chemistry', 'Biology', 'Economics']
    levels = ['101', '201', '301', '401']
    return f"{random.choice(subjects)} {random.choice(levels)}"

def generate_name():
    first_names = ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve', 'Frank', 'Grace', 'Hank', 'Ivy', 'Jack']
    last_names = ['Smith', 'Johnson', 'Williams', 'Jones', 'Brown', 'Davis', 'Miller', 'Wilson', 'Moore', 'Taylor']
    return f"{random.choice(first_names)} {random.choice(last_names)}"



class QueryProcessor (ServerHandler):
    def __init__(self):
        schemas = {}
        self.storage_engine = StorageEngine(buffer_manager=BufferManager(buffer_size=5, schemas=schemas))
        self.failure_recovery = FailureRecovery(self.storage_engine)
        self.transaction_status = False
        self.concurrency_control = ConcurrencyControlWrapper(algorithm='Timestamp')
        self.transaction_id = -1
        self.initialize_schema()

    def initialize_schema(self):
        self.storage_engine.create_table(student_schema)
        self.storage_engine.create_table(student_course_schema)
        self.storage_engine.create_table(course_schema)

        for student_id in range(1, 100):
            full_name = generate_name()
            nick_name = "cupi"
            gpa = round(random.uniform(2.0, 4.0), 2)
            data_write = DataWrite(
                table="students",
                columns=['StudentID', 'FullName', 'Nickname','GPA'],
                new_value=[student_id, full_name, nick_name, gpa],
                conditions=[]
            )
            self.storage_engine.insert(data_write)

#         student_course_schema = TableSchema(
#     table_name="students_courses_relation",
#     columns={
#         "StudentId": "int",
#         "Year": "int",
#         "CourseId": "int"
#     }
# )

#         for course_id in range(1, 100):
#             course_name = generate_course_name()
            
#             # storage_engine.buffer_manager.flush_all_block()  # This for testing purpose, is in order to write all thing from buffer to disk, flexible for failure recovery
#             # print(f"Inserted student: {student_id}, {full_name}, {gpa}")
            

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
                        query_optimizer = OptimizationEngine(storageEngine=StorageEngine)
                        parsed_query = query_optimizer.parse_query(query)
                        optimized_query = query_optimizer.optimize_query(parsed_query)
                        tree_handler = TreeHandler(self.storage_engine)
                        result = tree_handler.proccess_node(optimized_query, self.transaction_id)
                        return result
                    else:
                        self.transaction_id = self.concurrency_control.begin_transaction()
                        self.transaction_status = True
                        query_optimizer = OptimizationEngine()
                        parsed_query = query_optimizer.parse_query(query)
                        optimized_query = query_optimizer.optimize_query(parsed_query)
                        tree_handler = TreeHandler(self.storage_engine)
                        result = tree_handler.proccess_node(optimized_query, self.transaction_id)
                        self.concurrency_control.end_transaction(self.transaction_id)
                        return result
                
        except Exception as e:
            # Code to handle the exception
            print(f"An error occurred: {e}")
            raise e

        

        
