import socketserver
from abc import abstractmethod
from concurrencyControl.CCWrapper import ConcurrencyControlWrapper
from storageManager.core.BufferManager import BufferManager
from failureRecovery.core.FailureRecovery import FailureRecovery
from storageManager.core.StorageEngine import StorageEngine


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

from storageManager.core.TableSchema import TableSchema
from storageManager.functions.DataWrite import DataWrite
import random


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

class ServerRunner:
    def run(self, handler: ServerHandler):
        """
        Start the server with the specified handler class.
        
        :param handler: A subclass of ServerHandler that implements the `handle` method.
        """
        schemas = {}
        storage_engine = StorageEngine(buffer_manager=BufferManager(buffer_size=5, schemas=schemas))
        failure_recovery = FailureRecovery(storage_engine)
        concurrency_control = ConcurrencyControlWrapper(algorithm="Timestamp")


        storage_engine.create_table(student_schema)
        storage_engine.create_table(student_course_schema)
        storage_engine.create_table(course_schema)

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
            storage_engine.insert(data_write)

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