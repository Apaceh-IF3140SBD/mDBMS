import sys
import os

# Add project directories dynamically for runtime
# sys.path.append(os.path.dirname(os.path.abspath(__file__)))
# sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "classes"))
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "processor"))
# sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "processor", "classes"))
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "storage"))
# sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "storage", "utils"))



from core.BufferManager import BufferManager
from core.StorageEngine import StorageEngine
from core.TableSchema import TableSchema
from classes.QueryProcessor import QueryProcessor
from functions.Condition import Condition
from functions.DataWrite import DataWrite
from functions.DataRetrieval import DataRetrieval
from functions.DataDeletion import DataDeletion
import random




def format_as_table(data):
    if not data or not isinstance(data, list):
        return "No data found."

    # Extract headers dynamically
    headers = [f"Column {i+1}" for i in range(len(data[0]))]
    header_line = " | ".join(f"{header:^15}" for header in headers)
    separator = "-+-".join("-" * 15 for _ in headers)

    # Format rows
    rows = []
    for row in data:
        row_line = " | ".join(f"{str(value):^15}" for value in row)
        rows.append(row_line)

    table = f"{header_line}\n{separator}\n" + "\n".join(rows)
    return table


def initialize_storage(storage_engine):
    # Define and create the "students" table
    student_schema = TableSchema(
        table_name="students",
        columns={
            "StudentID": "int",
            "FullName": "varchar(50)",
            "GPA": "float"
        }
    )
    storage_engine.create_table(student_schema)

    # Insert test data
    test_data = [
        (1, "Alice", 3.9),
        (2, "Bob", 3.2),
        (3, "Charlie", 3.6),
        (4, "Diana", 2.8)
    ]
    for student in test_data:
        storage_engine.insert(
            data_write=DataWrite(
                table="students",
                columns=["StudentID", "FullName", "GPA"],
                new_value=student,
                conditions=[]
            )
        )
    print("Test data inserted into 'students' table.")


def execute_query_demo():
    # Initialize BufferManager and StorageEngine
    schemas = {}
    buffer_manager = BufferManager(buffer_size=5, schemas=schemas)
    storage_engine = StorageEngine(buffer_manager)
    query_processor = QueryProcessor(storage_engine)

    initialize_storage(storage_engine)

    print("\n[SELECT QUERY]")
    query_tree_select = {
        "command": "SELECT",
        "table": "students",
        "attributes": ["StudentID", "FullName", "GPA"],
        "conditions": [
            {"column": "GPA", "operation": ">", "operand": 3.5}
        ]
    }
    result = query_processor.execute_query(query_tree_select)
    print(format_as_table(result))

    print("\n[INSERT QUERY]")
    query_tree_insert = {
        "command": "INSERT",
        "table": "students",
        "attributes": ["StudentID", "FullName", "GPA"],
        "values": [6, "Eve", 3.8]
    }
    result = query_processor.execute_query(query_tree_insert)
    print(result)

    print("\n[UPDATE QUERY]")
    query_tree_update = {
        "command": "UPDATE",
        "table": "students",
        "attributes": ["GPA"],
        "values": [4.0],
        "conditions": [
            {"column": "StudentID", "operation": "=", "operand": 5}
        ]
    }
    result = query_processor.execute_query(query_tree_update)
    print(result)

    print("\n[SELECT QUERY AFTER UPDATE]")
    query_tree_select_all = {
        "command": "SELECT",
        "table": "students",
        "attributes": ["StudentID", "FullName", "GPA"]
    }
    result = query_processor.execute_query(query_tree_select_all)
    print(format_as_table(result))

    print("\n[DELETE QUERY]")
    query_tree_delete = {
        "command": "DELETE",
        "table": "students",
        "conditions": [
            {"column": "GPA", "operation": "<", "operand": 3.3}
        ]
    }
    result = query_processor.execute_query(query_tree_delete)
    print(result)

    print("\n[SELECT QUERY AFTER DELETE]")
    result = query_processor.execute_query(query_tree_select_all)
    print(format_as_table(result))


if __name__ == "__main__":
    execute_query_demo()
