### Main file, only for testing purposes

from storage.core.BufferManager import BufferManager
from storage.core.StorageEngine import StorageEngine
from storage.core.TableSchema import TableSchema
from storage.functions.Condition import Condition
from storage.functions.DataWrite import DataWrite
from storage.functions.DataRetrieval import DataRetrieval
from storage.functions.DataDeletion import DataDeletion
from datetime import datetime
from functions.ExecutionResult import ExecutionResult, DataPass, Rows
from core.FailureRecovery import FailureRecovery
from functions.WALLogEntry import WALLogEntry
from functions.RecoverCriteria import RecoverCriteria
import random

# rows = Rows([[1, "Alice"], [2, "Bob"]])
# log_entry = WALLogEntry(
#     log_sequence_number=1,
#     transaction_id=123,
#     operation_type="INSERT",
#     data_before=None,
#     data_after=rows,
#     table_name="Users",
#     active_trans=[123],
#     timestamp=datetime.now(),
# )
# print(log_entry.to_dict())  # Ensure no serialization issues

# exit()

# Define table schemas
student_schema = TableSchema(
    table_name="students",
    columns={
        "StudentID": "int",
        "FullName": "varchar(50)",
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

# Initialize schemas and buffer manager
schemas = {}
buffer_manager = BufferManager(buffer_size=5, schemas=schemas)
storage_engine = StorageEngine(buffer_manager)

# Create tables
storage_engine.create_table(student_schema)
storage_engine.create_table(student_course_schema)
storage_engine.create_table(course_schema)

recovery = FailureRecovery(storageEngine=storage_engine)

# Helper function to generate random names
def generate_name():
    first_names = ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve', 'Frank', 'Grace', 'Hank', 'Ivy', 'Jack']
    last_names = ['Smith', 'Johnson', 'Williams', 'Jones', 'Brown', 'Davis', 'Miller', 'Wilson', 'Moore', 'Taylor']
    return f"{random.choice(first_names)} {random.choice(last_names)}"

# Helper function to generate random course names
def generate_course_name():
    subjects = ['Math', 'Science', 'History', 'Art', 'Music', 'Literature', 'Physics', 'Chemistry', 'Biology', 'Economics']
    levels = ['101', '201', '301', '401']
    return f"{random.choice(subjects)} {random.choice(levels)}"

# Start Transaction
transaction_id = 1
# start_log = ExecutionResult(
#     transaction_id=transaction_id,
#     timestamp=datetime.now(),
#     message="Transaction Started",
#     data_before=None,
#     data_after=None,
#     query="START"
# )
# recovery.write_log(start_log)

# Insert students
full_name = generate_name()
gpa = round(random.uniform(2.0, 2.99), 2)
data_write = DataWrite(
    table="students",
    columns=['StudentID', 'FullName', 'GPA'],
    new_value=[1, full_name, gpa],
    conditions=[]  # No conditions for insert
)
insert_log = ExecutionResult(
    transaction_id=transaction_id,
    timestamp=datetime.now(),
    message="INSERT",
    data_before=None,
    data_after=DataPass(
        db="database_name",
        table="students",
        cols=['StudentID', 'FullName', 'GPA'],
        data=Rows(data=[[1, full_name, gpa], [2, full_name, gpa - 1], [3, full_name, gpa - 2]]),
        todo=data_write
    ),
    query="INSERT INTO students"
)
recovery.write_log(insert_log)

# Insert a course
course_name = generate_course_name()
data_write = DataWrite(
    table="courses",
    columns=['CourseID', 'Year', 'CourseName', 'CourseDescription'],
    new_value=[1, 2004, course_name, "skibidi toilet"],
    conditions=[]  # No conditions for insert
)
insert_log = ExecutionResult(
    transaction_id=2,
    timestamp=datetime.now(),
    message="INSERT",
    data_before=None,
    data_after=DataPass(
        db="database_name",
        table="courses",
        cols=['CourseID', 'Year', 'CourseName', 'CourseDescription'],
        data=Rows(data=[[1, 2004, course_name, "skibidi toilet"]]),
        todo=data_write
    ),
    query="INSERT INTO courses"
)
recovery.write_log(insert_log)

print("-----------------------------------")
print(storage_engine.buffer_manager.buffer_pool)
print("-----------------------------------")

# Update student GPA
uc = Condition(column='GPA', operation='<', operand=3.0)
ctu = ['GPA']
nv = [3.5]
data_update = DataWrite(
    table='students',
    columns=ctu,
    new_value=nv,
    conditions=[uc]
)
update_log = ExecutionResult(
    transaction_id=transaction_id,
    timestamp=datetime.now(),
    message="UPDATE",
    data_before=DataPass(
        db="database_name",
        table="students",
        cols=['StudentID', 'FullName', 'GPA'],
        data=Rows([[1, full_name, gpa]]),
        todo=None
    ),
    data_after=DataPass(
        db="database_name",
        table="students",
        cols=['StudentID', 'FullName', 'GPA'],
        data=Rows([[1, full_name, 3.5]]),
        todo=data_update
    ),
    query="UPDATE students SET GPA=3.5 WHERE GPA < 3.0"
)
recovery.write_log(update_log)

delete_log = ExecutionResult(
    transaction_id=3,
    timestamp=datetime.now(),
    message="DELETE",
    data_before=DataPass(
        db="database_name",
        table="students",
        cols=['StudentID', 'FullName', 'GPA'],
        data=Rows([[1, full_name, gpa]]),
        todo=None
    ),
    data_after=None,
    query="DELETE"
)
recovery.write_log(delete_log)

abortion = RecoverCriteria(None, 1)
# recovery.recover(abortion)
recovery.commit(1)

# Abort Transaction
# abort_log = ExecutionResult(
#     transaction_id=transaction_id,
#     timestamp=datetime.now(),
#     message="Transaction Aborted",
#     data_before=None,
#     data_after=None,
#     query="ABORT"
# )
# recovery.write_log(abort_log)

# full_name = generate_name()
# gpa = round(random.uniform(2.0, 2.99), 2)
# data_write = DataWrite(
#     table="students",
#     columns=['StudentID', 'FullName', 'GPA'],
#     new_value=[1, full_name, gpa],
#     conditions=[]  # No conditions for insert
# )
# insert_log = ExecutionResult(
#     transaction_id=transaction_id,
#     timestamp=datetime.now(),
#     message="INSERT",
#     data_before=None,
#     data_after=DataPass(
#         db="database_name",
#         table="students",
#         cols=['StudentID', 'FullName', 'GPA'],
#         data=Rows(data=[[1, full_name, gpa], [2, full_name, gpa - 1], [3, full_name, gpa - 2]]),
#         todo=data_write
#     ),
#     query="INSERT INTO students"
# )
# recovery.write_log(insert_log)

recovery.save_checkpoint()
