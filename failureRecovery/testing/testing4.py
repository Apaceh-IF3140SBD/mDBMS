# write_log integration testing

from storage.core.BufferManager import BufferManager
from storage.core.StorageEngine import StorageEngine
from storage.core.TableSchema import TableSchema
from storage.functions.Condition import Condition
from storage.functions.DataWrite import DataWrite
from storage.functions.DataRetrieval import DataRetrieval
from storage.functions.DataDeletion import DataDeletion
from datetime import datetime
from functions.ExecutionResult import ExecutionResult
from core.FailureRecovery import FailureRecovery
import random

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
start_log = ExecutionResult(
    transaction_id=transaction_id,
    timestamp=datetime.now(),
    message="Transaction Started",
    data_before=None,
    data_after=None,
    query="START"
)
# Convert ExecutionResult to WALLogEntry
recovery.write_log(start_log)

# Insert students
full_name = generate_name()
gpa = round(random.uniform(2.0, 4.0), 2)
data_write = DataWrite(
    table="students",
    columns=['StudentID', 'FullName', 'GPA'],
    new_value=[1, full_name, gpa],
    conditions=[]  # No conditions for insert
)
insert_log = ExecutionResult(
    transaction_id=transaction_id,
    timestamp=datetime.now(),
    message="Insert Operation",
    data_before=None,
    data_after=data_write,
    query="INSERT INTO example_table"
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
    transaction_id=transaction_id,
    timestamp=datetime.now(),
    message="Insert Operation",
    data_before=None,
    data_after=data_write,
    query="INSERT INTO courses"
)
recovery.write_log(insert_log)

print("-----------------------------------")
print(storage_engine.buffer_manager.buffer_pool)
print("-----------------------------------")

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
    message="UPDATE Operation",
    data_before=None,
    data_after=data_update,
    query="UPDATE courses"
)
recovery.write_log(update_log)