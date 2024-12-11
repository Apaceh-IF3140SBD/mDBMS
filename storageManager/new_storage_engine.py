from core.BufferManager import BufferManager
from core.StorageEngine import StorageEngine
from core.TableSchema import TableSchema
from functions.Condition import Condition
from functions.DataWrite import DataWrite
from functions.DataRetrieval import DataRetrieval
from functions.DataDeletion import DataDeletion
import random
import os

def delete_directory(dir_path):
    if os.path.exists(dir_path):
        for root, dirs, files in os.walk(dir_path, topdown=False):
            for file in files:
                os.remove(os.path.join(root, file))
            for directory in dirs:
                os.rmdir(os.path.join(root, directory))
        os.rmdir(dir_path)
        print(f"Directory '{dir_path}' and its contents have been removed.")
    else:
        print(f"Directory '{dir_path}' does not exist.")

delete_directory("bin")

# Define table schemas
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

schemas = {}
buffer_manager = BufferManager(buffer_size=5, schemas=schemas)
storage_engine = StorageEngine(buffer_manager)

storage_engine.create_table(student_schema)
storage_engine.create_table(student_course_schema)
storage_engine.create_table(course_schema)

# Helper functions for generating random data
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

for student_id in range(1, 100):
    # full_name = generate_name()
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
    # storage_engine.buffer_manager.flush_all_block()  # This for testing purpose, is in order to write all thing from buffer to disk, flexible for failure recovery
    # print(f"Inserted student: {student_id}, {full_name}, {gpa}")

#testing char type
full_name = "Yusuf Rafi"
nick_name = "cupi"
gpa = round(random.uniform(2.0, 4.0), 2)
data_write = DataWrite(
    table="students",
    columns=['StudentID', 'FullName', 'Nickname','GPA'],
    new_value=[4, full_name,nick_name, gpa],
    conditions=[]
)
storage_engine.insert(data_write)
# print(f"Inserted student: {1}, {full_name}, {gpa}")

for student_id in range(1, 500):
    # full_name = generate_name()
    full_name = generate_name()
    nick_name = "cupi"
    gpa = round(random.uniform(2.0, 4.0), 2)
    data_write = DataWrite(
        table="students",
        columns=['StudentID', 'FullName', 'Nickname','GPA'],
        new_value=[student_id, full_name, nick_name, gpa],
        conditions=[]
    )
    # storage_engine.insert(data_write)
    # storage_engine.buffer_manager.flush_all_block()  # This for testing purpose, is in order to write all thing from buffer to disk, flexible for failure recovery
    # print(f"Inserted student: {student_id}, {full_name}, {gpa}")

# storage_engine.buffer_manager.flush_all_block()
print(storage_engine.buffer_manager.buffer_pool)

uc = Condition(column='GPA', operation='<', operand=10)
ctu = ['GPA']
nv = [0]

data_update = DataWrite(
    table='students',
    columns=ctu,
    new_value=nv,
    conditions=[uc]
)

stats = storage_engine.get_stats("students")
print(stats)

conditions_test = [Condition(column='FullName', operation='=', operand='Yusuf Rafi')]

columns = ["StudentID", "FullName", "GPA"]

import time

start_time = time.time()
# Without Index
data_retrieve = DataRetrieval(conditions=conditions_test, columns=columns, table="students")
selected_rows = storage_engine.select(data_retrieval=data_retrieve)
print(len(selected_rows))

end_time = time.time()
elapsed_time = end_time - start_time

print(f"Time taken for the select operation: {elapsed_time:.6f} seconds")

storage_engine.set_index("students", "FullName", "hash")
start_time = time.time()
# With hash index
data_retrieve = DataRetrieval(conditions=[], columns=columns, table="students", index="hash")
selected_rows = storage_engine.select(data_retrieval=data_retrieve)
print(len(selected_rows))

end_time = time.time()
elapsed_time = end_time - start_time

print(f"Time taken for the second select operation: {elapsed_time:.6f} seconds")

# Sudah terbukti, hash index lebih kenceng, kalau kasus dipaksa hehehe, sekian terimakasih

condition = Condition(column='GPA', operation='>', operand=3)
data_retrieve = DataRetrieval(conditions=[condition], columns=columns, table="students")
selected_rows = storage_engine.select(data_retrieval=data_retrieve)
print(len(selected_rows))

# data_deletion = DataDeletion(
#     table="students",
#     conditions=[condition]
# )
# deleted_data = storage_engine.delete(data_deletion)
# print("delete", deleted_data)

data_update = DataWrite(
    table="students",
    conditions=[Condition(column="GPA", operation=">", operand=3)],
    columns=["GPA"],
    new_value=[5]
)
updated_data = storage_engine.update(data_update)
print(updated_data)

data_retrieve = DataRetrieval(conditions=[condition], columns=columns, table="students")
selected_rows = storage_engine.select(data_retrieval=data_retrieve)
print(len(selected_rows))