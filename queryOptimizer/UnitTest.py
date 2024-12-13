import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '../storageManager'))
sys.path.append(os.path.join(os.path.dirname(__file__), 'classes'))

from storageManager.core.BufferManager import BufferManager
from storageManager.core.StorageEngine import StorageEngine
from storageManager.core.TableSchema import TableSchema
from storageManager.functions.Condition import Condition
from storageManager.functions.DataWrite import DataWrite
from storageManager.functions.DataRetrieval import DataRetrieval
from storageManager.functions.DataDeletion import DataDeletion
import random
from Query import ParsedQuery, QueryTree
from TreeManager import TreeManager
from OptimizationEngine import OptimizationEngine


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

for student_id in range(1, 4):
    full_name = generate_name()
    gpa = round(random.uniform(2.0, 4.0), 2)
    data_write = DataWrite(
        table="students",
        columns=['StudentID', 'FullName', 'GPA'],
        new_value=[student_id, full_name, gpa],
        conditions=[]
    )
    storage_engine.insert(data_write)
    print(f"Inserted student: {student_id}, {full_name}, {gpa}")

storage_engine.buffer_manager.flush_all_block()
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


query = "SELECT FullName, Year FROM (student NATURAL JOIN student_course) NATURAL JOIN Proyek WHERE Year <= 13 AND Year > '100' OR Year < 13 AND Year > 100 ORDER BY A ASC LIMIT 10;"
query2 = "SELECT Nama FROM (SELECT Nama, Jabatan FROM (SELECT Nama, Jabatan, Gaji FROM Pegawai JOIN Hai));"
query3 = "SELECT Nama FROM student JOIN student_course ON StudentId = students ;"
query4 = "SELECT Nama FROM student, student_course WHERE StudentId = students ;"
query4 = "SELECT FullName, Year Year FROM students JOIN students_courses_relation ON StudentId = students WHERE StudentId = 10;"
query5 = "SELECT FullName FROM students, students_courses_relation WHERE StudentID = StudentId"

optimizeEngine = OptimizationEngine(storage_engine, schemas)
query_tree = QueryTree()
tree_mng = TreeManager(storage_engine, schemas)
optimizeEngine.optimize_query(query)
