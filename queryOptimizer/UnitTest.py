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
    table_name="Student",
    columns={
        "StudentID": "int",
        "FullName": "varchar(50)",
        "GPA": "float"
    }
)

course_schema = TableSchema(
    table_name="Course",
    columns={
        "CourseID": "int",
        "Year": "int",
        "CourseName": "varchar(50)",
        "CourseDescription": "varchar(150)"
    }
)

attends_schema = TableSchema(
    table_name="Attends",
    columns={
        "StudentID": "int",
        "CourseID": "int"
    }
)

schemas = {"students": student_schema}
buffer_manager = BufferManager(buffer_size=5, schemas=schemas)
storage_engine = StorageEngine(buffer_manager)

storage_engine.create_table(student_schema)
storage_engine.create_table(course_schema)
storage_engine.create_table(attends_schema)

def generate_name():
    first_names = ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve', 'Frank', 'Grace', 'Hank', 'Ivy', 'Jack']
    last_names = ['Smith', 'Johnson', 'Williams', 'Jones', 'Brown', 'Davis', 'Miller', 'Wilson', 'Moore', 'Taylor']
    return f"{random.choice(first_names)} {random.choice(last_names)}"

def generate_course_name():
    subjects = ['Math', 'Science', 'History', 'Art', 'Music', 'Literature', 'Physics', 'Chemistry', 'Biology', 'Economics']
    levels = ['101', '201', '301', '401']
    return f"{random.choice(subjects)} {random.choice(levels)}"

for student_id in range(1, 51):
    full_name = generate_name()
    gpa = round(random.uniform(2.0, 4.0), 2)
    storage_engine.insert(
        DataWrite(
            table="students",
            columns=["StudentID", "FullName", "GPA"],
            new_value=[student_id, full_name, gpa],
            conditions=[]
        )
    )

for course_id in range(1, 51):
    course_name = generate_course_name()
    year = random.randint(2000, 2024)
    description = f"Description of {course_name}"
    storage_engine.insert(
        DataWrite(
            table="Course",
            columns=["CourseID", "Year", "CourseName", "CourseDescription"],
            new_value=[course_id, year, course_name, description],
            conditions=[]
        )
    )

for _ in range(50):
    student_id = random.randint(1, 50)
    course_id = random.randint(1, 50)
    storage_engine.insert(
        DataWrite(
            table="Attends",
            columns=["StudentID", "CourseID"],
            new_value=[student_id, course_id],
            conditions=[]
        )
    )

storage_engine.buffer_manager.flush_all_block()
print("Dummy database setup complete.")

optimize_engine = OptimizationEngine(storage_engine, schemas)
query_tree = QueryTree()
cost = QueryCost(storage_engine, schemas)

test_queries = [
    "SELECT FullName, Year FROM (Student NATURAL JOIN Course) NATURAL JOIN Attends WHERE Year <= 13 OR Year > 5 AND Year > 100 ORDER BY Year ASC LIMIT 10",
    "SELECT Student.StudentID, Student.FullName, Course.CourseName FROM Student JOIN Course ON Student.StudentID = Course.CourseID WHERE Student.GPA > 18 AND Course.Year > 3;",
]


for i, query in enumerate(test_queries, start=1):
    print(f"Testing query {i}: {query}")

    initial_tree = optimize_engine.parse_query(query)
    initial_cost = optimize_engine.get_cost(initial_tree)


    optimized_tree = optimize_engine.optimize_query(query)
    optimized_cost = optimize_engine.get_cost(optimized_tree)

    print(f"Initial cost: {initial_cost}, Optimized cost: {optimized_cost}")

    if optimized_cost <= initial_cost:
        print(f"Query {i} optimization SUCCESS: Cost reduced from {initial_cost} to {optimized_cost}\n")
    else:
        print(f"Query {i} optimization FAILED: Cost not reduced\n")
