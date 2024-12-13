import os
import random
from storageManager.core.BufferManager import BufferManager
from storageManager.core.StorageEngine import StorageEngine
from storageManager.core.TableSchema import TableSchema
from storageManager.functions.DataWrite import DataWrite

def seed_storage():
    bin_dir = os.path.join(os.path.dirname(__file__), "bin")
    if not os.path.exists(bin_dir):
        os.makedirs(bin_dir)
    
    schemas = {}
    buffer_manager = BufferManager(buffer_size=5, schemas=schemas)
    storage_engine = StorageEngine(buffer_manager)

    student_schema = TableSchema(
        table_name="students",
        columns={
            "StudentID": "int",
            "FullName": "char(50)",
            "Nickname": "varchar(50)",
            "GPA": "float"
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

    student_course_schema = TableSchema(
        table_name="students_courses_relation",
        columns={
            "StudentID": "int",
            "Year": "int",
            "CourseID": "int"
        }
    )

    storage_engine.create_table(student_schema)
    storage_engine.create_table(course_schema)
    storage_engine.create_table(student_course_schema)

    def generate_name():
        first_names = ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve', 'Frank', 'Grace', 'Hank', 'Ivy', 'Jack']
        last_names = ['Smith', 'Johnson', 'Williams', 'Jones', 'Brown', 'Davis', 'Miller', 'Wilson', 'Moore', 'Taylor']
        return f"{random.choice(first_names)} {random.choice(last_names)}"

    def generate_course_name():
        subjects = ['Math', 'Science', 'History', 'Art', 'Music', 'Literature', 'Physics', 'Chemistry', 'Biology', 'Economics']
        levels = ['101', '201', '301', '401']
        return f"{random.choice(subjects)} {random.choice(levels)}"

    for student_id in range(1, 10):
        full_name = generate_name()
        nick_name = "nick_" + str(student_id)
        gpa = round(random.uniform(2.0, 4.0), 2)
        data_write = DataWrite(
            table="students",
            columns=["StudentID", "FullName", "Nickname", "GPA"],
            new_value=[student_id, full_name, nick_name, gpa],
            conditions=[]
        )
        storage_engine.insert(data_write)

    for course_id in range(1, 3):
        course_name = generate_course_name()
        year = random.randint(2010, 2024)
        course_description = f"This is a description for {course_name}."
        data_write = DataWrite(
            table="courses",
            columns=["CourseID", "Year", "CourseName", "CourseDescription"],
            new_value=[course_id, year, course_name, course_description],
            conditions=[]
        )
        storage_engine.insert(data_write)

    buffer_manager.flush_all_block()
    print("Seeding completed and data flushed to disk.")

if __name__ == "__main__":
    seed_storage()
