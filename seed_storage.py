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
        table_name="student",
        columns={
            "StudentID": "int",
            "FullName": "char(50)",
            # "Nickname": "varchar(50)",
            "GPA": "float"
        }
    )

    course_schema = TableSchema(
        table_name="course",
        columns={
            "CourseID": "int",
            "Year": "int",
            "CourseName": "varchar(50)",
            "CourseDescription": "varchar(65535)"
        }
    )

    attends_schema = TableSchema(
        table_name="attends",
        columns={
            "StudentID": "int",
            "CourseID": "int",
        }
    )

    # student_course_schema = TableSchema(
    #     table_name="students_courses_relation",
    #     columns={
    #         "StudentID": "int",
    #         "Year": "int",
    #         "CourseID": "int"
    #     }
    # )

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

    data_write = DataWrite(
        table="student",
        columns=["StudentID", "FullName", "Nickname", "GPA"],
        new_value=[1, 'Alice Johnson', 3.8],
        conditions=[]
    )
    storage_engine.insert(data_write)

    data_write = DataWrite(
        table="student",
        columns=["StudentID", "FullName", "Nickname", "GPA"],
        new_value=[2, 'Bob Smith', 3.4],
        conditions=[]
    )
    storage_engine.insert(data_write)

    data_write = DataWrite(
        table="student",
        columns=["StudentID", "FullName", "Nickname", "GPA"],
        new_value=[3, 'Charlie Brown', 2.9],
        conditions=[]
    )
    storage_engine.insert(data_write)

    data_write = DataWrite(
        table="course",
        columns=["CourseID", "Year", "CourseName", "CourseDescription"],
        new_value=[101, 2024, 'Introduction to Databases', 'A foundational course on database system and SQL'],
        conditions=[]
    )
    storage_engine.insert(data_write)

    data_write = DataWrite(
        table="course",
        columns=["CourseID", "Year", "CourseName", "CourseDescription"],
        new_value=[102, 2024, 'Data Structures', 'An in-depth course on algorithms and data structures'],
        conditions=[]
    )
    storage_engine.insert(data_write)

    data_write = DataWrite(
        table="course",
        columns=["CourseID", "Year", "CourseName", "CourseDescription"],
        new_value=[103, 2024, 'Oprating Systems', 'A course on operating system concepts'],
        conditions=[]
    )
    storage_engine.insert(data_write)

    data_write = DataWrite(
        table="attends",
        columns=["StudentID", "CourseID"],
        new_value=[1, 101],
        conditions=[]
    )
    storage_engine.insert(data_write)
    
    data_write = DataWrite(
        table="attends",
        columns=["StudentID", "CourseID"],
        new_value=[1, 102],
        conditions=[]
    )
    storage_engine.insert(data_write)

    data_write = DataWrite(
        table="attends",
        columns=["StudentID", "CourseID"],
        new_value=[2, 101],
        conditions=[]
    )
    storage_engine.insert(data_write)

    data_write = DataWrite(
        table="attends",
        columns=["StudentID", "CourseID"],
        new_value=[3, 103],
        conditions=[]
    )
    storage_engine.insert(data_write)

    buffer_manager.flush_all_block()
    print("Seeding completed and data flushed to disk.")
    
    # for student_id in range(1, 50):
    #     full_name = generate_name()
    #     # nick_name = "nick_" + str(student_id)
    #     gpa = round(random.uniform(2.0, 4.0), 2)
    #     data_write = DataWrite(
    #         table="student",
    #         columns=["StudentID", "FullName", "Nickname", "GPA"],
    #         new_value=[student_id, full_name, gpa],
    #         conditions=[]
    #     )
    #     storage_engine.insert(data_write)

    # for course_id in range(1, 50):
    #     course_name = generate_course_name()
    #     year = random.randint(2010, 2024)
    #     course_description = f"This is a description for {course_name}."
    #     data_write = DataWrite(
    #         table="course",
    #         columns=["CourseID", "Year", "CourseName", "CourseDescription"],
    #         new_value=[course_id, year, course_name, course_description],
    #         conditions=[]
    #     )
    #     storage_engine.insert(data_write)

    # for i in range(1, 50):
    #     student_id = random.randint(1, 50)
    #     course_id =  random.randint(1,50)
    #     data_write = DataWrite(
    #         table="attends",
    #         columns=["StudentID", "CourseID"],
    #         new_value=[student_id, course_id],
    #         conditions=[]
    #     )
    #     storage_engine.insert(data_write)

    # buffer_manager.flush_all_block()
    # print("Seeding completed and data flushed to disk.")

if __name__ == "__main__":
    seed_storage()
