import unittest
from core.StorageEngine import StorageEngine
from core.TableSchema import TableSchema
from core.BufferManager import BufferManager
from functions.Condition import Condition
from functions.DataRetrieval import DataRetrieval
from functions.DataWrite import DataWrite
from functions.DataDeletion import DataDeletion
import random
import os
import time

class Test1(unittest.TestCase):
    def setUp(self):
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

        buffer_manager = BufferManager(buffer_size=5, schemas={})
        self.storage_engine = StorageEngine(buffer_manager)

        student_schema = TableSchema(
            table_name="students",
            columns={
                "StudentID": "int",
                "FullName": "varchar(50)",
                "Nickname": "char(50)",
                "GPA": "float"
            }
        )
        self.storage_engine.create_table(student_schema)

        for student_id in range(1, 500):
            full_name = f"Student{student_id}"
            nick_name = f"Nick{student_id}"
            gpa = round(random.uniform(2.0, 4.0), 2)
            data_write = DataWrite(
                table="students",
                columns=['StudentID', 'FullName', 'Nickname','GPA'],
                new_value=[student_id, full_name, nick_name, gpa],
                conditions=[]
            )
            self.storage_engine.insert(data_write)

    def test_create_index_and_select(self):
        self.storage_engine.set_index("students", "GPA", "hash")

        condition = Condition(column="GPA", operation="=", operand=3.5)
        data_retrieve = DataRetrieval(
            table="students",
            columns=["StudentID", "FullName", "GPA"],
            conditions=[condition],
            index="hash"
        )
        results = self.storage_engine.select(data_retrieve)
        self.assertTrue(all(row[2] == 3.5 for row in results))
        print(f"Select with index results: {results}")

    def test_update_with_index(self):
        condition = Condition(column="GPA", operation="=", operand=3.5)
        data_update = DataWrite(
            table="students",
            columns=["GPA"],
            new_value=[4.0],
            conditions=[condition]
        )
        affected_rows = self.storage_engine.update(data_update)
        self.assertTrue(affected_rows > 0)
        print("Update affected row", affected_rows)

        condition = Condition(column="GPA", operation="=", operand=4.0)
        data_retrieve = DataRetrieval(
            table="students",
            columns=["StudentID", "FullName", "GPA"],
            conditions=[condition],
            index="hash"
        )
        results = self.storage_engine.select(data_retrieve)
        self.assertTrue(all(row[2] == 4.0 for row in results))
        print(f"Update results: {results}")

    def test_delete_with_conditions(self):
        condition = Condition(column="GPA", operation="<", operand=3.5)
        data_deletion = DataDeletion(
            table="students",
            conditions=[condition]
        )

        affected_rows = self.storage_engine.delete(data_deletion)
        self.assertTrue(affected_rows > 0)

        condition = Condition(column="GPA", operation="<", operand=3.5)

        data_retrieve = DataRetrieval(
            table="students",
            columns=["StudentID", "FullName", "GPA"],
            conditions=[condition],
            index=""
        )

        results = self.storage_engine.select(data_retrieve)
        self.assertEqual(len(results), 0)

    def test_insert_and_verify(self):
        new_records = [
            [10001, "NewStudent1", "NewNick1", 3.8],
            [10002, "NewStudent2", "NewNick2", 3.9]
        ]
        for record in new_records:
            data_write = DataWrite(
                table="students",
                columns=['StudentID', 'FullName', 'Nickname','GPA'],
                new_value=record,
                conditions=[]
            )
            self.storage_engine.insert(data_write)

        condition = Condition(column="StudentID", operation=">=", operand=10001)
        data_retrieve = DataRetrieval(
            table="students",
            columns=["StudentID", "FullName", "GPA"],
            conditions=[condition],
            index=""
        )
        results = self.storage_engine.select(data_retrieve)
        self.assertEqual(len(results), len(new_records))
        self.assertEqual(results[0][1], "NewStudent1")
        self.assertEqual(results[1][1], "NewStudent2")
        print(f"Insert results: {results}")

    def test_hash_index_vs_full_scan_performance(self):
        self.storage_engine.set_index("students", "GPA", "hash")

        random_gpa = round(random.uniform(2.0, 4.0), 2)
        condition = Condition(column="GPA", operation="=", operand=random_gpa)

        data_retrieve_full_scan = DataRetrieval(
            table="students",
            columns=["StudentID", "FullName", "GPA"],
            conditions=[condition],
            index=""
        )
        start_time_full_scan = time.time()
        results_full_scan = self.storage_engine.select(data_retrieve_full_scan)
        full_scan_time = time.time() - start_time_full_scan

        data_retrieve_hash_index = DataRetrieval(
            table="students",
            columns=["StudentID", "FullName", "GPA"],
            conditions=[condition],
            index="hash"
        )
        start_time_hash_index = time.time()
        results_hash_index = self.storage_engine.select(data_retrieve_hash_index)
        hash_index_time = time.time() - start_time_hash_index

        self.assertEqual(results_full_scan, results_hash_index)

        print(f"Full scan time: {full_scan_time:.6f} seconds")
        print(f"Hash index time: {hash_index_time:.6f} seconds")
        print(f"Results: {results_hash_index}")

        self.assertTrue(
            hash_index_time < full_scan_time,
            "Hash index query should be faster than full table scan"
        )


if __name__ == "__main__":
    unittest.main()