import unittest
from core.StorageEngine import StorageEngine
from core.TableSchema import TableSchema
from core.BufferManager import BufferManager
from functions.Condition import Condition
from functions.DataRetrieval import DataRetrieval
from functions.DataWrite import DataWrite
from functions.DataDeletion import DataDeletion
import random
import time
import os

class TestCombinedOperations(unittest.TestCase):
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
        
        # if create new table
        delete_directory("bin")
        self.storage_engine.create_table(student_schema)

        # load existing table
        # self.storage_engine.load_all_table()

        for student_id in range(1, 101):
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

    def test_combined_operations(self):
        # 1. set index for GPA column (just for testing purpose)
        self.storage_engine.set_index("students", "GPA", "hash")

        condition_gpa_3_5 = Condition(column="GPA", operation="<", operand=3.5)
        data_retrieve_index = DataRetrieval(
            table="students",
            columns=["StudentID", "FullName", "GPA"],
            conditions=[condition_gpa_3_5],
            index="hash"
        )
        results_index = self.storage_engine.select(data_retrieve_index)
        self.assertTrue(all(row[2] < 3.5 for row in results_index))
        self.assertTrue(len(results_index), 102)
        print(f"Select with index results: {results_index}")

        condition_gpa_4_0 = Condition(column="GPA", operation="=", operand=4.0)
        data_retrieve_before_update = DataRetrieval(
            table="students",
            columns=["StudentID", "FullName", "GPA"],
            conditions=[condition_gpa_4_0],
            index="hash"
        )
        result_data_retrieve_before_update = self.storage_engine.select(data_retrieve_before_update)

        # 2. update gpa from <3.5 to 4.0
        data_update = DataWrite(
            table="students",
            columns=["GPA"],
            new_value=[4.0],
            conditions=[condition_gpa_3_5]
        )
        affected_rows = self.storage_engine.update(data_update)
        self.assertTrue(affected_rows > 0)
        print(f"Update affected rows: {affected_rows}")

        # Verify the update
        condition_gpa_4_0 = Condition(column="GPA", operation="=", operand=4.0)
        data_retrieve_updated = DataRetrieval(
            table="students",
            columns=["StudentID", "FullName", "GPA"],
            conditions=[condition_gpa_4_0],
            index="hash"
        )
        results_updated = self.storage_engine.select(data_retrieve_updated)
        self.assertTrue(len(result_data_retrieve_before_update) + affected_rows, len(results_updated))
        self.assertTrue(all(row[2] == 4.0 for row in results_updated))
        print(f"Update results: {results_updated}")

        # 3. delete all record that has gpa 4.0
        data_deletion = DataDeletion(
            table="students",
            conditions=[condition_gpa_4_0]
        )

        deleted_rows = self.storage_engine.delete(data_deletion)
        self.assertTrue(deleted_rows > 0)
        print(f"Deleted rows with GPA == 4: {deleted_rows}")

        # verify deletion, check if the deleted rows is same as updated previously
        data_retrieve_deleted = DataRetrieval(
            table="students",
            columns=["StudentID", "FullName", "GPA"],
            conditions=[condition_gpa_4_0],
            index=""
        )
        results_deleted = self.storage_engine.select(data_retrieve_deleted)
        self.assertEqual(deleted_rows, len(results_updated))
        print("Deletion verification passed: No records with GPA 4.0 found.")
        print("After deletion result:", results_deleted)

        # 4. insert two new record and verify those two
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

        condition_new_students = Condition(column="StudentID", operation=">=", operand=10001)
        data_retrieve_new = DataRetrieval(
            table="students",
            columns=["StudentID", "FullName", "GPA"],
            conditions=[condition_new_students],
            index=""
        )
        results_new = self.storage_engine.select(data_retrieve_new)
        self.assertEqual(len(results_new), len(new_records))
        self.assertEqual(results_new[0][1], "NewStudent1")
        self.assertEqual(results_new[1][1], "NewStudent2")
        print(f"Insert results: {results_new}")

        # 5. compare performance between hash index and full scan table

        # for hash index to outperform full table scan, need larger data
        for student_id in range(101, 701):
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
            
        condition_random_gpa = Condition(column="GPA", operation="=", operand=3.0)

        # Full table scan
        data_retrieve_full_scan = DataRetrieval(
            table="students",
            columns=["StudentID", "FullName", "GPA"],
            conditions=[condition_random_gpa],
            index=""
        )
        start_time_full_scan = time.time()
        results_full_scan = self.storage_engine.select(data_retrieve_full_scan)
        full_scan_time = time.time() - start_time_full_scan

        # Hash index scan
        data_retrieve_hash_index = DataRetrieval(
            table="students",
            columns=["StudentID", "FullName", "GPA"],
            conditions=[condition_random_gpa],
            index="hash"
        )
        start_time_hash_index = time.time()
        results_hash_index = self.storage_engine.select(data_retrieve_hash_index)
        hash_index_time = time.time() - start_time_hash_index

        # ensure with hash and without deliver same result
        self.assertEqual(results_full_scan, results_hash_index)

        print(f"Full scan time: {full_scan_time:.6f} seconds")
        print(f"Hash index time: {hash_index_time:.6f} seconds")
        print(f"Results from hash index: {results_hash_index}")

        # ensure hash index is faster
        self.assertTrue(
            hash_index_time <= full_scan_time,
            "Hash index query should be faster or equal to full table scan"
        )

if __name__ == "__main__":
    unittest.main()
