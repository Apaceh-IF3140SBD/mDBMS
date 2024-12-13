import unittest
import os
from core.BufferManager import BufferManager
from core.TableSchema import TableSchema
from core.StorageEngine import StorageEngine
from utils.DataBlock import DataBlock


class TestBufferManager(unittest.TestCase):
    def setUp(self):
        self.bin_path = os.path.join(os.path.dirname(__file__), "bin")
        
        self.schema = TableSchema(
            table_name="test_table",
            columns={"id": "int", "name": "varchar(50)", "age": "int"}
        )

        schemas = {}
        self.buffer_manager = BufferManager(buffer_size=3, schemas=schemas)
        self.storage_engine = StorageEngine(self.buffer_manager)
        self.storage_engine.create_table(self.schema)

    def tearDown(self):
        for file in os.listdir(self.bin_path):
            file_path = os.path.join(self.bin_path, file)
            if os.path.isfile(file_path):
                os.remove(file_path)


    def test_read_and_write_block(self):
        block = DataBlock(block_id=1, schema=self.schema)
        block.add_record(0, (1, "Alice", 25))
        block.add_record(1, (2, "Bob", 30))
        self.buffer_manager.write_block("test_table", 1, block)

        loaded_block = self.buffer_manager.read_block("test_table", 1)
        self.assertEqual(len(loaded_block.rows), 2)
        self.assertEqual(loaded_block.rows[0], (1, "Alice", 25))

    def test_evict_block(self):
        for i in range(4):
            block = DataBlock(block_id=i, schema=self.schema)
            block.add_record(0, (i, f"User{i}", 20 + i))
            self.buffer_manager.write_block("test_table", i, block)

        self.assertEqual(len(self.buffer_manager.buffer_pool), 3)
        self.assertNotIn(("test_table", 0), self.buffer_manager.buffer_pool)

        evicted_block_path = f"{self.bin_path}/test_table_block_0.bin"

        self.assertTrue(os.path.exists(evicted_block_path))

    def test_flush_all_blocks(self):
        for i in range(3):
            block = DataBlock(block_id=i, schema=self.schema)
            block.add_record(0, (i, f"User{i}", 20 + i))
            self.buffer_manager.write_block("test_table", i, block)

        self.buffer_manager.flush_all_block()

        for i in range(3):
            block_path = f"{self.bin_path}/test_table_block_{i}.bin"
            self.assertTrue(os.path.exists(block_path))

    def test_delete_block(self):
        block = DataBlock(block_id=1, schema=self.schema)
        block.add_record(0, (1, "Alice", 25))
        self.buffer_manager.write_block("test_table", 1, block)

        self.buffer_manager.delete_block("test_table", 1)

        self.assertNotIn(("test_table", 1), self.buffer_manager.buffer_pool)
        block_path = f"{self.bin_path}/test_table_block_1.bin"
        self.assertFalse(os.path.exists(block_path))

    def test_load_block_from_disk(self):
        block = DataBlock(block_id=1, schema=self.schema)
        block.add_record(0, (1, "Alice", 25))
        self.buffer_manager.write_block_to_disk("test_table", 1, block)

        loaded_block = self.buffer_manager.load_block_from_disk("test_table", 1)
        print(loaded_block.rows.items())
        self.assertEqual(len(loaded_block.rows), 1)
        self.assertEqual(loaded_block.rows[8], (1, "Alice", 25))


if __name__ == "__main__":
    unittest.main()
