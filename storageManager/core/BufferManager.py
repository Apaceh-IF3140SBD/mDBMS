import struct
import sys
import os
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "storage"))

from typing import Dict, List, Tuple
from storageManager.core.TableSchema import TableSchema
from storageManager.functions.BPlusTree import BPlusTree
from storageManager.utils.DataBlock import DataBlock
# import os

class BufferManager:
    def __init__(self, buffer_size: int, schemas: Dict[str, TableSchema]):
        self.buffer_size = buffer_size # buffer size means how many blocks could fit in the buffer, just adjust it here
        self.schemas = schemas
        self.buffer_pool: Dict[Tuple[str, int], DataBlock] = {} # current buffer condition (schema_name, block_id)
        self.block_usage: List[Tuple[str, int]] = [] # queue for lru
        self.bin_path = os.path.join(os.path.dirname(__file__), "../bin")

    def read_block(self, table_name, block_id):
        key = (table_name, block_id)
        if key in self.buffer_pool: # if the block existed in buffer_pool
            self.block_usage.remove(key)
            self.block_usage.append(key) # update lru table
            return self.buffer_pool[key]
        else:
            # load from disk
            block = self.load_block_from_disk(table_name, block_id)

            # print("tes", block)

            self.add_block_to_buffer(key, block)
            return block

    def write_block(self, table_name, block_id, block: DataBlock):
        key = (table_name, block_id)
        self.buffer_pool[key] = block
        block.is_dirty = True
        if key not in self.block_usage:
            self.block_usage.append(key)
        else:
            self.block_usage.remove(key)
            self.block_usage.append(key)
        if len(self.buffer_pool) > self.buffer_size:
            self.evict_block()

    def delete_block(self, table_name: str, block_id: int):
        key = (table_name, block_id)

        if key in self.buffer_pool: # check if exist in pool
            self.buffer_pool.pop(key)
            if key in self.block_usage:
                self.block_usage.remove(key) # also remove from lru list
    
        # delete also from disk
        file_path = f"{self.bin_path}/{table_name}_block_{block_id}.bin"
        if os.path.exists(file_path):
            os.remove(file_path)

    def add_block_to_buffer(self, key, block: DataBlock): 
        if len(self.buffer_pool) >= self.buffer_size: # exceed size buffer then remove least used block
            self.evict_block()
        self.buffer_pool[key] = block
        # print("abtdb", block)
        self.block_usage.append(key)

    def evict_block(self): # func to remove least used block
        # evict oldest block (index 0)
        evict_key = self.block_usage.pop(0)
        evict_block = self.buffer_pool.pop(evict_key)
        # print("epicT", evict_block)
        if evict_block.is_dirty:
            self.write_block_to_disk(evict_key[0], evict_key[1], evict_block)
    
    def flush_all_block(self): # FOR TESTING PURPOSE: to write all thing in buffer into disk
        for (table_name, block_id), block in self.buffer_pool.items():
            if block.is_dirty:
                self.write_block_to_disk(table_name, block_id, block)

    def load_block_from_disk(self, table_name, block_id): #output action
        file_path = f"{self.bin_path}/{table_name}_block_{block_id}.bin"
        if os.path.exists(file_path):
            with open(file_path, 'rb') as f:
                block_size_data = f.read(4)
                block_size = struct.unpack("I", block_size_data)[0]

                row_count_data = f.read(4)
                if not row_count_data:
                    raise ValueError("Unexpected EOF: Block size information is missing.")

                row_count = struct.unpack("I", row_count_data)[0]

                block_data = f.read()
            block = DataBlock(block_id, self.schemas[table_name])
            block.from_bytes(block_data, row_count)
            return block
        else:
            return DataBlock(block_id, self.schemas[table_name])

    def write_block_to_disk(self, table_name, block_id, block: DataBlock): # input action
        file_path = f"{self.bin_path}/{table_name}_block_{block_id}.bin"
        with open(file_path, 'wb') as f:
            f.write(block.to_bytes())
        block.is_dirty = False

    def add_table_schema(self, table_name: str, schema: TableSchema):
        if table_name in self.schemas:
            print(f"Schema for table '{table_name}' already exists in BufferManager.")
            return
        self.schemas[table_name] = schema
        print(f"Schema for table '{table_name}' added to BufferManager.")