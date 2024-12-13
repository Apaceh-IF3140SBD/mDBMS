import sys
import os
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "storage"))

import struct
from typing import List, Dict, Tuple
from storageManager.functions.HashIndex import HashIndex
from storageManager.functions.BPlusTree import BPlusTree
from storageManager.functions.Condition import Condition
from storageManager.functions.DataRetrieval import DataRetrieval
from storageManager.functions.DataWrite import DataWrite
from storageManager.functions.DataDeletion import DataDeletion
from storageManager.functions.Statistic import Statistic
from storageManager.core.TableSchema import TableSchema
from storageManager.core.BufferManager import BufferManager
from storageManager.utils.Calc import Calc
from storageManager.utils.DataBlock import DataBlock
import math
import os
import re

from storageManager.utils.SchemaManager import SchemaManager


class StorageEngine:
    def __init__(self, buffer_manager: BufferManager):
        self.buffer_manager = buffer_manager
        self.schemas = buffer_manager.schemas
        self.bin_path = os.path.join(os.path.dirname(__file__), "../../bin")

    def select(self, data_retrieval: DataRetrieval):
        conditions: List[Condition] = data_retrieval.conditions
        columns = data_retrieval.columns
        table_name = data_retrieval.table
        index_type = data_retrieval.index

        use_index = False
        indexed_conditions: List[Condition] = []

        for condition in conditions:
            index_name = self.schemas[table_name].get_index_name(condition.column)
            if index_name and condition.operation == "=": 
                indexed_conditions.append(condition)
                use_index = True

        block_ids = self.get_block_ids_for_table(table_name)
        # possibly unwritten data, but in buffer like after delete update or insert
        buffer_only_blocks = [
            block_id
            for (_, block_id) in self.buffer_manager.buffer_pool.keys()
            if block_id not in block_ids
        ]
        all_block_ids = block_ids + buffer_only_blocks

        results = []
        if (not use_index or index_type == ""):
            for block_id in all_block_ids:
                block: DataBlock = self.buffer_manager.read_block(table_name, block_id)
                for row in block.rows.values():
                    if self.evaluate_conditions(row, conditions, table_name):
                        results.append(self.project_columns(row, columns, table_name))
        elif use_index and index_type == "hash":
            hash_indexes: List[Tuple[Condition, HashIndex]] = []

            for condition in indexed_conditions:
                hash_index = HashIndex.load_from_file(
                    f"{self.bin_path}/{table_name}_hash_index_{condition.column}.bin"
                )
                hash_indexes.append((condition, hash_index))

            matching_offsets = set()
            for condition, hash_index in hash_indexes:
                key = condition.operand
                matched = hash_index.get(key)
                matching_offsets.update(matched)

            # print(matching_offsets)

            for offset, block_id in matching_offsets:
                block: DataBlock = self.buffer_manager.read_block(table_name, block_id)
                # print(block.rows)
                row = block.rows.get(offset)
                if row:
                    # print("pake offset ni", row)
                    if self.evaluate_conditions(row, conditions, table_name):
                        results.append(self.project_columns(row, columns, table_name))
                else:
                    for row in block.rows.values():
                        if self.evaluate_conditions(row, conditions, table_name):
                            results.append(self.project_columns(row, columns, table_name))

        return results

    
    def insert(self, data_write: DataWrite):
        table_name = data_write.table
        values = data_write.new_value

        new_record = tuple(values)

        tuple_size = self.calc_tuple_size(table_name, new_record)
        max_block_id_buffer = -1     
        
        # check in buffer first
        for (buffer_table_name, block_id), block in self.buffer_manager.buffer_pool.items():
            # print ("new record size", tuple_size)
            # print ("current block", block.block_id, "size : ", block.calculate_current_block_size())

            if (max_block_id_buffer < block_id):
                max_block_id_buffer = block_id

            if buffer_table_name == table_name and block.is_possible_to_add(tuple_size):  
                highest_offset = max(block.rows.keys(), default=DataBlock.NEW_ROW_ID)
                tuple_with_highest_offset = block.rows[highest_offset] if highest_offset is not DataBlock.NEW_ROW_ID else DataBlock.NEW_ROW_ID
                tuple_with_highest_offset_size = self.calc_tuple_size(table_name, tuple_with_highest_offset) 
                new_offset = highest_offset + tuple_with_highest_offset_size     
                block.add_record(new_offset, new_record)
                self.buffer_manager.write_block(table_name, block_id, block)
                self.update_index(table_name, new_record, block_id, new_offset)
                return 1

        # find possible block to be inserted row (traverse harddisk)
        block_ids = self.get_block_ids_for_table(table_name)
        new_offset = 0
        for block_id in block_ids:
            block = self.buffer_manager.read_block(table_name, block_id)
            highest_offset = max(block.rows.keys(), default=DataBlock.NEW_ROW_ID)
            tuple_with_highest_offset = block.rows[highest_offset] if highest_offset is not DataBlock.NEW_ROW_ID else DataBlock.NEW_ROW_ID
            tuple_with_highest_offset_size = self.calc_tuple_size(table_name, tuple_with_highest_offset) 
            new_offset = highest_offset + tuple_with_highest_offset_size     
            if (block.is_possible_to_add(tuple_size)):
                block.add_record(new_offset, new_record)
                self.buffer_manager.write_block(table_name, block_id, block)
                self.update_index(table_name, new_record, block_id, new_offset)
                return 1
        
        new_block_id = max(max(block_ids) if block_ids else -1, max_block_id_buffer) + 1
        new_block = DataBlock(new_block_id, self.schemas[table_name])
        new_block.add_record(new_offset, new_record)
        self.buffer_manager.write_block(table_name, new_block_id, new_block)
        self.update_index(table_name, new_record, new_block_id, new_offset)
        return 1

    def update(self, data_write: DataWrite):
        table_name = data_write.table
        columns = data_write.columns
        values = data_write.new_value
        conditions = data_write.conditions

        schema = self.schemas[table_name]
        affected_row = 0

        block_ids = self.get_block_ids_for_table(table_name)
        # possibly unwritten data, but in buffer like after delete update or insert
        buffer_only_blocks = [
            block_id
            for (_, block_id) in self.buffer_manager.buffer_pool.keys()
            if block_id not in block_ids
        ]
        all_block_ids = block_ids + buffer_only_blocks

        for block_id in all_block_ids:
            block = self.buffer_manager.read_block(table_name, block_id)
            modified = False

            for offset, row in list(block.rows.items()):
                if self.evaluate_conditions(row, conditions, table_name):
                    updated_row = list(row)

                    for col_name, new_value in zip(columns, values):
                        col_index = list(schema.columns.keys()).index(col_name)
                        old_value = updated_row[col_index]
                        updated_row[col_index] = new_value

                        if col_name in schema.indexes:
                            self.update_index_on_update(
                                table_name, old_value, new_value, col_name, block_id, offset
                        )
                    
                    block.rows[offset] = tuple(updated_row)
                    affected_row += 1
                    modified = True

            if modified:
                self.buffer_manager.write_block(table_name, block_id, block)

        return affected_row
    
    def delete (self, data_deletion: DataDeletion):
        table_name = data_deletion.table
        conditions = data_deletion.conditions

        affected_rows = 0

        block_ids = self.get_block_ids_for_table(table_name)
        # possibly unwritten data, but in buffer like after delete update or insert
        buffer_only_blocks = [
            block_id
            for (_, block_id) in self.buffer_manager.buffer_pool.keys()
            if block_id not in block_ids
        ]
        all_block_ids = block_ids + buffer_only_blocks

        for block_id in all_block_ids:
            block = self.buffer_manager.read_block(table_name, block_id)
            initial_row_count = len(block.rows)

            for offset, row in list(block.rows.items()):
                if self.evaluate_conditions(row, conditions, table_name):
                    for col_name, value in zip(self.schemas[table_name].columns.keys(), row):
                        if col_name in self.schemas[table_name].indexes:
                            self.update_index_on_delete(table_name, value, col_name, block_id, offset)
                    del block.rows[offset]

            affected_rows += initial_row_count - len(block.rows)
            self.buffer_manager.write_block(table_name, block_id, block)
                
        return affected_rows

    # to get all block ids that exist for a specific table (by checking the bin file existence)
    def get_block_ids_for_table(self, table_name: str) -> List[int]:
        block_files = [
            f for f in os.listdir(self.bin_path) if re.match(f"{table_name}_block_(\\d+).bin", f)
        ]
        block_ids = [
            int(re.search(f"{table_name}_block_(\\d+).bin", file).group(1)) for file in block_files
        ]
        return sorted(block_ids)
    
    def evaluate_conditions(self, row, conditions: List[Condition], table_name: str):
        schema = self.schemas[table_name]
        for condition in conditions:
            col_index = list(schema.columns.keys()).index(condition.column)
            if not self.check_condition(row[col_index], condition):
                return False
        return True 
    
    def check_condition(self, value, condition: Condition):
        if condition.operation == "=":
            return value == condition.operand
        elif condition.operation == "<>":
            return value != condition.operand
        elif condition.operation == ">":
            return value > condition.operand
        elif condition.operation == ">=":
            return value >= condition.operand
        elif condition.operation == "<":
            return value < condition.operand
        elif condition.operation == "<=":
            return value <= condition.operand
        return False

    def project_columns(self, row, columns: List[str], table_name: str):
        schema = self.schemas[table_name]
        col_indices = [list(schema.columns.keys()).index(col) for col in columns]
        return tuple(row[idx] for idx in col_indices)

    def calc_tuple_size(self, table_name, tuple_record):
        format_str = "="
        tuple_size = 0

        for idx, (col_name, col_type) in enumerate(self.schemas[table_name].columns.items()):
            value = tuple_record[idx]

            if col_type.startswith("varchar"):
                tuple_size += 4 # add int for length of s
                if isinstance(value, str):
                    encoded_value = value.encode('utf-8')
                elif isinstance(value, bytes):
                    encoded_value = value
                else:
                    raise ValueError(f"Expected string or bytes for column '{col_name}', got {type(value)}.")

                str_len = len(encoded_value)
                format_str += f"{str_len}s"
            elif col_type.startswith("char"):
                max_len = int(col_type[col_type.find("(") + 1 : col_type.find(")")])
                if isinstance(value, str):
                    encoded_value = value.encode('utf-8').ljust(max_len, b'\x00')  # Pad to max_len
                elif isinstance(value, bytes):
                    encoded_value = value.ljust(max_len, b'\x00')  # Pad to max_len if value is bytes
                else:
                    raise ValueError(f"Expected string or bytes for column '{col_name}', got {type(value)}.")
                
                if len(encoded_value) > max_len:
                    raise ValueError(f"Value for column '{col_name}' exceeds defined CHAR({max_len}) length.")
                
                # `char` is fixed-size, so pad it to `max_len`
                tuple_size += max_len
                format_str += f"{max_len}s"
            else:
                format_char = Calc.get_format_char(self, col_type)
                if not format_char:
                    raise ValueError(f"Unsupported column type '{col_type}' for column '{col_name}'.")
                format_str += format_char
        # print(format_str)
        tuple_size += struct.calcsize(format_str)
        return tuple_size
    
    def create_table(self, schema: TableSchema):
        table_name = schema.table_name
        if table_name in self.schemas:
            print(f"Table '{table_name}' already exists.")
            return

        self.schemas[table_name] = schema

        try:
            bin_dir = "bin"
            if not os.path.exists(bin_dir):
                os.makedirs(bin_dir)

            SchemaManager.save_schema(schema, f"{bin_dir}/{table_name}_schema.bin")
            print(f"Schema for table '{table_name}' created and saved.")
        except Exception as e:
            print(f"Failed to save schema for table '{table_name}': {e}")

    def load_all_table(self):
        for file_name in os.listdir(self.bin_path):
            if file_name.endswith("_schema.bin"):
                table_name = file_name.rsplit("_schema.bin", 1)[0]
                file_path = os.path.join(self.bin_path, file_name)
                try:
                    schema = SchemaManager.load_schema(file_path)
                    self.schemas[table_name] = schema
                except Exception as e:
                    print(f"Failed to load schema for {file_name}: {e}")

    def get_stats(self, table_name: str) -> Statistic:
        schema = self.schemas[table_name]

        block_ids = set(self.get_block_ids_for_table(table_name))
        buffer_block_ids = {
            block_id for (buffer_table_name, block_id) in self.buffer_manager.buffer_pool.keys()
            if buffer_table_name == table_name
        }

        all_block_ids = block_ids.union(buffer_block_ids)

        n_r = 0
        b_r = len(all_block_ids)
        distinct_values = {col_name: set() for col_name in schema.columns.keys()}

        for block_id in all_block_ids:
            if (table_name, block_id) in self.buffer_manager.buffer_pool:
                block: DataBlock = self.buffer_manager.buffer_pool[(table_name, block_id)]
                n_r += len(block.rows)

                for row in block.rows.values():
                    for col_name, value in zip(schema.columns.keys(), row):
                        distinct_values[col_name].add(value)
            else:
                file_path = os.path.join(self.bin_path, f"{table_name}_block_{block_id}.bin")
                if os.path.exists(file_path):
                    with open(file_path, 'rb') as f:
                        f.seek(4) # skip reading block size

                        row_count_data = f.read(4) 
                        row_count = struct.unpack("I", row_count_data)[0]
                        n_r += row_count

                        block_data = f.read()
                        block = DataBlock(block_id, schema)
                        block.from_bytes(block_data, row_count)
                        for row in block.rows.values():
                            for col_name, value in zip(schema.columns.keys(), row):
                                distinct_values[col_name].add(value)

        l_r = schema.calc_tuple_size_schema()
        f_r = math.floor(DataBlock.BLOCK_SIZE / l_r)

        V_a_r = {col: len(values) for col, values in distinct_values.items()}   

        return Statistic(n_r, b_r, l_r, f_r, V_a_r)
    
    def set_index(self, table: str, column: str, index_type: str) -> None:
        schema = self.schemas[table]
        column_type = schema.columns[column]
        
        if column in schema.indexes:
            existing_index_type = schema.indexes[column]
            print(f"Index for column '{column}' already exists as '{existing_index_type}'.")
            return
        
        if (index_type.lower() == "hash"):
            index_file = f"{self.bin_path}/{table}_hash_index_{column}.bin"

            print(f"Creating hash index for column '{column}' in table '{table}'...")
            hash_index = HashIndex()

            block_ids = self.get_block_ids_for_table(table)
            # possibly unwritten data, but in buffer like after delete update or insert
            buffer_only_blocks = [
                block_id
                for (_, block_id) in self.buffer_manager.buffer_pool.keys()
                if block_id not in block_ids
            ]
            all_block_ids = block_ids + buffer_only_blocks

            col_index = list(schema.columns.keys()).index(column)

            for block_id in all_block_ids:
                block: DataBlock = self.buffer_manager.read_block(table, block_id)
                binary_data = block.to_bytes()
                row_offsets = block.calculate_offsets(binary_data, len(block.rows))

                for offset, row in zip(row_offsets, block.rows.values()):
                    key = row[col_index]  # Value of the indexed column
                    value = (offset, block_id)  # (Offset, Block ID)
                    # print("key", key)
                    # print("value", value)
                    hash_index.add(key, value)

            # hash_index.print_index()
            
            # Save the hash index to a file
            # print("coltype" ,HashIndex.column_type_to_number(column_type))
            hash_index.save_to_file(index_file, HashIndex.column_type_to_number(column_type))

            # new_hash_index = HashIndex.load_from_file(index_file)
            # print("=== BATAS SUCI ===")
            # new_hash_index.print_index()
            # print(f"Hash index saved to {index_file}.")
            schema.indexes[column] = "hash"
        else: 
            bPlusTree = BPlusTree()
            block_ids = self.get_block_ids_for_table(table)

            for block_id in block_ids:
                block = self.buffer_manager.read_block(table, block_id)
                index = 0
                for row in block.rows:
                    bPlusTree.insert(index, row)
                    index += 1

            bPlusTree.print_tree()
            schema.indexes[column] = "b+tree"
            print(f"B+ Tree index created at column '{column}' in table '{table}'.")

    def update_index(self, table_name, record, block_id, offset):
        schema = self.schemas[table_name]
        for col_name, value in zip(schema.columns.keys(), record):
            if col_name in schema.indexes:
                index_file = f"{self.bin_path}/{table_name}_hash_index_{col_name}.bin"
                hash_index = HashIndex.load_from_file(index_file)
                hash_index.add(value, (offset, block_id))
                hash_index.save_to_file(index_file, HashIndex.column_type_to_number(schema.columns[col_name]))

    def update_index_on_update(self, table_name, old_value, new_value, col_name, block_id, offset):
        schema = self.schemas[table_name]
        index_file = f"{self.bin_path}/{table_name}_hash_index_{col_name}.bin"
        hash_index = HashIndex.load_from_file(index_file)

        if hash_index.get(old_value):
            hash_index.remove(old_value, (offset, block_id))

        hash_index.add(new_value, (offset, block_id))

        hash_index.save_to_file(index_file, HashIndex.column_type_to_number(schema.columns[col_name]))


    def update_index_on_delete(self, table_name, value, col_name, block_id, offset):
        schema = self.schemas[table_name]
        index_file = f"{self.bin_path}/{table_name}_hash_index_{col_name}.bin"
        hash_index = HashIndex.load_from_file(index_file)

        if hash_index.get(value):
            hash_index.remove(value, (offset, block_id))
        hash_index.save_to_file(index_file, HashIndex.column_type_to_number(schema.columns[col_name]))
