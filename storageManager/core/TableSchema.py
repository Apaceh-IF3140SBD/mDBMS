import struct
import re
import sys
import os
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "storage"))

from typing import Dict

from storageManager.functions.BPlusTree import BPlusTree

class TableSchema: 
    def __init__(self, table_name, columns):
        self.table_name = table_name
        self.columns = columns # expected type: Dictionary{col_name: col_type}
        self.indexes : Dict[str, str] = {} # change to str
    
    # function to_bytes to serialiize table schema into binary format
    def to_bytes(self):
        # step: save table name length, table name, columns name length - name
        # imagine struct in C
        # struct bin_schema { uint table_name_length, char* table_name, uint col_name len, char* col_name name, char* col_type (fixed padding - 20bytes )}

        # init struct table scheme metadata
        bin_schema = struct.pack("I", len(self.table_name))
        bin_schema += self.table_name.encode("utf-8")

        # add column's metadata
        bin_schema += struct.pack("I", len(self.columns))
        for col_name, col_type in self.columns.items():
            bin_schema += struct.pack("I", len(col_name))
            bin_schema += col_name.encode("utf-8")

            fixed_col_type = col_type.ljust(20, '\x00')
            bin_schema += fixed_col_type.encode("utf-8")

        return bin_schema

    # funciton from_bytes to read binary_data to SchemaInfo
    # reverse action from to_bytes
    @staticmethod
    def from_bytes(binary_data):
        offset = 0
        len_table_name = struct.unpack_from("I", binary_data, offset)[0]

        offset += 4
        table_name = binary_data[offset:(offset + len_table_name)].decode("utf-8")
        offset += len_table_name
        
        col_count = struct.unpack_from("I", binary_data, offset)[0]
        offset += 4

        columns = {}
        for _ in range (col_count):
            col_name_len = struct.unpack_from("I", binary_data, offset)[0]
            offset += 4

            col_name = binary_data[offset:(offset + col_name_len)].decode("utf-8")
            offset += col_name_len

            col_type = binary_data[offset:(offset + 20)].strip(b'\x00').decode("utf-8")
            offset += 20
            columns[col_name] = col_type

        return TableSchema(table_name, columns)
    
    def validate_row(self, row):
        for col_name, value in row.items():
            col_type = self.columns[col_name]

            if col_type.startswith("varchar"):
                max_len = int(re.match(r"varchar\((\d+)\)", col_type).group(1))
                if len(value) > max_len:
                    raise ValueError(f"Length of '{col_name}' exceeds maximum length of {max_len} chars.")
                elif col_type.startswith("char"):
                    char_len = int(re.match(r"char\((\d+)\)", col_type).group(1))
                    if len(value) != char_len:
                        raise ValueError(f"Length of '{col_name}' must be exactly {char_len} chars.")
                
    def calc_tuple_size_schema(self) -> int:
        size = 0

        for _, col_type in self.columns.items():
            if col_type.startswith("varchar"):
                max_len = int(col_type[col_type.find("(") + 1 : col_type.find(")")])
                size += (4 + max_len)  # Length prefix + actual string length
            elif col_type.startswith("char"):
                char_len = int(col_type[col_type.find("(") + 1 : col_type.find(")")])
                size += char_len  # Fixed size without prefix
            elif col_type == "int" or col_type == "float":
                size += 4 
            else:
                raise ValueError(f"Unsupported column type: {col_type}")

        return size
    
    def get_index_name(self, column_name: str) -> str:
        return self.indexes.get(column_name, "")

# dev explain:
# 1. dev made the type as fixed len, which is allocated 20 bytes, since the longest type len is 14 sized - varchar(65535)