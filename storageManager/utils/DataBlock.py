import struct
from typing import Dict, List
from storageManager.utils.Calc import Calc

class DataBlock:
    BLOCK_SIZE = 4096 # 4 KB , 4096 bytes
    NEW_ROW_ID = -1 

    def __init__(self, block_id, schema, rows=None):
        self.block_size = DataBlock.BLOCK_SIZE
        self.schema = schema
        self.block_id = block_id
        self.rows: Dict[int, tuple] = rows if rows is not None else {} # list of records in the blocks
        self.is_dirty = False # True if written to disk, and vice versa

    def calculate_current_block_size(self):
        total_size = 0

        for _, row in self.rows.items():
            for idx, (col_name, col_type) in enumerate(self.schema.columns.items()):
                value = row[idx]

                if col_type.startswith("varchar"):
                    if isinstance(value, str):
                        encoded_value = value.encode('utf-8')
                    elif isinstance(value, bytes):
                        encoded_value = value
                    else:
                        raise ValueError(f"Expected string or bytes for column '{col_name}', got {type(value)}.")

                    length = len(encoded_value)
                    # Size of length prefix (4 bytes) + size of the actual string
                    total_size += 4 + length
                elif col_type.startswith("char"):
                    # Handling char(N) type: Always fixed size N
                    max_len = int(col_type[col_type.find("(") + 1 : col_type.find(")")])
                    if isinstance(value, str):
                        encoded_value = value.encode('utf-8').ljust(max_len, b'\x00')  # Pad to max_len
                    elif isinstance(value, bytes):
                        encoded_value = value.ljust(max_len, b'\x00')  # Pad to max_len if value is bytes
                    else:
                        raise ValueError(f"Expected string or bytes for column '{col_name}', got {type(value)}.")
                    
                    # For char(N), the total size is fixed and equal to max_len
                    total_size += max_len
                else:
                    # Size of the fixed-size data type
                    total_size += struct.calcsize(Calc.get_format_char(self, col_type))

        return total_size + 8
    
    def add_record(self, offset, row):
        self.rows[offset] = row
        DataBlock.NEW_ROW_ID -= 1

    def to_bytes(self): ## current_block_size, row_count, content
        # content? 
        # char : (char) // {n}s
        # varchar : (strlen)(varchar) // I{n}s
        # int : (int) // i
        # float : (float) // f
        # so in a block (offset), cbs(0), row_count(4), c
        binary_data = b""

        content_data = b""
        for _, row in self.rows.items():
            format_str = "="
            values = []
            
            for idx, (col_name, col_type) in enumerate(self.schema.columns.items()):
                value = row[idx]

                if col_type.startswith("varchar"):
                    if isinstance(value, str):
                        encoded_value = value.encode('utf-8')
                    elif isinstance(value, bytes):
                        encoded_value = value
                    else:
                        raise ValueError(f"Expected string or bytes for column '{col_name}', got {type(value)}.")

                    length = len(encoded_value)
                    format_str += f"I{length}s"
                    values.extend([length, encoded_value])
                elif col_type.startswith("char"):
                    max_len = int(col_type[col_type.find("(") + 1 : col_type.find(")")])
                    if isinstance(value, str):
                        encoded_value = value.encode('utf-8').ljust(max_len, b'\x00')
                    elif isinstance(value, bytes):
                        encoded_value = value.ljust(max_len, b'\x00')
                    else:
                        raise ValueError(f"Expected string or bytes for column '{col_name}', got {type(value)}.")
                    
                    format_str += f"{len(encoded_value)}s"
                    values.append(encoded_value)
                else:
                    format_str += Calc.get_format_char(self, col_type)
                    values.append(value)
            content_data += struct.pack(format_str, *values)
        
        current_block_size = self.calculate_current_block_size()
        row_count = len(self.rows)

        binary_data = struct.pack("ii", *[current_block_size, row_count])

        full_bin_data = binary_data + content_data

        return full_bin_data

    def from_bytes(self, binary_data, record_count):
        self.rows = {}

        offset = 0

        # print(offset, "sadfas", binary_data)

        for _ in range(record_count):
            row = []

            start_offset = offset + 8

            for _, col_type in self.schema.columns.items():
                if col_type.startswith("varchar"):
                    len_str = struct.unpack_from("I", binary_data, offset)[0]
                    offset += 4 

                    str_format = f"{len_str}s"
                    str_data = struct.unpack_from(str_format, binary_data, offset)[0]
                    offset += len_str

                    value = str_data.decode('utf-8').rstrip('\x00')
                elif col_type.startswith("char"):
                    max_len = int(col_type[col_type.find("(") + 1 : col_type.find(")")])
                    str_format = f"{max_len}s"  # Fixed size
                    
                    # No need to unpack length (I) for char columns
                    char_data = struct.unpack_from(str_format, binary_data, offset)[0]
                    offset += max_len

                    value = char_data.decode('utf-8', errors='ignore').rstrip('\x00')
                else:
                    format_char = Calc.get_format_char(self, col_type)
                    value = struct.unpack_from(format_char, binary_data, offset)[0]
                    offset += struct.calcsize(format_char)
                row.append(value)
            
            self.rows[start_offset] = tuple(row) 
    
    def is_possible_to_add(self, record_size):
        return self.calculate_current_block_size() + record_size < self.block_size
    
    def calculate_offsets(self, binary_data: bytes, row_count: int) -> List[int]:
        offsets = []
        offset = 8  

        for _ in range(row_count):
            offsets.append(offset)

            for _, col_type in self.schema.columns.items():
                if col_type.startswith("varchar"):
                    length = struct.unpack_from("I", binary_data, offset)[0]
                    offset += 4 + length  
                elif col_type.startswith("char"):
                    max_len = int(col_type[col_type.find("(") + 1 : col_type.find(")")])
                    offset += max_len  
                elif col_type == "int":
                    offset += 4
                elif col_type == "float":
                    offset += 4 
                else:
                    raise ValueError(f"Unsupported column type: {col_type}")

        return offsets
