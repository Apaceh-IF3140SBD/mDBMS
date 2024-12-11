import struct
import os
from typing import Any, Tuple, Dict, List


class HashIndex:
    def __init__(self, bucket_size=128):
        self.bucket_size = bucket_size
        self.buckets: List[List[Tuple[Any, Tuple[int, int]]]] = [[] for _ in range(bucket_size)]  # Array of buckets

    def _hash_function(self, key: Any) -> int:
        if isinstance(key, int):
            return key % self.bucket_size
        elif isinstance(key, float):
            return int(key) % self.bucket_size
        elif isinstance(key, str):
            return sum(ord(c) for c in key) % self.bucket_size
        elif isinstance(key, bytes):
            return sum(byte for byte in key) % self.bucket_size
        else:
            raise ValueError(f"Unsupported key type: {type(key)}")

    def add(self, key, value: Tuple[int, int]):
        bucket_index = self._hash_function(key)
        bucket = self.buckets[bucket_index]
        bucket.append((key, value))

    def get(self, key: Any) -> List[Tuple[int, int]]:
        bucket_index = self._hash_function(key)
        bucket = self.buckets[bucket_index]
        return [value for existing_key, value in bucket if existing_key == key]
    
    def remove(self, key: Any, value: Tuple[int, int] = None) -> bool:
        bucket_index = self._hash_function(key)
        bucket = self.buckets[bucket_index]

        for i, (existing_key, existing_value) in enumerate(bucket):
            if existing_key == key and existing_value == value:
                del bucket[i]
                return True
            
        return False

    def to_bytes(self, key_type: int) -> bytes:
        binary_data = b""

        # Write the key type and bucket size
        binary_data += struct.pack("I", key_type)
        binary_data += struct.pack("I", self.bucket_size)

        for bucket in self.buckets:
            # Write the size of each bucket
            binary_data += struct.pack("I", len(bucket))
            for key, (offset, block_id) in bucket:
                if key_type == 1:  # Integer
                    key_data = struct.pack("i", key)
                    binary_data += key_data
                elif key_type == 2:  # Float
                    key_data = struct.pack("f", key)
                    binary_data += key_data
                elif key_type == 3:  # String
                    key_data = key.encode("utf-8")
                    key_length = len(key_data)
                    binary_data += struct.pack("I", key_length)
                    binary_data += struct.pack(f"{key_length}s", key_data)
                else:
                    raise ValueError(f"Unsupported key type: {key_type}")

                # Append key data and value (offset, block_id)
                binary_data += struct.pack("II", offset, block_id)

        return binary_data

    @staticmethod
    def from_bytes(binary_data: bytes) -> "HashIndex":
        offset = 0

        # Read key type and bucket size
        key_type = struct.unpack_from("I", binary_data, offset)[0]
        offset += 4
        bucket_size = struct.unpack_from("I", binary_data, offset)[0]
        offset += 4
        index = HashIndex(bucket_size=bucket_size)

        for _ in range(bucket_size):
            # Read the size of the current bucket
            bucket_count = struct.unpack_from("I", binary_data, offset)[0]
            offset += 4

            for _ in range(bucket_count):
                if key_type == 1:  # Integer
                    key = struct.unpack_from("i", binary_data, offset)[0]
                    offset += 4
                elif key_type == 2:  # Float
                    key = struct.unpack_from("f", binary_data, offset)[0]
                    offset += 4
                elif key_type == 3:  # String
                    key_length = struct.unpack_from("I", binary_data, offset)[0]
                    offset += 4
                    # print(key_length)
                    key = struct.unpack_from(f"{key_length}s", binary_data, offset)[0].decode("utf-8")
                    offset += key_length
                else:
                    raise ValueError(f"Unsupported key type: {key_type}")

                # Read value (offset, block_id)
                record_offset, block_id = struct.unpack_from("II", binary_data, offset)
                offset += 8

                # Add the key-value pair to the hash index
                index.add(key, (record_offset, block_id))

        return index
    
    def save_to_file(self, file_path: str, column_type_int: int):
        with open(file_path, "wb") as f:
            f.write(self.to_bytes(key_type=column_type_int))

    @staticmethod
    def load_from_file(file_path: str) -> "HashIndex":
        with open(file_path, "rb") as f:
            return HashIndex.from_bytes(f.read())
        
    def print_index(self):
        print(f"Hash Index with {self.bucket_size} buckets:")
        for bucket_index, bucket in enumerate(self.buckets):
            print(f"Bucket {bucket_index}:")
            if not bucket:
                print("  [Empty]")
            for key, (offset, block_id) in bucket:
                print(f"  Key: {key}, Offset: {offset}, Block ID: {block_id}")
    
    @staticmethod
    def column_type_to_number(column_type: str) -> int: # return -1 when invalid
        if (column_type.startswith("varchar") or column_type.startswith("char")):
            return 3
        elif (column_type == "float"):
            return 2
        elif (column_type == "int"):
            return 1
        
        return -1