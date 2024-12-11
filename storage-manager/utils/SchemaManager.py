from storage.core.TableSchema import TableSchema

class SchemaManager:
    @staticmethod
    def save_schema(schema, file_path):
        with open(file_path, 'wb') as f:
            f.write(schema.to_bytes())

    @staticmethod
    def load_schema(file_path):
        with open(file_path, 'rb') as f:
            return TableSchema.from_bytes(f.read())