from .schema import Schema
import struct
import os

class SchemaManager:
    def __init__(self, base_path='data'):
        self.schemas = {}
        self.base_path = base_path

    def add_table_schema(self, table_name, schema):
        self.schemas[table_name] = schema

    def save_schemas(self):
        path = os.path.join(self.base_path, 'schema.dat')
        with open(path, 'wb') as f:
            f.write(struct.pack('i', len(self.schemas)))

            for table_name, schema in self.schemas.items():
                table_name_bytes = table_name.encode('utf-8')
                f.write(struct.pack('i', len(table_name_bytes)))
                f.write(table_name_bytes)

                schema_data = schema.serialize()
                f.write(struct.pack('i', len(schema_data)))
                f.write(schema_data)
    
    def load_schemas(self):
        path = os.path.join(self.base_path, 'schema.dat')
        self.schemas = {}
        with open(path, 'rb') as f:
            num_tables_bytes = f.read(4)
            num_tables = struct.unpack('i', num_tables_bytes)[0]

            for _ in range(num_tables):
                table_name_len_bytes = f.read(4)
                table_name_len = struct.unpack('i', table_name_len_bytes)[0]
                table_name_bytes = f.read(table_name_len)
                table_name = table_name_bytes.decode('utf-8')

                schema_data_len_bytes = f.read(4)
                schema_data_len = struct.unpack('i', schema_data_len_bytes)[0]
                schema_data = f.read(schema_data_len)

                schema = Schema().deserialize(schema_data)
                self.schemas[table_name] = schema

    def get_table_schema(self, table_name):
        return self.schemas.get(table_name)
    
    def list_tables(self):
        return list(self.schemas.keys())

   