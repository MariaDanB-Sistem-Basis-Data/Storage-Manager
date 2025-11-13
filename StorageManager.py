import os
import math
from helper.row_serializer import RowSerializer
from helper.schema_manager import SchemaManager
from helper.slotted_page import SlottedPage
from model.statistic import Statistic

class StorageManager:
    def __init__(self, storage_path='data'):
        self.storage_path = storage_path
        self.schema_manager = SchemaManager()
        
        if not os.path.exists(self.storage_path):
            os.makedirs(self.storage_path)
        
        schema_file = os.path.join(self.storage_path, 'schema.dat')
        if os.path.exists(schema_file):
            self.schema_manager.load_schemas()

    def read_block(self, data_retrieval):
        # Implementation for reading a block of data based on the data_retrieval parameters
        pass

    def write_block(self, data_write):
        
        pass

    def delete_block(self, data_deletion):
        # Implementation for deleting a block of data based on the data_deletion parameters
        pass

    def set_index(self, table, column, index_type):
        # Implementation for setting an index on a specified table and column
        pass

    def get_stats(self, table_name=None):
        if table_name is None or table_name == '':
            return self._get_all_stats()
        else:
            return self._get_table_stats(table_name)
    
    def _get_all_stats(self):
        all_stats = {}
        tables = self.schema_manager.list_tables()
        
        for table in tables:
            all_stats[table] = self._get_table_stats(table)
        
        return all_stats
    
    def _get_table_stats(self, table_name):
        schema = self.schema_manager.get_table_schema(table_name)
        
        if schema is None:
            return Statistic(n_r=0, b_r=0, l_r=0, f_r=0, v_a_r={})
        
        table_file = os.path.join(self.storage_path, f"{table_name}.dat")
        
        if not os.path.exists(table_file):
            return Statistic(n_r=0, b_r=0, l_r=0, f_r=0, v_a_r={})
        
        n_r = 0
        l_r = 0
        v_a_r = {}
        
        attributes = schema.get_attributes()
        for attr in attributes:
            attr_type = attr['type']
            attr_size = attr['size']
            
            if attr_type == 'int':
                l_r += 4
            elif attr_type == 'float':
                l_r += 4
            elif attr_type == 'char':
                l_r += attr_size
            elif attr_type == 'varchar':
                l_r += 4 + (attr_size // 2)
        
        file_size = os.path.getsize(table_file)
        page_count = file_size // 4096
        
        serializer = RowSerializer()
        distinct_values = {attr['name']: set() for attr in attributes}
        
        try:
            with open(table_file, 'rb') as f:
                for page_num in range(page_count):
                    page_data = f.read(4096)
                    if len(page_data) < 4096:
                        break
                    
                    page = SlottedPage()
                    page.load(page_data)
                    
                    n_r += page.record_count
                    
                    for i in range(page.record_count):
                        try:
                            record_bytes = page.get_record(i)
                            record = serializer.deserialize(schema, record_bytes)
                            
                            for attr_name, value in record.items():
                                distinct_values[attr_name].add(str(value))
                        except:
                            pass
        except:
            pass
        
        for attr_name, values in distinct_values.items():
            v_a_r[attr_name] = len(values)
        
        page_size = 4096
        if l_r > 0:
            f_r = page_size // l_r
            if f_r == 0:
                f_r = 1
        else:
            f_r = 1
        
        if f_r > 0 and n_r > 0:
            b_r = math.ceil(n_r / f_r)
        else:
            b_r = page_count
        
        return Statistic(n_r=n_r, b_r=b_r, l_r=l_r, f_r=f_r, v_a_r=v_a_r)
