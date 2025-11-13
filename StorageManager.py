import os
import math
from storagemanager_helper.row_serializer import RowSerializer
from storagemanager_model.statistic import Statistic
from storagemanager_helper.schema_manager import SchemaManager
from storagemanager_helper.slotted_page import SlottedPage, PAGE_SIZE
from storagemanager_model.condition import Condition
from storagemanager_model.data_retrieval import DataRetrieval

class StorageManager:
    def __init__(self, base_path='data'):
        self.base_path = base_path
        self.storage_path = base_path
        self.row_serializer = RowSerializer()
        self.schema_manager = SchemaManager(base_path)
        
        if not os.path.exists(self.storage_path):
            os.makedirs(self.storage_path)
        
        schema_file = os.path.join(self.storage_path, 'schema.dat')
        if os.path.exists(schema_file):
            self.schema_manager.load_schemas()

    def read_block(self, data_retrieval: DataRetrieval):
        table = data_retrieval.table
        columns = data_retrieval.column
        conditions = data_retrieval.conditions or []

        schema = self.schema_manager.get_table_schema(table)
        if schema is None:
            raise ValueError(f"Tabel '{table}' tidak ditemukan")

        schema_attrs = [attr["name"] for attr in schema.get_attributes()]

        if columns != "*" and columns is not None:
            if isinstance(columns, str):
                columns = [columns]
            for col in columns:
                if col not in schema_attrs:
                    raise ValueError(f"Kolom '{col}' tidak ada di tabel '{table}'")

        for cond in conditions:
            if cond.column not in schema_attrs:
                raise ValueError(f"Kolom '{cond.column}' tidak ada di tabel '{table}'")

        table_path = os.path.join(self.base_path, f"{table}.dat")
        if not os.path.exists(table_path):
            raise FileNotFoundError(f"File data '{table_path}' tidak ditemukan")

        results = []

        with open(table_path, "rb") as f:
            while True:
                page_bytes = f.read(PAGE_SIZE)
                if not page_bytes:
                    break
                if len(page_bytes) < PAGE_SIZE:
                    page_bytes = page_bytes.ljust(PAGE_SIZE, b"\x00")

                page = SlottedPage()
                page.load(page_bytes)

                for slot_idx in range(page.record_count):
                    try:
                        record_bytes = page.get_record(slot_idx)
                        row = self.row_serializer.deserialize(schema, record_bytes)
                    except Exception as e:
                        raise ValueError(f"Gagal decode record: {e}")

                    if not self._match_all(row, conditions):
                        continue

                    results.append(self._project(row, columns))

        return results

    def _match_all(self, row, conditions):
        for cond in conditions:
            if not self._match(row, cond):
                return False
        return True

    def _match(self, row, cond: Condition):
        a = row.get(cond.column)
        b = cond.operand
        op = cond.operation

        if isinstance(a, (int, float)) and isinstance(b, str):
            s = b.strip()
            if s.replace('.', '', 1).lstrip('+-').isdigit():
                b = float(s) if '.' in s else int(s)

        if op == "=": return a == b
        if op in ("<>", "!="): return a != b
        if op == ">": return a > b
        if op == ">=": return a >= b
        if op == "<": return a < b
        if op == "<=": return a <= b
        return False

    def _project(self, row, columns):
        if columns == "*" or columns is None:
            return row
        if isinstance(columns, str):
            columns = [columns]
        return {c: row[c] for c in columns}


    def write_block(self, data_write):
        table = data_write.table
        column = data_write.column
        conditions = data_write.conditions
        new_value = data_write.new_value

        schema = self.schema_manager.get_table_schema(table)
        if schema is None:
            raise ValueError(f"Tabel '{table}' tidak ditemukan")

        table_path = os.path.join(self.base_path, f"{table}.dat")
        if not os.path.exists(table_path):
            raise FileNotFoundError(f"File data '{table_path}' tidak ditemukan")

        if column is None and not conditions:
            return self._insert_record(table_path, schema, new_value)
        else:
            schema_attrs = [attr["name"] for attr in schema.get_attributes()] 
            if column != "*" and column is not None:
                if isinstance(column, str):
                    column = [column]
                for col in column:
                    if col not in schema_attrs:
                        raise ValueError(f"Kolom '{col}' tidak ada di tabel '{table}'")
            
            if conditions:
                for cond in conditions:
                    if cond.column not in schema_attrs:
                        raise ValueError(f"Kolom '{cond.column}' tidak ada di tabel '{table}'")   
            return self._update_record(table_path, schema, conditions, column, new_value)

    def _insert_record(self, table_path, schema, new_record):
        record_bytes = self.row_serializer.serialize(schema, new_record)

        with open(table_path, "rb+") as f:
            f.seek(0, os.SEEK_END)
            file_size = f.tell()
            if file_size == 0:
                page = SlottedPage()
            else:
                f.seek(file_size - PAGE_SIZE)
                page_bytes = f.read(PAGE_SIZE)
                page = SlottedPage()
                page.load(page_bytes)

            try:
                page.add_record(record_bytes)
                f.seek(file_size - PAGE_SIZE)
            except Exception:
                f.seek(0, os.SEEK_END)
                page = SlottedPage()
                page.add_record(record_bytes)

            f.write(page.serialize())
        
        return 1

    def _update_record(self, table_path, schema, conditions, column, new_value):
        rows_affected = 0

        if not isinstance(new_value, dict):
            if isinstance(column, list) and len(column) == 1:
                new_value = {column[0]: new_value}
            elif isinstance(column, str):
                new_value = {column: new_value}
            else:
                raise ValueError("new_value must be a dictionary")
        
        with open(table_path, "rb+") as f:
            pages = []
            while page_bytes := f.read(PAGE_SIZE):
                page = SlottedPage()
                page.load(page_bytes)

                for i in range(page.record_count):
                    record_bytes = page.get_record(i)
                    record = self.row_serializer.deserialize(schema, record_bytes)

                    if self._match_all(record, conditions):
                        for col in column:
                            record[col] = new_value[col]

                        new_record_bytes = self.row_serializer.serialize(schema, record)
                        page.update_record(i, new_record_bytes)
                        rows_affected += 1
                
                pages.append(page)
        
            f.seek(0)
            for page in pages:
                f.write(page.serialize())
            
        return rows_affected

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
