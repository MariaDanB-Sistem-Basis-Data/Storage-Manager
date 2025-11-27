import os
import math
from storagemanager_helper.row_serializer import RowSerializer
from storagemanager_model.statistic import Statistic
from storagemanager_helper.schema_manager import SchemaManager
from storagemanager_helper.slotted_page import SlottedPage, PAGE_SIZE
from storagemanager_model.condition import Condition
from storagemanager_model.data_retrieval import DataRetrieval
from storagemanager_model.index import HashIndexEntry
from storagemanager_helper.index import HashIndexManager, BPlusTreeIndexManager
class StorageManager:
    def __init__(self, base_path='data'):
        self.base_path = base_path
        self.storage_path = base_path
        self.row_serializer = RowSerializer()
        self.schema_manager = SchemaManager(base_path)
        self.hash_index_manager = HashIndexManager(base_path)
        self.bplus_tree_index_manager = BPlusTreeIndexManager(base_path)
        
        if not os.path.exists(self.storage_path):
            os.makedirs(self.storage_path)
        
        schema_file = os.path.join(self.storage_path, 'schema.dat')
        if os.path.exists(schema_file):
            self.schema_manager.load_schemas()

    def _get_table_file_path(self, table_name: str) -> str:
        exact_path = os.path.join(self.base_path, f"{table_name}.dat")
        if os.path.exists(exact_path):
            return exact_path
        
        lower_path = os.path.join(self.base_path, f"{table_name.lower()}.dat")
        if os.path.exists(lower_path):
            return lower_path
        
        upper_path = os.path.join(self.base_path, f"{table_name.upper()}.dat")
        if os.path.exists(upper_path):
            return upper_path
        
        return lower_path

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

        index_used = False
        results = []

        if len(conditions) == 1:
            cond = conditions[0]
            if cond.operation == "=":
                index_locations = self.hash_index_manager.search(table, cond.column, cond.operand)
                
                if not index_locations:
                    index_locations = self.bplus_tree_index_manager.search(table, cond.column, cond.operand)

                if index_locations:
                    index_used = True
                    table_path = self._get_table_file_path(table)
                    
                    with open(table_path, "rb") as f:
                        for page_id, slot_id in index_locations:
                            f.seek(page_id * PAGE_SIZE)
                            page_bytes = f.read(PAGE_SIZE)
                            
                            if len(page_bytes) < PAGE_SIZE:
                                page_bytes = page_bytes.ljust(PAGE_SIZE, b"\x00")
                            
                            page = SlottedPage()
                            page.load(page_bytes)
                            
                            try:
                                record_bytes = page.get_record(slot_id)
                                row = self.row_serializer.deserialize(schema, record_bytes)
                                results.append(self._project(row, columns))
                            except:
                                pass 
            
            elif cond.operation in (">", "<", ">=", "<="):
                btree_indexes = self.bplus_tree_index_manager.list_indexes(table)
                has_btree = any(idx['column'] == cond.column for idx in btree_indexes)
                
                if has_btree:
                    index_used = True
                    table_path = self._get_table_file_path(table)
                    
                    if cond.operation in (">", ">="):
                        index_data = self.bplus_tree_index_manager.load_index(table, cond.column)
                        if index_data and index_data['root']:
                            node = index_data['root']
                            while not node.is_leaf:
                                node = node.children[-1]
                            max_key = node.keys[-1] if node.keys else cond.operand
                            
                            if cond.operation == ">":
                                range_results = self.bplus_tree_index_manager.range_search(
                                    table, cond.column, cond.operand, max_key
                                )
                                range_results = [(k, v) for k, v in range_results if k > cond.operand]
                            else:  # >=
                                range_results = self.bplus_tree_index_manager.range_search(
                                    table, cond.column, cond.operand, max_key
                                )
                        else:
                            range_results = []
                    
                    else:  # < or <=
                        index_data = self.bplus_tree_index_manager.load_index(table, cond.column)
                        if index_data and index_data['root']:
                            node = index_data['root']
                            while not node.is_leaf:
                                node = node.children[0]
                            min_key = node.keys[0] if node.keys else cond.operand
                            
                            if cond.operation == "<":
                                range_results = self.bplus_tree_index_manager.range_search(
                                    table, cond.column, min_key, cond.operand
                                )
                                range_results = [(k, v) for k, v in range_results if k < cond.operand]
                            else:  # <=
                                range_results = self.bplus_tree_index_manager.range_search(
                                    table, cond.column, min_key, cond.operand
                                )
                        else:
                            range_results = []
                    
                    with open(table_path, "rb") as f:
                        for key, (page_id, slot_id) in range_results:
                            f.seek(page_id * PAGE_SIZE)
                            page_bytes = f.read(PAGE_SIZE)
                            
                            if len(page_bytes) < PAGE_SIZE:
                                page_bytes = page_bytes.ljust(PAGE_SIZE, b"\x00")
                            
                            page = SlottedPage()
                            page.load(page_bytes)
                            
                            try:
                                record_bytes = page.get_record(slot_id)
                                row = self.row_serializer.deserialize(schema, record_bytes)
                                results.append(self._project(row, columns))
                            except:
                                pass
    
        # Full table scan
        if not index_used:

            table_path = self._get_table_file_path(table)
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

        table_path = self._get_table_file_path(table)
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
        table_name = os.path.basename(table_path)[:-4]

        with open(table_path, "rb+") as f:
            f.seek(0, os.SEEK_END)
            file_size = f.tell()

            if file_size == 0:
                page = SlottedPage()
                page_id = 0
                slot_id= page.add_record(record_bytes)
                f.seek(0)
                f.write(page.serialize())
            else:
                page_id = (file_size // PAGE_SIZE) - 1
                f.seek(page_id * PAGE_SIZE)
                page_bytes = f.read(PAGE_SIZE)

                page = SlottedPage()
                page.load(page_bytes)

            try:
                slot_id = page.add_record(record_bytes)
                f.seek(page_id * PAGE_SIZE)
                f.write(page.serialize())
            except Exception:
                f.seek(0, os.SEEK_END)
                page = SlottedPage()
                page_id = file_size // PAGE_SIZE
                slot_id = page.add_record(record_bytes)
                f.write(page.serialize())
        
        hash_indexes = self.hash_index_manager.list_indexes(table_name)
        for idx in hash_indexes:
            column_name = idx['column']
            key_value = new_record.get(column_name)
            self.hash_index_manager.insert_entry(table_name, column_name, key_value, page_id, slot_id)
            self.hash_index_manager.save_index(table_name, column_name)
        
        btree_indexes = self.bplus_tree_index_manager.list_indexes(table_name)
        for idx in btree_indexes:
            column_name = idx['column']
            key_value = new_record.get(column_name)
            self.bplus_tree_index_manager.insert_entry(table_name, column_name, key_value, page_id, slot_id)
            self.bplus_tree_index_manager.save_index(table_name, column_name)
        
        
        return 1

    def _update_record(self, table_path, schema, conditions, column, new_value):
        rows_affected = 0
        table_name = os.path.basename(table_path)[:-4]

        if not isinstance(new_value, dict):
            if isinstance(column, list) and len(column) == 1:
                new_value = {column[0]: new_value}
            elif isinstance(column, str):
                new_value = {column: new_value}
            else:
                raise ValueError("new_value must be a dictionary")
        
        with open(table_path, "rb+") as f:
            page_id = 0

            while True:
                page_start = page_id * PAGE_SIZE
                f.seek(page_start)
                page_bytes = f.read(PAGE_SIZE)
                if not page_bytes:
                    break
                
                if len(page_bytes) < PAGE_SIZE:
                    page_bytes = page_bytes.ljust(PAGE_SIZE, b"\x00")
                
                page = SlottedPage()
                page.load(page_bytes)
                
                page_modified = False 

                for slot_id in range(page.record_count):
                    try:  
                        record_bytes = page.get_record(slot_id)
                        record = self.row_serializer.deserialize(schema, record_bytes)
                    except:
                        continue  

                    if self._match_all(record, conditions):
                      
                        hash_indexes = self.hash_index_manager.list_indexes(table_name)
                        for idx in hash_indexes:
                            column_name = idx['column']
                            if column_name in new_value:
                                old_key = record[column_name]
                                new_key = new_value[column_name]
                                self.hash_index_manager.update_entry(
                                    table_name, column_name, old_key, new_key, page_id, slot_id
                                )
                        btree_indexes = self.bplus_tree_index_manager.list_indexes(table_name)
                        for idx in btree_indexes:
                            column_name = idx['column']
                            if column_name in new_value:
                                old_key = record[column_name]
                                new_key = new_value[column_name]
                                self.bplus_tree_index_manager.update_entry(
                                    table_name, column_name, old_key, new_key, page_id, slot_id
                                )

                        for col in column:
                            record[col] = new_value[col]

                        new_record_bytes = self.row_serializer.serialize(schema, record)
                        page.update_record(slot_id, new_record_bytes)
                        page_modified = True 
                        rows_affected += 1
                
                if page_modified:
                    f.seek(page_start)
                    f.write(page.serialize())
                
                page_id += 1
       
        hash_indexes = self.hash_index_manager.list_indexes(table_name)
        for idx in hash_indexes:
            self.hash_index_manager.save_index(table_name, idx['column'])
        
        btree_indexes = self.bplus_tree_index_manager.list_indexes(table_name)
        for idx in btree_indexes:
            self.bplus_tree_index_manager.save_index(table_name, idx['column'])
        
        return rows_affected

    def delete_block(self, data_deletion):
        table = data_deletion.table
        conditions = data_deletion.conditions

        schema = self.schema_manager.get_table_schema(table)
        if schema is None:
            raise ValueError(f"Tabel '{table}' tidak ditemukan")

        schema_attrs = [attr["name"] for attr in schema.get_attributes()]
        for cond in conditions:
            if cond.column not in schema_attrs:
                raise ValueError(f"Kolom '{cond.column}' tidak ada di tabel '{table}'")

        table_path = os.path.join(self.base_path, f"{table}.dat")
        if not os.path.exists(table_path):
            raise FileNotFoundError(f"File data '{table_path}' tidak ditemukan")

        rows_deleted = 0
        pages = []

        with open(table_path, "rb+") as f:
            while page_bytes := f.read(PAGE_SIZE):
                page = SlottedPage()
                page.load(page_bytes)

                i = 0
                while i < page.record_count:
                    record_bytes = page.get_record(i)
                    record = self.row_serializer.deserialize(schema, record_bytes)

                    if self._match_all(record, conditions):
                        page.delete_record(i)
                        rows_deleted += 1
                        continue     
                    else:
                        i += 1

                pages.append(page)

            f.seek(0)
            for page in pages:
                f.write(page.serialize())

            f.truncate(len(pages) * PAGE_SIZE)

        return rows_deleted


    def _set_index(self, table, column, index_type):
        schema = self.schema_manager.get_table_schema(table)
        if schema is None:
            raise ValueError(f"Tabel '{table}' tidak ditemukan")
        
        schema_attrs = [attr["name"] for attr in schema.get_attributes()]
        if column not in schema_attrs:
            raise ValueError(f"Kolom '{column}' tidak ada di tabel '{table}'")
    
        if index_type.lower() == 'hash':
            self.hash_index_manager.rebuild_index(table, column, self)
            return True
        elif index_type.lower() == 'btree':
            self.bplus_tree_index_manager.rebuild_index(table, column, self)
            return True
        else:
            raise ValueError(f"Index type '{index_type}' tidak tersedia.")        

    def _calculate_tree_depth(self, node):
        if node is None:
            return 0
        
        if node.is_leaf:
            return 1
        
        # For internal nodes, recurse on first child and add 1
        if node.children:
            return 1 + self._calculate_tree_depth(node.children[0])
        
        return 1
    
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
            return Statistic(n_r=0, b_r=0, l_r=0, f_r=0, v_a_r={}, i_r={})
        
        table_file = self._get_table_file_path(table_name)
        
        if not os.path.exists(table_file):
            return Statistic(n_r=0, b_r=0, l_r=0, f_r=0, v_a_r={}, i_r={})
        
        n_r = 0
        l_r = 0
        v_a_r = {}
        i_r = {}
        
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
        
        for attr in attributes:
            attr_name = attr['name']
            i_r[attr_name] = {'Type': 'none', 'Value': None}
        
        hash_indexes = self.hash_index_manager.list_indexes(table_name)
        for idx in hash_indexes:
            column_name = idx['column']
            index_type = idx['type']
            
            if index_type == 'hash':
                index_data = self.hash_index_manager.load_index(table_name, column_name)
                if index_data:
                    num_buckets = index_data.get('num_buckets', 200)  # Default 200 if not found
                    i_r[column_name] = {'Type': 'hash', 'Value': num_buckets}
                else:
                    i_r[column_name] = {'Type': 'hash', 'Value': 200}
        
        # Collect B+ tree indexes
        btree_indexes = self.bplus_tree_index_manager.list_indexes(table_name)
        for idx in btree_indexes:
            column_name = idx['column']
            index_type = idx['type']
            
            if index_type == 'btree':
                index_data = self.bplus_tree_index_manager.load_index(table_name, column_name)
                if index_data and index_data.get('root'):
                    depth = self._calculate_tree_depth(index_data['root'])
                    i_r[column_name] = {'Type': 'btree', 'Value': depth}
                else:
                    i_r[column_name] = {'Type': 'btree', 'Value': 0}
        
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
        
        return Statistic(n_r=n_r, b_r=b_r, l_r=l_r, f_r=f_r, v_a_r=v_a_r, i_r=i_r)
