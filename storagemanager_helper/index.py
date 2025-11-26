import os
import struct
from storagemanager_model.index import HashIndexEntry
from storagemanager_helper.slotted_page import PAGE_SIZE, SlottedPage

class HashIndexManager:
    def __init__(self, base_path='data'):
        self.base_path = base_path
        self.index_path = os.path.join(base_path, 'indexes')
        
        if not os.path.exists(self.index_path):
            os.makedirs(self.index_path)
        
        self.loaded_indexes = {}
    
    def _get_index_filename(self, table_name, column_name):
        return os.path.join(self.index_path, f"{table_name}_{column_name}_hash.idx")
    
    def _hash_function(self, key_value, num_buckets=200):
        # Polynomial Rolling Hash
        if key_value is None:
            key_str = "NULL"
        else:
            key_str = str(key_value)
        
        hash_val = 0
        prime = 31
        
        for char in key_str:
            hash_val = (hash_val * prime + ord(char)) % (2**32)
        
        return hash_val % num_buckets
    
    def _serialize_entry(self, entry):
        key_value = entry.key_value
        
        if key_value is None:
            key_type = 0
            key_bytes = b''
        elif isinstance(key_value, int):
            key_type = 1
            key_bytes = struct.pack('i', key_value)
        elif isinstance(key_value, float):
            key_type = 2
            key_bytes = struct.pack('f', key_value)
        else:  # string
            key_type = 3
            key_str = str(key_value)
            key_bytes = key_str.encode('utf-8')
        
        result = struct.pack('B', key_type) 
        result += struct.pack('I', len(key_bytes))  
        result += key_bytes
        result += struct.pack('I', entry.page_id) 
        result += struct.pack('I', entry.slot_id)
        
        return result
    
    def _deserialize_entry(self, data, offset=0):
        key_type = struct.unpack('B', data[offset:offset+1])[0]
        offset += 1
        
        key_len = struct.unpack('I', data[offset:offset+4])[0]
        offset += 4
        
        if key_type == 0:  # None
            key_value = None
        elif key_type == 1:  # int
            key_value = struct.unpack('i', data[offset:offset+4])[0]
        elif key_type == 2:  # float
            key_value = struct.unpack('f', data[offset:offset+4])[0]
        else:  # string
            key_value = data[offset:offset+key_len].decode('utf-8')
        
        offset += key_len
        
        page_id = struct.unpack('I', data[offset:offset+4])[0]
        offset += 4
        
        slot_id = struct.unpack('I', data[offset:offset+4])[0]
        offset += 4
        
        entry = HashIndexEntry(key_value, page_id, slot_id)
        return entry, offset
    
    def _serialize_index(self, index_data):
        metadata = index_data['metadata']
        buckets = index_data['buckets']
        
        table_bytes = metadata['table'].encode('utf-8')
        column_bytes = metadata['column'].encode('utf-8')
        
        result = struct.pack('I', len(table_bytes))
        result += table_bytes
        result += struct.pack('I', len(column_bytes))
        result += column_bytes
        result += struct.pack('I', metadata['num_buckets'])
        result += struct.pack('I', metadata['num_entries'])
        
        for bucket_id, entries in buckets.items():
            if len(entries) > 0:
                result += struct.pack('I', bucket_id)
                result += struct.pack('I', len(entries))
                
                for entry in entries:
                    result += self._serialize_entry(entry)
        
        return result
    
    def _deserialize_index(self, data):
        offset = 0
        
        table_len = struct.unpack('I', data[offset:offset+4])[0]
        offset += 4
        table_name = data[offset:offset+table_len].decode('utf-8')
        offset += table_len
        
        column_len = struct.unpack('I', data[offset:offset+4])[0]
        offset += 4
        column_name = data[offset:offset+column_len].decode('utf-8')
        offset += column_len
        
        num_buckets = struct.unpack('I', data[offset:offset+4])[0]
        offset += 4
        
        num_entries = struct.unpack('I', data[offset:offset+4])[0]
        offset += 4
        
        metadata = {
            'table': table_name,
            'column': column_name,
            'num_buckets': num_buckets,
            'index_type': 'hash',
            'num_entries': num_entries
        }
        
        buckets = {}
        while offset < len(data):
            bucket_id = struct.unpack('I', data[offset:offset+4])[0]
            offset += 4
            
            entry_count = struct.unpack('I', data[offset:offset+4])[0]
            offset += 4
            
            buckets[bucket_id] = []
            for _ in range(entry_count):
                entry, offset = self._deserialize_entry(data, offset)
                buckets[bucket_id].append(entry)
        
        return {
            'metadata': metadata,
            'buckets': buckets
        }
    
    def create_index(self, table_name, column_name, num_buckets=200):
        index_metadata = {
            'table': table_name,
            'column': column_name,
            'num_buckets': num_buckets,
            'index_type': 'hash',
            'num_entries': 0
        }
        
        index_data = {
            'metadata': index_metadata,
            'buckets': {}
        }
        
        index_file = self._get_index_filename(table_name, column_name)
        with open(index_file, 'wb') as f:
            f.write(self._serialize_index(index_data))
        
        self.loaded_indexes[(table_name, column_name)] = index_data
        
        return True
    
    def load_index(self, table_name, column_name):
        cache_key = (table_name, column_name)
        if cache_key in self.loaded_indexes:
            return self.loaded_indexes[cache_key]
        
        index_file = self._get_index_filename(table_name, column_name)
        if not os.path.exists(index_file):
            return None
        
        with open(index_file, 'rb') as f:
            data = f.read()
        
        index_data = self._deserialize_index(data)
  
        self.loaded_indexes[cache_key] = index_data
        return index_data
    
    def insert_entry(self, table_name, column_name, key_value, page_id, slot_id):
        index_data = self.load_index(table_name, column_name)
        if index_data is None:
            raise ValueError(f"Index on {table_name}.{column_name} does not exist")
        
        num_buckets = index_data['metadata']['num_buckets']
        bucket_id = self._hash_function(key_value, num_buckets)
        
        entry = HashIndexEntry(key_value, page_id, slot_id)
        
        if bucket_id not in index_data['buckets']:
            index_data['buckets'][bucket_id] = []
        
        index_data['buckets'][bucket_id].append(entry)
        index_data['metadata']['num_entries'] += 1
        
        self.loaded_indexes[(table_name, column_name)] = index_data
        
        return True
    
    def search(self, table_name, column_name, key_value):
        index_data = self.load_index(table_name, column_name)
        if index_data is None:
            return []
        
        num_buckets = index_data['metadata']['num_buckets']
        bucket_id = self._hash_function(key_value, num_buckets)
        
        results = []
        bucket = index_data['buckets'].get(bucket_id, [])
        
        for entry in bucket:
            if entry.key_value == key_value:
                results.append((entry.page_id, entry.slot_id))
        
        return results
    
    def delete_entry(self, table_name, column_name, key_value, page_id, slot_id):

        index_data = self.load_index(table_name, column_name)
        if index_data is None:
            return False
        
        num_buckets = index_data['metadata']['num_buckets']
        bucket_id = self._hash_function(key_value, num_buckets)
        
        bucket = index_data['buckets'].get(bucket_id, [])
        
        for i in range(len(bucket)):
            entry = bucket[i]
            if (entry.key_value == key_value and 
                entry.page_id == page_id and 
                entry.slot_id == slot_id):
                bucket.pop(i)
                index_data['metadata']['num_entries'] -= 1
                return True
        
        return False
    
    def update_entry(self, table_name, column_name, old_key, new_key, page_id, slot_id):
 
        self.delete_entry(table_name, column_name, old_key, page_id, slot_id)
        self.insert_entry(table_name, column_name, new_key, page_id, slot_id)
        return True
    
    def save_index(self, table_name, column_name):

        index_data = self.loaded_indexes.get((table_name, column_name))
        if index_data is None:
            return False
        
        index_file = self._get_index_filename(table_name, column_name)
        with open(index_file, 'wb') as f:
            f.write(self._serialize_index(index_data))
        
        return True
    
    def drop_index(self, table_name, column_name):

        index_file = self._get_index_filename(table_name, column_name)
        
        if os.path.exists(index_file):
            os.remove(index_file)
        
        cache_key = (table_name, column_name)
        if cache_key in self.loaded_indexes:
            del self.loaded_indexes[cache_key]
        
        return True
    
    def rebuild_index(self, table_name, column_name, storage_manager):
        
        self.drop_index(table_name, column_name)
        
        self.create_index(table_name, column_name)
        
        schema = storage_manager.schema_manager.get_table_schema(table_name)
        if schema is None:
            raise ValueError(f"Table {table_name} not found")
        
        schema_attrs = [attr["name"] for attr in schema.get_attributes()]
        if column_name not in schema_attrs:
            raise ValueError(f"Column {column_name} not found in {table_name}")
        
        table_path = os.path.join(storage_manager.base_path, f"{table_name}.dat")
        if not os.path.exists(table_path):
            return True  
        
        page_id = 0
        with open(table_path, "rb") as f:
            while True:
                page_bytes = f.read(PAGE_SIZE)
                if not page_bytes:
                    break
                
                if len(page_bytes) < PAGE_SIZE:
                    page_bytes = page_bytes.ljust(PAGE_SIZE, b"\x00")
                
                page = SlottedPage()
                page.load(page_bytes)
                
                for slot_id in range(page.record_count):
                    try:
                        record_bytes = page.get_record(slot_id)
                        row = storage_manager.row_serializer.deserialize(schema, record_bytes)
                        
                        key_value = row.get(column_name)
                        self.insert_entry(table_name, column_name, key_value, page_id, slot_id)
                    except Exception as e:
                        print(f"Warning: Failed to index record at page {page_id}, slot {slot_id}: {e}")
                
                page_id += 1
        
        self.save_index(table_name, column_name)
        
        return True
    
    def get_index_stats(self, table_name, column_name):
        index_data = self.load_index(table_name, column_name)
        if index_data is None:
            return None
        
        metadata = index_data['metadata']
        buckets = index_data['buckets']
        
        non_empty_buckets = len(buckets)
        
        max_chain_length = 0
        for bucket in buckets.values():
            if len(bucket) > max_chain_length:
                max_chain_length = len(bucket)
        
        avg_chain_length = metadata['num_entries'] / non_empty_buckets if non_empty_buckets > 0 else 0
        
        stats = {
            'table': metadata['table'],
            'column': metadata['column'],
            'num_buckets': metadata['num_buckets'],
            'num_entries': metadata['num_entries'],
            'non_empty_buckets': non_empty_buckets,
            'utilization': non_empty_buckets / metadata['num_buckets'] * 100,
            'max_chain_length': max_chain_length,
            'avg_chain_length': avg_chain_length
        }
        
        return stats
    
    def list_indexes(self, table_name=None):
        indexes = []
        
        if not os.path.exists(self.index_path):
            return indexes
        
        for filename in os.listdir(self.index_path):
            if filename.endswith('_hash.idx'):
                # filename: table_column_hash.idx
                parts = filename[:-9].split('_')  # Remove '_hash.idx'
                if len(parts) >= 2:
                    idx_table = '_'.join(parts[:-1])
                    idx_column = parts[-1]
                    
                    if table_name is None or idx_table == table_name:
                        indexes.append({
                            'table': idx_table,
                            'column': idx_column,
                            'type': 'hash'
                        })
        
        return indexes