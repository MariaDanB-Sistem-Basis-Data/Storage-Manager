import os
import struct
from storagemanager_model.index import HashIndexEntry ,BPlusTreeNode, BPlusTreeIndexEntry
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
                parts = filename[:-9].split('_') 
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
    
class BPlusTreeIndexManager:
    def __init__(self, base_path='data'):
        self.base_path = base_path
        self.index_path = os.path.join(base_path, 'indexes')
        
        if not os.path.exists(self.index_path):
            os.makedirs(self.index_path)
        
        self.loaded_indexes = {}
    
    def _get_index_filename(self, table_name, column_name):
        return os.path.join(self.index_path, f"{table_name}_{column_name}_btree.idx")

    def _compare_keys(self, key1, key2):
        if key1 is None and key2 is None:
            return 0
        if key1 is None:
            return -1
        if key2 is None:
            return 1
        
        if key1 < key2:
            return -1
        elif key1 > key2:
            return 1
        else:
            return 0
    
    def _serialize_key(self, key):
        if key is None:
            key_type = 0
            key_bytes = b''
        elif isinstance(key, int):
            key_type = 1
            key_bytes = struct.pack('i', key)
        elif isinstance(key, float):
            key_type = 2
            key_bytes = struct.pack('f', key)
        else:
            key_type = 3
            key_bytes = str(key).encode('utf-8')
        
        result = struct.pack('B', key_type)
        result += struct.pack('I', len(key_bytes))
        result += key_bytes
        
        return result
    
    def _deserialize_key(self, data, offset=0):
        key_type = struct.unpack('B', data[offset:offset+1])[0]
        offset += 1
        
        key_len = struct.unpack('I', data[offset:offset+4])[0]
        offset += 4
        
        if key_type == 0:
            key_value = None
        elif key_type == 1:
            key_value = struct.unpack('i', data[offset:offset+4])[0]
            offset += 4
        elif key_type == 2:
            key_value = struct.unpack('f', data[offset:offset+4])[0]
            offset += 4
        else:
            key_value = data[offset:offset+key_len].decode('utf-8')
            offset += key_len
        
        return key_value, offset
    
    def _serialize_node(self, node):
        result = struct.pack('B', 1 if node.is_leaf else 0)
        result += struct.pack('I', node.order)
        result += struct.pack('I', len(node.keys))
        
        for key in node.keys:
            result += self._serialize_key(key)
        
        if node.is_leaf:
            result += struct.pack('I', len(node.values))
            for page_id, slot_id in node.values:
                result += struct.pack('I', page_id)
                result += struct.pack('I', slot_id)
        else:
            result += struct.pack('I', len(node.children))
        
        return result
    
    def _deserialize_node(self, data, offset=0):
        is_leaf = struct.unpack('B', data[offset:offset+1])[0] == 1
        offset += 1
        
        order = struct.unpack('I', data[offset:offset+4])[0]
        offset += 4
        
        num_keys = struct.unpack('I', data[offset:offset+4])[0]
        offset += 4
        
        node = BPlusTreeNode(is_leaf=is_leaf, order=order)
        
        for _ in range(num_keys):
            key, offset = self._deserialize_key(data, offset)
            node.keys.append(key)
        
        if is_leaf:
            num_values = struct.unpack('I', data[offset:offset+4])[0]
            offset += 4
            
            for _ in range(num_values):
                page_id = struct.unpack('I', data[offset:offset+4])[0]
                offset += 4
                slot_id = struct.unpack('I', data[offset:offset+4])[0]
                offset += 4
                node.values.append((page_id, slot_id))
            
            num_children = 0
        else:
            num_children = struct.unpack('I', data[offset:offset+4])[0]
            offset += 4
        
        return node, num_children, offset
    
    def _serialize_tree(self, node):
        if node is None:
            return struct.pack('B', 0)
        
        result = struct.pack('B', 1)
        result += self._serialize_node(node)
        
        if not node.is_leaf:
            for child in node.children:
                result += self._serialize_tree(child)
        
        return result
    
    def _deserialize_tree(self, data, offset, parent=None):
        null_marker = struct.unpack('B', data[offset:offset+1])[0]
        offset += 1
        
        if null_marker == 0:
            return None, offset
        
        node, num_children, offset = self._deserialize_node(data, offset)
        node.parent = parent
        
        if not node.is_leaf:
            for i in range(num_children):
                child, offset = self._deserialize_tree(data, offset, parent=node)
                node.children.append(child)
            
            if node.children and node.children[0].is_leaf:
                for i in range(len(node.children) - 1):
                    node.children[i].next_leaf = node.children[i + 1]
        
        return node, offset
    
    def _serialize_index(self, index_data):
        metadata = index_data['metadata']
        root = index_data['root']
        
        table_bytes = metadata['table'].encode('utf-8')
        column_bytes = metadata['column'].encode('utf-8')
        
        result = struct.pack('I', len(table_bytes))
        result += table_bytes
        result += struct.pack('I', len(column_bytes))
        result += column_bytes
        result += struct.pack('I', metadata['order'])
        result += struct.pack('I', metadata['num_entries'])
        
        tree_bytes = self._serialize_tree(root)
        result += struct.pack('I', len(tree_bytes))
        result += tree_bytes
        
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
        
        order = struct.unpack('I', data[offset:offset+4])[0]
        offset += 4
        
        num_entries = struct.unpack('I', data[offset:offset+4])[0]
        offset += 4
        
        metadata = {
            'table': table_name,
            'column': column_name,
            'index_type': 'btree',
            'order': order,
            'num_entries': num_entries
        }
        
        tree_len = struct.unpack('I', data[offset:offset+4])[0]
        offset += 4
        
        root, _ = self._deserialize_tree(data, offset,parent=None)
        
        return {
            'metadata': metadata,
            'root': root
        }
    
    def create_index(self, table_name, column_name, order=4):
        metadata = {
            'table': table_name,
            'column': column_name,
            'index_type': 'btree',
            'order': order,
            'num_entries': 0
        }
        
        root = BPlusTreeNode(is_leaf=True, order=order)
        
        index_data = {
            'metadata': metadata,
            'root': root
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
    
    def _find_leaf(self, node, key):
        if node.is_leaf:
            return node
        
        i = 0
        while i < len(node.keys) and self._compare_keys(key, node.keys[i]) >= 0:
            i += 1
        
        return self._find_leaf(node.children[i], key)
    
    def _insert_in_leaf(self, leaf, key, page_id, slot_id):
        i = 0
        while i < len(leaf.keys) and self._compare_keys(key, leaf.keys[i]) > 0:
            i += 1
        
        leaf.keys.insert(i, key)
        leaf.values.insert(i, (page_id, slot_id))
    
    def _split_leaf(self, leaf):
        mid = len(leaf.keys) // 2
        
        new_leaf = BPlusTreeNode(is_leaf=True, order=leaf.order)
        new_leaf.keys = leaf.keys[mid:]
        new_leaf.values = leaf.values[mid:]
        new_leaf.next_leaf = leaf.next_leaf
        
        leaf.keys = leaf.keys[:mid]
        leaf.values = leaf.values[:mid]
        leaf.next_leaf = new_leaf
        
        return new_leaf.keys[0], new_leaf
    
    def _split_internal(self, node):
        mid = len(node.keys) // 2
        
        new_node = BPlusTreeNode(is_leaf=False, order=node.order)
        new_node.keys = node.keys[mid+1:]
        new_node.children = node.children[mid+1:]
        
        for child in new_node.children:
            child.parent = new_node
        
        promote_key = node.keys[mid]
        
        node.keys = node.keys[:mid]
        node.children = node.children[:mid+1]
        
        return promote_key, new_node
    
    def _insert_in_parent(self, left, key, right):
        if left.parent is None:
            new_root = BPlusTreeNode(is_leaf=False, order=left.order)
            new_root.keys = [key]
            new_root.children = [left, right]
            left.parent = new_root
            right.parent = new_root
            return new_root
        
        parent = left.parent
        
        i = 0
        while i < len(parent.keys) and self._compare_keys(key, parent.keys[i]) > 0:
            i += 1
        
        parent.keys.insert(i, key)
        parent.children.insert(i + 1, right)
        right.parent = parent
        
        if parent.is_full():
            promote_key, new_node = self._split_internal(parent)
            return self._insert_in_parent(parent, promote_key, new_node)
        
        return self._get_root(parent)
    
    def _get_root(self, node):
        while node.parent is not None:
            node = node.parent
        return node
    
    def insert_entry(self, table_name, column_name, key_value, page_id, slot_id):
        index_data = self.load_index(table_name, column_name)
        if index_data is None:
            raise ValueError(f"Index on {table_name}.{column_name} does not exist")
        
        root = index_data['root']
        
        leaf = self._find_leaf(root, key_value)
        self._insert_in_leaf(leaf, key_value, page_id, slot_id)
        
        if leaf.is_full():
            promote_key, new_leaf = self._split_leaf(leaf)
            new_root = self._insert_in_parent(leaf, promote_key, new_leaf)
            index_data['root'] = new_root
        
        index_data['metadata']['num_entries'] += 1
        
        return True
    
    def search(self, table_name, column_name, key_value):
        index_data = self.load_index(table_name, column_name)
        if index_data is None:
            return []
        
        root = index_data['root']
        leaf = self._find_leaf(root, key_value)
        
        results = []
        for i, key in enumerate(leaf.keys):
            if key == key_value:
                results.append(leaf.values[i])
        
        return results
    
    def range_search(self, table_name, column_name, start_key, end_key):
        index_data = self.load_index(table_name, column_name)
        if index_data is None:
            return []
        
        root = index_data['root']
        leaf = self._find_leaf(root, start_key)
        
        results = []
        
        while leaf is not None:
            for i, key in enumerate(leaf.keys):
                if self._compare_keys(key, start_key) >= 0 and self._compare_keys(key, end_key) <= 0:
                    results.append((key, leaf.values[i]))
                elif self._compare_keys(key, end_key) > 0:
                    return results
            
            leaf = leaf.next_leaf
        
        return results
    
    def delete_entry(self, table_name, column_name, key_value, page_id, slot_id):
        index_data = self.load_index(table_name, column_name)
        if index_data is None:
            return False
        
        root = index_data['root']
        leaf = self._find_leaf(root, key_value)
        
        for i, (key, (p_id, s_id)) in enumerate(zip(leaf.keys, leaf.values)):
            if key == key_value and p_id == page_id and s_id == slot_id:
                leaf.keys.pop(i)
                leaf.values.pop(i)
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
    
    def rebuild_index(self, table_name, column_name, storage_manager, order=4):
        self.drop_index(table_name, column_name)
        self.create_index(table_name, column_name, order)
        
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
                        record = storage_manager.row_serializer.deserialize(schema, record_bytes)
                        key_value = record.get(column_name)
                        
                        self.insert_entry(table_name, column_name, key_value, page_id, slot_id)
                    except Exception as e:
                        continue
                
                page_id += 1
        
        self.save_index(table_name, column_name)
        
        return True
    
    def get_index_stats(self, table_name, column_name):
        index_data = self.load_index(table_name, column_name)
        if index_data is None:
            return None
        
        metadata = index_data['metadata']
        root = index_data['root']
        
        height = self._calculate_height(root)
        node_count = self._count_nodes(root)
        leaf_count = self._count_leaves(root)
        
        stats = {
            'table': metadata['table'],
            'column': metadata['column'],
            'index_type': 'btree',
            'order': metadata['order'],
            'num_entries': metadata['num_entries'],
            'height': height,
            'node_count': node_count,
            'leaf_count': leaf_count
        }
        
        return stats
    
    def _calculate_height(self, node):
        if node is None:
            return 0
        if node.is_leaf:
            return 1
        return 1 + self._calculate_height(node.children[0])
    
    def _count_nodes(self, node):
        if node is None:
            return 0
        
        count = 1
        if not node.is_leaf:
            for child in node.children:
                count += self._count_nodes(child)
        
        return count
    
    def _count_leaves(self, node):
        if node is None:
            return 0
        if node.is_leaf:
            return 1
        
        count = 0
        for child in node.children:
            count += self._count_leaves(child)
        
        return count
    
    def list_indexes(self, table_name=None):
        indexes = []
        
        if not os.path.exists(self.index_path):
            return indexes
        
        for filename in os.listdir(self.index_path):
            if filename.endswith('_btree.idx'):
                parts = filename[:-10].rsplit('_', 1)
                if len(parts) >= 2:
                    idx_table = parts[0]
                    idx_column = parts[1]
                    
                    if table_name is None or idx_table == table_name:
                        indexes.append({
                            'table': idx_table,
                            'column': idx_column,
                            'type': 'btree'
                        })
        
        return indexes