class HashIndexEntry:
    def __init__(self, key_value, page_id, slot_id):
        self.key_value = key_value  
        self.page_id = page_id      
        self.slot_id = slot_id      
    

class BPlusTreeNode:
    def __init__(self, is_leaf=False, order=4):
        self.is_leaf = is_leaf
        self.order = order  
        self.keys = []  
        self.children = []  
        self.values = []  
        self.next_leaf = None 
        self.parent = None
    
    def is_full(self):
        if self.is_leaf:
            return len(self.keys) >= self.order
        else:
            return len(self.children) > self.order
    
class BPlusTreeIndexEntry:
    def __init__(self, key_value, page_id, slot_id):
        self.key_value = key_value
        self.page_id = page_id
        self.slot_id = slot_id
    
    def __lt__(self, other):
        return self.key_value < other.key_value
    
    def __eq__(self, other):
        return (self.key_value == other.key_value and 
                self.page_id == other.page_id and 
                self.slot_id == other.slot_id)