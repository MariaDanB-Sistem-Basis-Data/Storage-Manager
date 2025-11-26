class HashIndexEntry:
    def __init__(self, key_value, page_id, slot_id):
        self.key_value = key_value  
        self.page_id = page_id      
        self.slot_id = slot_id      
    
    def __repr__(self):
        return f"HashIndexEntry(key={self.key_value}, page={self.page_id}, slot={self.slot_id})"