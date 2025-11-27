from typing import Any, Dict, List, Optional

class Schema:
    def __init__(self, attributes: Optional[List[Dict[str, Any]]] = None):
        self.attributes = attributes if attributes is not None else []

    def add_attribute(self, name, type, size):
        if any(attr['name'] == name for attr in self.attributes):
            raise ValueError(f"Attribute '{name}' already exists in the schema.")
        self.attributes.append({'name': name, 'type': type, 'size': size})

    def get_attributes(self):
        return self.attributes

    def get_attribute(self, name):
        for attr in self.attributes:
            if attr['name'] == name:
                return attr
        raise ValueError(f"Attribute '{name}' not found in the schema.")
    
    def get_metadata(self):
        return [(attr['name'], attr['type'], attr['size']) for attr in self.attributes]
    
    def serialize(self):
        data = bytearray()
        data.extend(len(self.attributes).to_bytes(2, byteorder='little'))

        for attr in self.attributes:
            name_bytes = attr['name'].encode('utf-8')
            data.extend(len(name_bytes).to_bytes(2, byteorder='little'))
            data.extend(name_bytes)
            
            type_bytes = attr['type'].encode('utf-8')
            data.extend(len(type_bytes).to_bytes(2, byteorder='little'))
            data.extend(type_bytes)

            data.extend(attr['size'].to_bytes(2, byteorder='little'))
        
        return data
    
    def deserialize(self, data: bytes):
        offset = 0
        attributes = []

        attr_count = int.from_bytes(data[offset:offset+2], byteorder='little')
        offset += 2

        for _ in range(attr_count):
            name_len = int.from_bytes(data[offset:offset+2], byteorder='little')
            offset += 2
            name = data[offset:offset+name_len].decode('utf-8')
            offset += name_len

            type_len = int.from_bytes(data[offset:offset+2], byteorder='little')
            offset += 2
            type = data[offset:offset+type_len].decode('utf-8')
            offset += type_len

            size = int.from_bytes(data[offset:offset+2], byteorder='little')
            offset += 2

            attributes.append({'name': name, 'type': type, 'size': size})

        return Schema(attributes)
    
    def __str__(self):
        lines = ["Name".ljust(15) + "Type".ljust(10) + "Size"]
        lines.append("-" * 32)
        for attr in self.attributes:
            lines.append(f"{attr['name']:<15}{attr['type']:<10}{attr['size']}")
        return "\n".join(lines)
        
