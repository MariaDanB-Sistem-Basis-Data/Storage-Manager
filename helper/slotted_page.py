import struct

PAGE_SIZE = 4096 
HEADER_SIZE = 4
SLOT_SIZE = 8

class SlottedPage:
    def __init__(self):
        self.data = bytearray(PAGE_SIZE)
        self.record_count = 0
        self.free_space_offset = HEADER_SIZE
        self.free_record_offset = PAGE_SIZE
        self.slots = []

    def add_record(self, record_bytes):
        record_length = len(record_bytes)
        
        record_start = self.free_record_offset - record_length
        if record_start < self.free_space_offset + SLOT_SIZE:
            raise Exception("Not enough space to add record")

        self.data[record_start:self.free_record_offset] = record_bytes

        self.data[self.free_space_offset:self.free_space_offset + SLOT_SIZE] = struct.pack('<II', record_start, record_length)

        self.slots.append((record_start, record_length))
        self.free_space_offset += SLOT_SIZE
        self.free_record_offset = record_start
        self.record_count += 1

    
    def serialize(self):
        header = struct.pack("<HH", self.record_count, self.free_space_offset)
        self.data[0:HEADER_SIZE] = header
        return bytes(self.data)
    
    def load(self, byte_data):
        self.data = bytearray(byte_data)
        self.record_count, self.free_space_offset = struct.unpack("<HH", self.data[:HEADER_SIZE])
        self.slots = []
        for i in range(self.record_count):
            offset = HEADER_SIZE + i * SLOT_SIZE
            record_start, record_length = struct.unpack("<II", self.data[offset:offset + SLOT_SIZE])
            self.slots.append((record_start, record_length))

    def get_record(self, slot_index):  
        record_start, record_length = self.slots[slot_index]
        return bytes(self.data[record_start:record_start + record_length])
