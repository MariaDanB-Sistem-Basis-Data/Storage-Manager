import struct

class DataEncoder:

    def __init__(self):
        pass

    def encode_int(self, value):
        return struct.pack('i', int(value))
    
    def encode_float(self, value):
        return struct.pack('f', float(value))

    def encode_char(self, value, length):
        encoded = str(value).encode('utf-8')[:length]
        padded = encoded.ljust(length, b'\x00')
        return struct.pack(f'{length}s', padded)
    
    def encode_varchar(self, value, max_length):
        encoded = str(value).encode('utf-8')[:max_length]
        length = len(encoded)
        return struct.pack("<I", length) + encoded

    def decode_int(self, byte_data, offset):
        value = struct.unpack_from('i', byte_data, offset)[0]
        return value, offset + 4
    
    def decode_float(self, byte_data, offset):
        value = struct.unpack_from('f', byte_data, offset)[0]
        return round(value,2), offset + 4
    
    def decode_char(self, byte_data, offset, length):
        raw_bytes = struct.unpack_from(f'{length}s', byte_data, offset)[0]
        value = raw_bytes.decode('utf-8').rstrip('\x00')
        return value, offset + length
    
    def decode_varchar(self, byte_data, offset):
        length = struct.unpack_from("<I", byte_data, offset)[0]
        offset += 4
        raw_bytes = struct.unpack_from(f'{length}s', byte_data, offset)[0]
        value = raw_bytes.decode('utf-8')
        return value, offset + length
    
