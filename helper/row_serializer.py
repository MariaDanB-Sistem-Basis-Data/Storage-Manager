from .data_encoder import DataEncoder

class RowSerializer:
    def __init__(self):
        self.encoder = DataEncoder()

    def serialize(self, schema, record):
        byte_array = bytearray()
        fields = schema.get_attributes()

        for field in fields:
            field_name = field["name"]
            field_type = field.get("type")
            field_size = field.get("size")

            if field_type == "int":
                byte_array += self.encoder.encode_int(record[field_name])
            elif field_type == "float":
                byte_array += self.encoder.encode_float(record[field_name])
            elif field_type == "char":
                byte_array += self.encoder.encode_char(record[field_name], field_size)
            elif field_type == "varchar":
                byte_array += self.encoder.encode_varchar(record[field_name], field_size)

        return bytes(byte_array)

    def deserialize(self, schema, byte_data):
        record = {}
        offset = 0

        fields = schema.get_attributes()

        for field in fields:
            field_name = field["name"]
            field_type = field.get("type")
            field_size = field.get("size")

            if field_type == "int":
                value, offset = self.encoder.decode_int(byte_data, offset)
            elif field_type == "float":
                value, offset = self.encoder.decode_float(byte_data, offset)
            elif field_type == "char":
                value, offset = self.encoder.decode_char(byte_data, offset, field_size)
            elif field_type == "varchar":
                value, offset = self.encoder.decode_varchar(byte_data, offset)

            record[field_name] = value

        return record