"""
Generated protobuf class for PublicAggreBookTickerV3Api
Based on the MEXC WebSocket API documentation
"""

class PublicAggreBookTickerV3Api:
    def __init__(self):
        self.bidPrice = ""
        self.bidQuantity = ""
        self.askPrice = ""
        self.askQuantity = ""
    
    def ParseFromString(self, data):
        """Parse protobuf data from string"""
        try:
            pos = 0
            while pos < len(data):
                # Read field header (varint)
                field_num, wire_type, pos = self._read_field_header(data, pos)
                
                if field_num == 1:  # bidPrice
                    self.bidPrice, pos = self._read_string(data, pos)
                elif field_num == 2:  # bidQuantity
                    self.bidQuantity, pos = self._read_string(data, pos)
                elif field_num == 3:  # askPrice
                    self.askPrice, pos = self._read_string(data, pos)
                elif field_num == 4:  # askQuantity
                    self.askQuantity, pos = self._read_string(data, pos)
                else:
                    # Skip unknown field
                    pos = self._skip_field(data, pos, wire_type)
            
            return True
        except Exception as e:
            print(f"Error parsing book ticker protobuf: {e}")
            return False
    
    def _read_field_header(self, data: bytes, pos: int):
        """Read protobuf field header"""
        field_num, pos = self._read_varint(data, pos)
        wire_type = field_num & 0x7
        field_num >>= 3
        return field_num, wire_type, pos
    
    def _read_varint(self, data: bytes, pos: int):
        """Read protobuf varint"""
        result = 0
        shift = 0
        while pos < len(data):
            byte = data[pos]
            pos += 1
            result |= (byte & 0x7F) << shift
            if (byte & 0x80) == 0:
                break
            shift += 7
        return result, pos
    
    def _read_string(self, data: bytes, pos: int):
        """Read protobuf string"""
        length, pos = self._read_varint(data, pos)
        if pos + length > len(data):
            return "", pos
        string_data = data[pos:pos + length]
        pos += length
        try:
            return string_data.decode('utf-8'), pos
        except:
            return "", pos
    
    def _skip_field(self, data: bytes, pos: int, wire_type: int):
        """Skip unknown field"""
        if wire_type == 0:  # varint
            _, pos = self._read_varint(data, pos)
        elif wire_type == 2:  # string/bytes
            length, pos = self._read_varint(data, pos)
            pos += length
        elif wire_type == 1:  # 64-bit
            pos += 8
        elif wire_type == 5:  # 32-bit
            pos += 4
        return pos
    
    def SerializeToString(self):
        """Serialize to protobuf string"""
        # Placeholder implementation
        return b""
