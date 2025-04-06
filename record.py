import struct

HeaderSize = 12


class Record:
    def __init__(self, key, value, type):
        self.key = key
        self.value = value
        if key is None and value is None:
            self.keySize = 0
            self.valueSize = 0
        elif key is not None and value is None:
            self.keySize = len(key)
            self.valueSize = 0
        else:
            self.keySize = len(key)
            self.valueSize = len(value)
        self.type = type

    def getSize(self):
        return HeaderSize + self.keySize + self.valueSize

    def encode(self):
        return struct.pack("> I I I {}s {}s".format(self.keySize, self.valueSize), self.keySize, self.valueSize,
                           self.type.value, self.key.encode(), self.value.encode())

    @classmethod
    def decode(cls, header):
        keySize, valueSize, type = struct.unpack('> I I I', header)
        return keySize, valueSize, type
