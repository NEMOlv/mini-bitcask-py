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
            self.keySize = len(key.encode())
            self.valueSize = 0
        else:
            self.keySize = len(key.encode())
            self.valueSize = len(value.encode())
        self.type = type

    def getSize(self):
        return HeaderSize + self.keySize + self.valueSize

    def encode(self):
        encKey = self.key.encode()
        encValue = self.value.encode() if self.value is not None else b''

        return struct.pack("> I I I {}s {}s".format(self.keySize, self.valueSize), self.keySize, self.valueSize,
                           self.type.value, encKey, encValue)

    @classmethod
    def decode(cls, header):
        keySize, valueSize, type = struct.unpack('> I I I', header)
        return keySize, valueSize, type
