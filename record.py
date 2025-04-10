import struct
from enum import Enum

HeaderSize = 16

class RecordType(Enum):
    PUT = 0
    DEL = 1
    TxPUT = 2
    TxDEL = 3
    Mark = 4

class Record:
    def __init__(self, key, value, type, TxNo=0):
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
        self.TxNo = TxNo

    def getSize(self):
        return HeaderSize + self.keySize + self.valueSize

    def encode(self):
        encKey = self.key.encode()
        encValue = self.value.encode() if self.value is not None else b''
        record_type = self.type if type(self.type) == int else self.type.value
        return struct.pack("> I I I I {}s {}s".format(self.keySize, self.valueSize), self.keySize, self.valueSize,
                           record_type, self.TxNo, encKey, encValue)

    @classmethod
    def decode(cls, header):
        keySize, valueSize, type, TxNo = struct.unpack('> I I I I', header)
        return keySize, valueSize, type, TxNo

    def __str__(self):
        return f"(Key:{self.key},Value:{self.value},type:{self.type})"
