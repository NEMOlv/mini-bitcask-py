from record import HeaderSize
from record import Record


class DataFile:
    def __init__(self, file, offset):
        self.file = file
        self.offset = offset

    def read(self, offset):
        self.file.seek(offset)
        header = self.file.read(HeaderSize)
        if header == b'':
            return None
        key = None
        value = None
        keySize, valueSize, type, TxNo = Record.decode(header)
        if keySize > 0:
            key = self.file.read(keySize).decode()
        if valueSize > 0:
            value = self.file.read(valueSize).decode()
        record = Record(key,value,type,TxNo)
        self.file.seek(0)
        return record

    def write(self, record):
        self.file.seek(self.offset)
        self.file.write(record.encode())
        # 此处记录的是下一个记录的写入偏移
        self.offset = self.file.tell()
