import os
import string
from enum import Enum
from threading import RLock
from typing import Dict, Optional
from data_file import DataFile
from record import Record

DataFileName = "minibitcask.data"


class RecordType(Enum):
    PUT = 0
    DEL = 1


class MiniBitcask:
    def __init__(self, dir_path: str):
        """
        初始化数据库实例
        :param dir_path: 数据库目录路径
        """
        # 处理绝对路径
        self.dirPath = os.path.abspath(dir_path)
        self.dataFile: DataFile = None
        self.mu = RLock()  # 可重入锁
        self.indexes: Dict[str, int] = {}

    def open(self):
        # 如果数据库目录不存在，则新建
        os.makedirs(self.dirPath, exist_ok=True)
        self.dataFile = DataFile(open(os.path.join(self.dirPath,DataFileName), 'ab+'), 0)
        self._load_indexes_from_file()

    def _load_indexes_from_file(self):
        """
        从数据文件加载索引（需要实现具体解析逻辑）
        这里需要根据实际数据格式解析文件内容，重建内存索引
        """
        if self.dataFile is None:
            return

        offset = 0
        while True:
            record = db.dataFile.read(offset)
            if record is None:
                break
            db.indexes[record.key] = offset
            if record.type == RecordType.DEL:
                # 删除内存中的key
                self.indexes.pop(record.key)
            offset += record.getSize()

    def close(self):
        if self.dataFile is None:
            raise ValueError("数据库实例不存在")
        db.dataFile.file.close()

    def get(self, key: str) -> string:
        """读取数据"""
        if len(key) == 0:
            return None

        with self.mu:
            offset = self.indexes.get(key)
            print(offset)
            if offset is None:
                raise ValueError("Key not found")
            record = self.dataFile.read(offset)

        return record.value

    def put(self, key: str, value: str) -> bool:
        """写入数据"""
        if len(key) == 0:
            return False

        with self.mu:
            # 此处取出的这一次记录的写入偏移
            offset = self.dataFile.offset

            record = Record(key, value, RecordType.PUT)

            # 写入磁盘
            self.dataFile.write(record)

            # 将offset写入内存
            self.indexes[key] = offset

        return True

    def delete(self, key: str):
        if len(key) == 0:
            return False
        with self.mu:
            if self.indexes.get(key) is None:
                raise ValueError("Key not found")
            record = Record(key, None, RecordType.DEL)
            # 写入磁盘
            self.dataFile.write(record)

            # 删除内存中的key
            self.indexes.pop(key)

# 使用示例
if __name__ == "__main__":
    # 初始化数据库
    db = MiniBitcask("D:\WorkSpace\PythonWS\mini-bitcask-py")
    # 打开数据库
    db.open()

    # 写入数据
    db.put("key1", "value1")
    db.put("key2", "value2")
    db.put("key3", "value3")

    # 读取数据
    print(db.get("key1"))  # 输出: b'value1'
    print(db.get("key2"))  # 输出: b'value2'
    # 关闭数据库
    db.close()
