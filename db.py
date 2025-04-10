import os
import string
from enum import Enum
from lock import RWLock
from typing import Dict
from data_file import DataFile
from record import Record, RecordType
from transaction import Transaction
from utils import delete_files

MasterPathName = "data"
MergePathName = "-merge"
DataFileName = "minibitcask.data"
MergeFinished = "MergeFinished.data"
TxFinished = "TxFinished"


class MiniBitcask:
    def __init__(self, dir_path: str, indexes):
        """
        初始化数据库实例
        :param dir_path: 数据库目录路径
        """
        # 处理绝对路径
        self.dirPath = os.path.abspath(dir_path)
        self.dataFile: DataFile = None
        self.rw = RWLock()  # 可重入锁
        self.indexes = indexes
        self.batch = {}
        self.TxNo = 1

    def open(self):
        # 如果数据库目录不存在，则新建
        os.makedirs(self.dirPath, exist_ok=True)
        self._load_mergefile()
        self._load_datafile()
        self._load_indexes_from_file()

    def _load_mergefile(self):
        master_data_file = os.path.join(self.dirPath, DataFileName)
        merge_data_path = self.dirPath + MergePathName
        merge_data_file = os.path.join(merge_data_path, DataFileName)
        merge_finished = os.path.join(merge_data_path, MergeFinished)

        # 如果没有merge目录直接返回
        if os.path.exists(merge_data_path) is False:
            return

        # 如果有merge目录但是没有数据文件，删除merge目录后返回
        if os.path.exists(merge_data_file) is False:
            delete_files(merge_data_path)
            return

        # 如果有merge目录，有数据文件，没有完成标识，删除merge目录后返回
        if os.path.exists(merge_finished) is False:
            delete_files(merge_data_path)
            return

        # 删除旧文件
        if os.path.exists(master_data_file):
            os.remove(master_data_file)
        # 移动新文件
        os.rename(merge_data_file, master_data_file)
        delete_files(merge_data_path)

    def _load_datafile(self):
        datafile = os.path.join(self.dirPath, DataFileName)
        self.dataFile = DataFile(open(datafile, 'ab+'), os.stat(datafile).st_size)

    def _load_indexes_from_file(self):
        """
        从数据文件加载索引（需要实现具体解析逻辑）
        这里需要根据实际数据格式解析文件内容，重建内存索引
        """

        batch = {}

        if self.dataFile is None:
            return

        offset = 0
        while True:
            record = self.dataFile.read(offset)
            if record is None:
                break
            if record.type in [RecordType.PUT.value, RecordType.DEL.value]:
                self.indexes.put(record.key, offset)
                if record.type == RecordType.DEL.value:
                    # 删除内存中的key
                    self.indexes.delete(record.key)
            elif record.type in [RecordType.TxPUT.value, RecordType.TxDEL.value]:
                if batch.get(record.TxNo) is None:
                    batch[record.TxNo] = []
                batch[record.TxNo].append((record.key, offset, record.type))
            if record.type == RecordType.Mark.value and record.key == TxFinished:
                for key, pos, type in batch.pop(record.TxNo):
                    self.indexes.put(key, pos)
                    if type == RecordType.TxDEL.value:
                        self.indexes.delete(key)

            offset += record.getSize()

    def close(self):
        if self.dataFile is None:
            raise ValueError("数据库实例不存在")
        self.dataFile.file.close()
        self.indexes.close()

    def get(self, key: str) -> string:
        """读取数据"""
        if len(key) == 0:
            return None

        with self.rw.read_lock:
            offset = self.indexes.get(key)
            if offset is None:
                raise ValueError("Key not found")
            record = self.dataFile.read(offset)

        return record.value


    def append_one_record(self, key: str, value: str | None, type: Enum):
        # 此处取出的这一次记录的写入偏移
        offset = self.dataFile.offset

        record = Record(key, value, type)

        # 写入磁盘
        self.dataFile.write(record)

        # 将offset写入内存
        self.indexes.put(key, offset)


    def append_batch_record(self, key: str, value: str, TxNo: int, type: Enum):
        if self.batch.get(TxNo) is None:
            self.batch[TxNo] = []

        self.batch[TxNo].append((key, value, type))
        return True


    def put(self, key: str, value: str, TxNo=None) -> bool:
        if key is None or len(key) == 0:
            return False

        if TxNo is None:
            return self._put_one_record(key, value)
        else:
            return self._put_batch_record(key, value, TxNo)


    def _put_one_record(self, key: str, value: str) -> bool:
        """写入数据"""
        with self.rw.write_lock:
            self.append_one_record(key, value, RecordType.PUT)

        return True


    def _put_batch_record(self, key, value, TxNo):
        return self.append_batch_record(key, value, TxNo, RecordType.TxPUT)


    def delete(self, key: str, TxNo=None):
        if key is None or len(key) == 0:
            return False

        if self.indexes.get(key) is None:
            return False

        if TxNo is None:
            return self._delete_one_record(key)
        else:
            return self._delete_batch_record(key, TxNo)


    def _delete_one_record(self, key: str) -> bool:
        """写入数据"""
        with self.rw.write_lock:
            self.append_one_record(key, None, RecordType.DEL)
            self.indexes.delete(key)
        return True


    def _delete_batch_record(self, key, TxNo):
        return self.append_batch_record(key, None, TxNo, RecordType.TxDEL)


    def inc_TxNo(self):
        # 这里要加锁，保证版本号是串行递增的：
        TxNo = self.TxNo
        self.TxNo = self.TxNo + 1
        return TxNo


    def start_Tx(self):
        return Transaction(self)


