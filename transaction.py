from record import Record,RecordType

TxFinished = "TxFinished"


class Transaction:
    def __init__(self, db):
        self.db = db
        self.tx_no = db.inc_TxNo()
        self._active = True

    def put(self, key, value):
        """在事务中插入数据"""
        if not self._active:
            raise RuntimeError("Transaction closed")
        self.db.put(key, value, self.tx_no)

    def delete(self, key):
        """在事务中插入数据"""
        if not self._active:
            raise RuntimeError("Transaction closed")
        self.db.delete(key, self.tx_no)

    def commit(self):
        """提交事务"""
        if self._active is False:
            return

        with self.db.rw.write_lock:
            if self.tx_no not in self.db.batch:
                return False

            # 写入磁盘
            positions = []
            for key, value, type in self.db.batch.pop(self.tx_no):
                # 此处取出的这一次记录的写入偏移
                offset = self.db.dataFile.offset
                record = Record(key, value, type ,self.tx_no)
                # 写入磁盘
                self.db.dataFile.write(record)
                positions.append((key, offset, type))  # 示例位置

            # 更新内存索引
            for key, pos, type in positions:
                self.db.indexes.put(key ,pos)
                if type == RecordType.TxDEL:
                    self.db.indexes.delete(key)

            # 手动写入完成标识，避免维护内存索引导致出错
            record = Record("TxFinished", None, RecordType.Mark, self.tx_no)
            self.db.dataFile.write(record)

            self._active = False

        return True

    def __enter__(self):
        """支持 with 语法"""
        return self

    def __exit__(self, exc_type, *_):
        """退出上下文时自动提交或回滚（此处简化处理）"""
        if exc_type is None:
            self.commit()
        else:
            self._active = False