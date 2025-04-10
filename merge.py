import os
from data_file import DataFile
from db import MiniBitcask
from index import HashMap, SkipList

MergeFinished = "MergeFinished.data"

index = SkipList(16, 0.5)


# index = HashMap()
def merge(activeDB: MiniBitcask, dirpath):
    if activeDB.dataFile is None:
        return
    records = []
    with activeDB.rw.write_lock:
        for _, pos in activeDB.indexes.items():
            records.append(activeDB.dataFile.read(pos))

    mergeDB = MiniBitcask(dirpath + "-merge", index)
    mergeDB.open()
    with mergeDB.rw.write_lock:
        # 写入磁盘
        for record in records:
            # print(record)
            mergeDB.dataFile.write(record)

            # 手动写入merge完成标识文件，避免维护内存索引导致出错
    datafile = os.path.join(dirpath + "-merge", MergeFinished)
    MergeFinishedDataFile = DataFile(open(datafile, 'ab+'), os.stat(datafile).st_size)
    MergeFinishedDataFile.file.close()
    mergeDB.close()
