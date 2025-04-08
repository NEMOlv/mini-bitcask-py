import os

from data_file import DataFile
from db import MiniBitcask, MergePathName, Record

MergeFinished = "MergeFinished.data"


def merge(activeDB: MiniBitcask, dirpath):
    mergeDB = MiniBitcask(dirpath + "-merge")
    mergeDB.open()
    if activeDB.dataFile is None:
        return
    for key in activeDB.indexes.keys():
        mergeDB.put(key, activeDB.get(key))

    # 手动写入merge完成标识文件，避免维护内存索引导致出错
    datafile = os.path.join(dirpath + "-merge", MergeFinished)
    MergeFinishedDataFile = DataFile(open(datafile, 'ab+'), os.stat(datafile).st_size)
    MergeFinishedDataFile.file.close()
    mergeDB.close()