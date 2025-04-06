from db import MiniBitcask, MergePathName, Record,RecordType


def merge(activeDB:MiniBitcask):
    mergeDB = MiniBitcask(MergePathName)
    mergeDB.open()
    with activeDB.mu and mergeDB.mu:
        if activeDB.dataFile is None:
            return
        for key in activeDB.indexes.keys():
            mergeDB.put(key,activeDB.get(key))
        mergeDB.close()


