from basestruct import SkipList as sl

class AbstractIndex(object):
    def put(self, key, value):
        pass

    def get(self, key):
        pass

    def delete(self, key):
        pass

    def size(self):
        pass

    def items(self):
        pass

    def close(self):
        pass

class HashMap(AbstractIndex):
    def __init__(self):
        self.index = {}

    def put(self, key, value):
        self.index[key] = value
        return True

    def get(self, key):
        return self.index.get(key)

    def delete(self, key):
        if self.index.get(key) is None:
            return
        self.index.pop(key)

    def size(self):
        return len(self.index)

    def items(self):
        return self.index.items()

    def close(self):
        self.index.clear()

class SkipList(AbstractIndex):
    def __init__(self,max_level,p):
        self.index = sl(max_level,p)

    def put(self, key, value):
        if self.index.search(key) is None:
            self.index.insert(key, value)
        else:
            self.index.update(key,value)
        return True

    def get(self, key):
        return self.index.search(key)

    def delete(self, key):
        self.index.delete(key)

    def size(self):
        pass
        # return self.index.size

    def items(self):
        return self.index.iterate()

    def close(self):
        del self