import random

class SkipListNode:
    def __init__(self, key, value, level):
        self.key = key
        self.value = value
        # 存储指向不同层次下一个节点的指针，初始化为None
        self.forward = [None] * (level + 1)


class SkipList:
    def __init__(self, max_level=16, p=0.5):
        self.max_level = max_level
        self.p = p
        # 初始化当前跳表的实际层数为0
        self.level = 0
        # 创建头节点，其值为None，包含max_level + 1个指针
        self.header = SkipListNode(None, None, max_level)

    def random_level(self):
        level = 0
        while random.random() < self.p and level < self.max_level:
            level += 1
        return level

    def insert(self, key, value):
        update = [None] * (self.max_level + 1)
        current = self.header

        # 找到每个层次的更新路径
        for i in range(self.level, -1, -1):
            while current.forward[i] and current.forward[i].key < key:
                current = current.forward[i]
            update[i] = current

        current = current.forward[0]

        if current is None or current.key != key:
            new_level = self.random_level()

            if new_level > self.level:
                for i in range(self.level + 1, new_level + 1):
                    update[i] = self.header
                self.level = new_level

            new_node = SkipListNode(key, value, new_level)

            # 更新指针
            for i in range(new_level + 1):
                new_node.forward[i] = update[i].forward[i]
                update[i].forward[i] = new_node

            # print(f"Inserted key: {key}, value: {value}")
        else:
            pass
            # print(f"Key {key} already exists")

    def search(self, key):
        current = self.header

        for i in range(self.level, -1, -1):
            while current.forward[i] and current.forward[i].key < key:
                current = current.forward[i]

        current = current.forward[0]

        if current and current.key == key:
            # print(f"Found key: {key}, value: {current.value}")
            return current.value
        else:
            # print(f"Key {key} not found")
            return None

    def update(self, key, new_value):
        current = self.header

        for i in range(self.level, -1, -1):
            while current.forward[i] and current.forward[i].key < key:
                current = current.forward[i]

        current = current.forward[0]

        if current and current.key == key:
            current.value = new_value
            # print(f"Updated key: {key}, new value: {new_value}")
        else:
            # print(f"Key {key} not found")
            pass

    def delete(self, key):
        update = [None] * (self.max_level + 1)
        current = self.header

        # 找到每个层次的更新路径
        for i in range(self.level, -1, -1):
            while current.forward[i] and current.forward[i].key < key:
                current = current.forward[i]
            update[i] = current

        current = current.forward[0]

        if current and current.key == key:
            # 更新指针
            for i in range(self.level + 1):
                if update[i].forward[i] != current:
                    break
                update[i].forward[i] = current.forward[i]

            # 调整当前跳表的层数
            while self.level > 0 and self.header.forward[self.level] is None:
                self.level -= 1

            # print(f"Deleted key: {key}")
        else:
            # print(f"Key {key} not found")
            pass
    def iterate(self):
        result = []
        current = self.header.forward[0]

        while current:
            result.append((current.key, current.value))
            current = current.forward[0]

        return result