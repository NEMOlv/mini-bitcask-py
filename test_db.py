import os

from unittest import TestCase

from index import HashMap,SkipList

from db import MiniBitcask

from merge import merge

# indexes = HashMap()
# merge_indexes = HashMap()

indexes = SkipList(16,0.5)
merge_indexes = SkipList(16,0.5)

class TestMiniBitcask(TestCase):
    def test_open_and_close(self):
        # 测试数据库是否会被正确打开
        # 声明数据库实例
        db = MiniBitcask("data",indexes)
        # 打开数据库
        db.open()
        # 测试数据库实例是否存在
        self.assertIsNotNone(db)

        # 测试数据库索引是否正常
        if db.indexes == dict():
            # 如果是首次打开数据库
            self.assertEqual(db.indexes,dict())

        else:
            # 不是首次打开数据库
            self.assertIsNotNone(db.indexes)

        # 测试数据库是否被正确关闭
        db.close()
        self.assertRaises(ValueError, db.put, "key","value")
        self.assertRaises(ValueError, db.get, "key")
        self.assertFalse(db.delete("key"))
        os.remove("data/minibitcask.data")

    def test_put(self):
        db = MiniBitcask("data",indexes)
        # 打开数据库
        db.open()

        # PUT不同key
        for i in range(100):
            db.put(f"key{i}", f"value{i}")


        for i in range(100):
            self.assertEqual(f"value{i}",db.get(f"key{i}"))

        # PUT相同key
        for i in range(100):
            db.put(f"key{i}", f"value{i}00")

        for i in range(100):
            self.assertEqual(f"value{i}00",db.get(f"key{i}"))

        # PUT 空key
        self.assertFalse(db.put(None,"value None"))
        self.assertFalse(db.put('',"value None"))

        # PUT 空value
        self.assertTrue(db.put("value-None",None))
        self.assertTrue(db.put("value-''",''))

        # 重启数据库
        db.close()
        db.open()

        # 重启后PUT数据
        # 0~99测试老数据，100~199测试新数据
        for i in range(200):
            db.put(f"key{i}", f"value{i}")

        for i in range(200):
            self.assertEqual(f"value{i}",db.get(f"key{i}"))

        db.close()
        os.remove("data/minibitcask.data")

    def test_get(self):
        db = MiniBitcask("data",indexes)
        # 打开数据库
        db.open()

        # 初始化数据
        for i in range(100):
            db.put(f"key{i}", f"value{i}")

        # GET 数据
        for i in range(100):
            self.assertEqual(f"value{i}",db.get(f"key{i}"))

        # 重新PUT数据后GET数据
        for i in range(100):
            db.put(f"key{i}", f"value{i}00")

        for i in range(100):
            self.assertEqual(f"value{i}00",db.get(f"key{i}"))

        # GET不存在的数据
        for i in range(100):
            self.assertRaises(ValueError,db.get,f"key{i}00")

        # DELETE数据后GET数据
        for i in range(50):
            db.delete(f"key{i}")

        for i in range(50):
            self.assertRaises(ValueError,db.get,f"key{i}")

        for i in range(50,100):
            self.assertEqual(f"value{i}00",db.get(f"key{i}"))

        # 重启后GET数据
        db.close()
        db.open()

        for i in range(50,100):
            self.assertEqual(f"value{i}00", db.get(f"key{i}"))

        db.close()
        os.remove("data/minibitcask.data")

    def test_delete(self):

        db = MiniBitcask("data", indexes)
        # 打开数据库
        db.open()

        # 初始化数据
        for i in range(100):
            db.put(f"key{i}", f"value{i}")

        # DELETE有效数据
        for i in range(50):
            self.assertTrue(db.delete(f"key{i}"))

        # DELETE无效数据
        for i in range(50):
            self.assertFalse(db.delete(f"key{i}"))
            self.assertFalse(db.delete(""))
            self.assertFalse(db.delete(None))

        # 重新PUT数据后再次删除
        for i in range(50):
            db.put(f"key{i}", f"value{i}")

        for i in range(50):
            self.assertTrue(db.delete(f"key{i}"))

        # 重启后DELET数据
        db.close()
        db.open()

        for i in range(50):
            self.assertFalse(db.delete(f"key{i}"))

        for i in range(50, 100):
            self.assertTrue(db.delete(f"key{i}"))
        db.close()
        os.remove("data/minibitcask.data")

    def test_merge(self):
        db = MiniBitcask("data", indexes)
        db.open()

        # 初始化数据
        for i in range(100):
            db.put(f"key{i}", f"value{i}")

        merge(db, "data")
        mergedb = MiniBitcask("data-merge", merge_indexes)
        mergedb.open()

        for key, _ in db.indexes.items():
            self.assertEqual(db.get(key), mergedb.get(key))

        db.close()
        mergedb.close()

        # 重启数据库
        db.open()

        for i in range(100):
            self.assertEqual(f"value{i}", db.get(f"key{i}"))

        db.close()
        os.remove("data/minibitcask.data")

    def test_transaction(self):
        db = MiniBitcask("data", indexes)
        db.open()

        # 1、批量写入数据
        Tx = db.start_Tx()

        # 1.1 未提交情况
        for i in range(100):
            Tx.put(f"key{i}", f"value{i}")
            self.assertRaises(ValueError, db.get, f"key{i}")

        # 1.2 提交情况
        Tx.commit()
        for i in range(100):
            self.assertEqual(f"value{i}", db.get(f"key{i}"))

        # 2、批量删除数据
        Tx = db.start_Tx()

        # 2.1 未提交情况
        for i in range(100):
            Tx.delete(f"key{i}")
            self.assertEqual(f"value{i}", db.get(f"key{i}"))

        # 2.2 提交情况
        Tx.commit()
        for i in range(100):
            self.assertRaises(ValueError, db.get, f"key{i}")

        # 3、批量写入和删除数据
        Tx = db.start_Tx()

        # 3.0 初始化数据
        for i in range(50, 100):
            db.put(f"key{i}", f"value{i}")

        # 3.1 未提交情况
        for i in range(50):
            Tx.put(f"key{i}", f"value{i}")
            self.assertRaises(ValueError, db.get, f"key{i}")

        for i in range(50, 100):
            Tx.delete(f"key{i}")
            self.assertEqual(f"value{i}", db.get(f"key{i}"))

        # 3.2 提交情况
        Tx.commit()
        for i in range(50):
            self.assertEqual(f"value{i}", db.get(f"key{i}"))
        for i in range(50, 100):
            self.assertRaises(ValueError, db.get, f"key{i}")

        # 重启
        db.close()
        db.open()

        # 4、重启后查询
        for i in range(50):
            self.assertEqual(f"value{i}", db.get(f"key{i}"))

        for i in range(50, 100):
            self.assertRaises(ValueError, db.get, f"key{i}")

        # 5、重启后批量写入数据
        Tx = db.start_Tx()

        # 5.1 未提交情况
        for i in range(100, 200):
            Tx.put(f"key{i}", f"value{i}")
            self.assertRaises(ValueError, db.get, f"key{i}")

        # 5.2 提交情况
        Tx.commit()
        for i in range(100, 200):
            self.assertEqual(f"value{i}", db.get(f"key{i}"))

        # 6、重启后批量删除数据
        Tx = db.start_Tx()

        # 6.1 未提交情况
        for i in range(100, 200):
            Tx.delete(f"key{i}")
            self.assertEqual(f"value{i}", db.get(f"key{i}"))

        # 6.2 提交情况
        Tx.commit()
        for i in range(100, 200):
            self.assertRaises(ValueError, db.get, f"key{i}")

        # 7、重启后批量写入和删除数据
        Tx = db.start_Tx()
        # 7.0 初始化数据
        for i in range(150, 200):
            db.put(f"key{i}", f"value{i}")

        # 7.1 未提交情况
        for i in range(100, 150):
            Tx.put(f"key{i}", f"value{i}")
            self.assertRaises(ValueError, db.get, f"key{i}")

        for i in range(150, 200):
            Tx.delete(f"key{i}")
            self.assertEqual(f"value{i}", db.get(f"key{i}"))

        # 7.2 提交情况
        Tx.commit()
        for i in range(100, 150):
            self.assertEqual(f"value{i}", db.get(f"key{i}"))

        for i in range(150, 200):
            self.assertRaises(ValueError, db.get, f"key{i}")

        # 8.自动提交或回滚
        with db.start_Tx() as Tx:
            for i in range(500, 1000):
                Tx.put(f"key{i}", f"value{i}")

        for i in range(500, 1000):
            self.assertEqual(f"value{i}", db.get(f"key{i}"))

        # 关闭
        db.close()
        os.remove("data/minibitcask.data")