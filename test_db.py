from unittest import TestCase

from db import MiniBitcask


class TestMiniBitcask(TestCase):
    def test_open_and_close(self):

        # 测试数据库是否会被正确打开
        # 声明数据库实例
        db = MiniBitcask("data")
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
        self.assertRaises(ValueError, db.delete, "key")

    def test_put(self):
        db = MiniBitcask("data")
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



    def test_get(self):
        db = MiniBitcask("data")
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

    def test_delete(self):
        db = MiniBitcask("data")
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
        for i in range(50,100):
            self.assertTrue(db.delete(f"key{i}"))