import threading

class RWLock:
    def __init__(self):
        self.lock = threading.Lock()
        self.cnt = 0
        self.extra = threading.Lock()

    class _ReadContextManager:
        def __init__(self, rwlock):
            self.rwlock = rwlock

        def __enter__(self):
            self.rwlock.read_acquire()
            return self  # 可返回自身，但非必须

        def __exit__(self, exc_type, exc_val, exc_tb):
            self.rwlock.read_release()
            return False  # 不处理异常，按正常流程传递

    class _WriteContextManager:
        def __init__(self, rwlock):
            self.rwlock = rwlock

        def __enter__(self):
            self.rwlock.write_acquire()
            return self  # 同上

        def __exit__(self, exc_type, exc_val, exc_tb):
            self.rwlock.write_release()
            return False

    def read_acquire(self):
        with self.extra:
            self.cnt += 1
            if self.cnt == 1:
                self.lock.acquire()

    def read_release(self):
        with self.extra:
            self.cnt -= 1
            if self.cnt == 0:
                self.lock.release()

    def write_acquire(self):
        self.lock.acquire()

    def write_release(self):
        self.lock.release()

    @property
    def read_lock(self):
        return self._ReadContextManager(self)

    @property
    def write_lock(self):
        return self._WriteContextManager(self)