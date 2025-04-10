from functools import wraps

from basestruct import SkipList
import time


def timeit(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print(f"Function {func.__name__} took {end_time - start_time:.4f} seconds to execute.")
        print(f"Function {func.__name__} took {(end_time - start_time)/n*1e9:.9f} ns/op to execute.")
        return result
    return wrapper

skip_list = SkipList(1000,0.5)
HashMap = {}
n = 1000000

@timeit
def insert(n):
    for i in range(n):
        skip_list.insert(f"key{i}", f"value{i}")
        # HashMap[f"key{i}"] = f"value{i}"

@timeit
def search(n):
    for i in range(n):
        skip_list.search(f"key{i}")
        # HashMap.get(f"key{i}")

@timeit
def delete(n):
    for i in range(n):
        skip_list.delete(f"key{i}")
        # HashMap.pop(f"key{i}")

if __name__ == '__main__':
    insert(n)
    search(n)
    delete(n)