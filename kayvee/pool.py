# standard libraries
import os
import queue
import contextlib
# third party libraries
pass
# first party libraries
pass


__where__ = os.path.dirname(os.path.abspath(__file__))


class Pool:
    
    def __init__(self, connect, min_size=1, max_size=1):
        self.queue = queue.Queue()
        self.connect = connect
        self._min_size = min_size
        self._max_size = max_size
        self.grow()
    
    def get_min_size(self):
        return self._min_size
    
    def set_min_size(self, min_size):
        if (min_size > self.max_size) or (min_size < 1):
            raise ValueError
        self._min_size = min_size
        self.grow()
    
    def get_max_size(self):
        return self._max_size
    
    def set_max_size(self, max_size):
        if max_size < self.min_size:
            raise ValueError
        self._max_size = max_size
        self.shrink()
    
    min_size = property(get_min_size, set_min_size)
    max_size = property(get_max_size, set_max_size)
    
    def grow(self):
        while len(self) < self.min_size:
            connection = self.connect()
            self.add(connection)
    
    def shrink(self):
        while len(self) >= self.max_size:
            self.pop()
    
    def empty(self):
        self.queue = queue.Queue()
    
    def add(self, connection):
        if len(self) < self.max_size:
            self.queue.put(connection)
    
    def pop(self):
        self.queue.get(block=False)
    
    @contextlib.contextmanager
    def get(self):
        try:
            try:
                connection = self.queue.get(block=False)
            except:
                connection = self.connect()
            yield connection
            self.add(connection)
        finally:
            self.grow()
    
    def __len__(self):
        return self.queue.qsize()
