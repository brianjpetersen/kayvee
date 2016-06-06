# standard libraries
import os
import collections
import uuid
import contextlib
import shutil
# third party libraries
pass
# first party libraries
pass


__where__ = os.path.dirname(os.path.abspath(__file__))


@contextlib.contextmanager
def open_for_atomic_write(path, sync=True):
    directory = os.path.dirname(path)
    temporary_path = os.path.join(directory, '.{}'.format(uuid.uuid4()))
    f = open(temporary_path, 'wb')
    try:
        yield f
        if sync:
            f.flush()
            os.fsync(f)
    except:
        try:
            f.close()
            os.remove(temporary_filename)
        except:
            pass
        raise
    else:
        f.close()
    os.replace(temporary_path, path)
    if sync:
        f = os.open(directory, os.O_DIRECTORY)
        os.fsync(f)
        os.close(f)


def atomic_write(path, content, sync=True):
    with open_for_atomic_write(path, sync) as f:
        f.write(content)


class Store(collections.abc.MutableMapping):
    
    def __init__(self, directory, prefix='', sync=True):
        os.makedirs(directory, exist_ok=True)
        self.directory = os.path.abspath(directory)
        if prefix.startswith('/'):
            prefix = prefix[1:]
        self.prefix = prefix
        self.sync = sync
    
    def __repr__(self):
        return '{}.{}(directory={}, prefix={})'.format(
            self.__module__,
            self.__class__.__name__,
            repr(self.directory),
            repr(self.prefix),
        )
    
    def __len__(self):
        return sum(1 for key in self)
    
    def __iter__(self):
        path = os.path.join(self.directory, self.prefix)
        for _, _, files in os.walk(path):
            for file in files:
                key = os.path.join(self.prefix, file)
                yield key
    
    def __getitem__(self, key):
        key = os.path.join(self.directory, self.prefix, key)
        if os.path.abspath(key).startswith(self.directory) == False:
            raise ValueError
        try:
            with open(key, 'rb') as f:
                value = f.read()
            return value
        except FileNotFoundError:
            raise KeyError from None
        
    def __setitem__(self, key, value):
        key = os.path.join(self.directory, self.prefix, key)
        if os.path.abspath(key).startswith(self.directory) == False:
            raise ValueError
        directory = os.path.dirname(key)
        os.makedirs(directory, exist_ok=True)
        atomic_write(key, value, self.sync)
    
    def __delitem__(self, key):
        key = os.path.join(self.directory, self.prefix, key)
        if os.path.abspath(key).startswith(self.directory) == False:
            raise ValueError
        try:
            os.remove(key)
        except FileNotFoundError:
            raise KeyError from None
    
    def clear(self):
        directory = os.path.join(self.directory, self.prefix)
        shutil.rmtree(directory, ignore_errors=True)
