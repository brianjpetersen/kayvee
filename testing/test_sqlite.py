# standard libraries
import datetime
# third party libraries
import pytest
# first party libraries
import kayvee


def test_sqlite():
    store = kayvee.sqlite.Store(':memory:', sync='normal')
    assert ('a' in store) == False
    with pytest.raises(KeyError):
        store['a']
    with pytest.raises(KeyError):
        del store['a']
    assert len(store) == 0
    store['a'] = '1'
    assert ('a' in store) == True
    assert store['a'] == '1'
    assert len(store) == 1
    del store['a']
    assert len(store) == 0
    with pytest.raises(KeyError):
        store['a']
    store['a'] = '1'
    store['b'] = '2'
    assert len(store) == 2
    assert list(store) == ['a', 'b']
    assert list(store.keys()) == ['a', 'b']
    assert list(store.values()) == ['1', '2']
    assert list(store.items()) == [('a', '1'), ('b', '2')]
    store.prefix = 'a'
    store['a'] = '1'
    assert len(store) == 1
    assert list(store.keys()) == ['a/a', ]
    store.prefix = ''
    assert len(store) == 3
    store.clear()
    assert len(store) == 0
    store.update({'a': '1', 'b': '2'})
    assert len(store) == 2
    store.prefix = b''
    store[b'a'] = b'1'
    assert store[b'a'] == b'1'
    assert store.pop(b'a') == b'1'
    assert len(store) == 0
    store.prefix = ''
    assert len(store) == 2
    store.clear()
    assert len(store) == 0
    store['a'] = '1'
    store.prefix = 'a'
    store['a'] = '1'
    store.prefix = ''
    assert len(store) == 2
    store.prefix = 'a'
    store.clear()
    store.prefix = ''
    assert len(store) == 1
    assert store['a'] == '1'
    store = kayvee.sqlite.Store(':memory:', sync='normal')
    store['a'] = '1'
    store['b'] = '2'
    store['c'] = '3'
    store['d'] = '4'
    assert list(store.before('c', inclusive=True)) == [
        ('a', '1'), ('b', '2'), ('c', '3'), 
    ] 
    assert list(store.before('c', inclusive=False)) == [
        ('a', '1'), ('b', '2'),  
    ]
    assert list(store.after('b', inclusive=True)) == [
        ('b', '2'), ('c', '3'), ('d', '4'),
    ]
    assert list(store.after('b', inclusive=False)) == [
        ('c', '3'), ('d', '4'),
    ]
    assert list(store.after('b', inclusive=False, ascending=False)) == [
        ('d', '4'), ('c', '3'), 
    ]
    store.prefix = 'a'
    store['a'] = 'a'
    store['b'] = 'b'
    store['c'] = 'c'
    store['d'] = 'd'
    assert list(store.before('c', inclusive=True)) == [
        ('a/a', 'a'), ('a/b', 'b'), ('a/c', 'c'), 
    ]
    assert list(store.between('b', 'c', True, True)) == [
        ('a/b', 'b'), ('a/c', 'c'), 
    ]
    
    
    

"""
def test_pool():
    stores = kayvee.sqlite.Stores(':memory:', sync='normal')
    assert stores.min_size == stores.max_size == 1
    stores.max_size = 100
    stores.min_size = 20
    assert len(stores) == 20
    with stores.get() as store:
        pass
"""

if __name__ == '__main__':
    pytest.main()


"""
import datetime
import random
import os
import time

store = kayvee.sqlite.Store('test.db', sync='normal', timeout=1000)

#store.clear()

print(len(store))

N = 10000000

start = datetime.datetime.now()
for _ in range(N):
    i = random.randint(0, 100*N)
    store[str(i)] = str(i)
    #time.sleep(0.0001)
stop = datetime.datetime.now()
print(float(N)/(stop - start).total_seconds())

start = datetime.datetime.now()
for _ in range(N):
    store[str(i)]
stop = datetime.datetime.now()
print(float(N)/(stop - start).total_seconds())
"""