# standard libraries
import datetime
import shutil
# third party libraries
import pytest
# first party libraries
import kayvee


def cleanup():
    try:
        shutil.rmtree('test')
    except:
        pass


def test_aws():
    cleanup()
    store = kayvee.filesystem.Store('test')
    store['a'] = b'a'
    assert store['a'] == b'a'
    assert list(store) == ['a', ]
    store.prefix = 'test'
    store['a'] = b'a'
    store['b'] = b'b'
    assert list(store) == ['test/a', 'test/b']
    assert 'a' in store
    assert 'b' in store
    del store['a']
    assert list(store) == ['test/b', ]
    assert len(store) == 1
    store.clear()
    assert len(store) == 0
    cleanup()


if __name__ == '__main__':
    pytest.main()