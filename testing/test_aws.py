# standard libraries
import datetime
# third party libraries
import pytest
# first party libraries
import secrets
import kayvee


def test_aws():
    store = kayvee.aws.Store(
        'podimetrics-test',
        secrets.aws.access_key_id,
        secrets.aws.secret_access_key
    )
    store['a'] = b'a'
    assert store['a'] == b'a'
    store.prefix = 'test'
    store['a'] = b'a'
    store['b'] = b'b'
    assert list(store) == ['test/a', 'test/b']


if __name__ == '__main__':
    pytest.main()