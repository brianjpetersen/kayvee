import datetime
import kayvee
import secrets

start = datetime.datetime.now()
store = kayvee.aws.Store(
    'podimetrics-test',
    secrets.aws.access_key_id,
    secrets.aws.secret_access_key
)
store['a'] = 35000*b'a'
stop = datetime.datetime.now()

print((stop - start).total_seconds())