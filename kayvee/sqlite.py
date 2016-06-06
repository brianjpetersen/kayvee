# standard libraries
import os
import posixpath
# third party libraries
import apsw
# first party libraries
pass


__where__ = os.path.dirname(os.path.abspath(__file__))


class Store:
    
    def __init__(self, path, prefix='', sync='normal', timeout=1):
        self.path = path
        self.prefix = prefix
        self.sync = sync
        # create database and setup table
        self.connection = apsw.Connection(self.path)
        self.cursor = self.connection.cursor()
        self.timeout = timeout
        if sync.lower() == 'normal':
            query = 'PRAGMA synchronous = NORMAL'
        elif sync.lower() == 'off':
            query = 'PRAGMA synchronous = OFF'
        elif sync.lower() == 'full':
            query = 'PRAGMA synchronous = FULL'
        elif sync.lower() == 'extra':
            query = 'PRAGMA synchronous = EXTRA'
        else:
            raise ValueError
        self.cursor.execute(query)
        query = 'PRAGMA journal_mode=WAL'
        self.cursor.execute(query)
        query = '''CREATE TABLE IF NOT EXISTS kayvee 
                   (key TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL)'''
        self.cursor.execute(query)
    
    def get_prefix(self):
        return self._prefix
        
    def set_prefix(self, prefix):
        if prefix is None:
            prefix = ''
        self._prefix = prefix
        
    prefix = property(get_prefix, set_prefix)
    
    def get_timeout(self):
        return self._timeout
    
    def set_timeout(self, timeout):
        self._timeout = timeout
        self.connection.setbusytimeout(1000*timeout)
    
    timeout = property(get_timeout, set_timeout)
    
    def __getitem__(self, key):
        key = posixpath.join(self.prefix, key)
        query = 'SELECT value FROM kayvee WHERE key = :key'
        try:
            (value, ), = self.cursor.execute(query, {'key': key})
            return value
        except:
            raise KeyError from None
    
    def __setitem__(self, key, value):
        key = posixpath.join(self.prefix, key)
        query = 'INSERT OR REPLACE INTO kayvee VALUES (:key, :value)'
        self.cursor.execute(query, {'key': key, 'value': value})
    
    def __contains__(self, key):
        key = posixpath.join(self.prefix, key)
        query = 'SELECT EXISTS (SELECT 1 FROM kayvee WHERE key=:key LIMIT 1)'
        (contains, ), = self.cursor.execute(query, {'key': key})
        return bool(contains)
    
    def after(self, key, inclusive=False, ascending=True):
        key = posixpath.join(self.prefix, key)
        comparison = '>=' if inclusive else '>'
        query = '''SELECT key, value FROM kayvee WHERE 
                   key {comparison} :key'''.format(comparison=comparison)
        if self.prefix != '':
            query += " AND key LIKE '{}/%'".format(self.prefix)
        if ascending == False:
            query += ' ORDER BY key DESC'
        for (key, value, ) in self.cursor.execute(query, {'key': key}):
            yield key, value
    
    def before(self, key, inclusive=True, ascending=True):
        key = posixpath.join(self.prefix, key)
        comparison = '<=' if inclusive else '<'
        query = '''SELECT key, value FROM kayvee WHERE 
                   key {comparison} :key'''.format(comparison=comparison)
        if self.prefix != '':
            query += " AND key LIKE '{}/%'".format(self.prefix)
        if ascending == False:
            query += ' ORDER BY key DESC'
        for (key, value, ) in self.cursor.execute(query, {'key': key}):
            yield key, value
        
    def between(self, after_key, before_key, inclusive_after=True, 
                inclusive_before=True, ascending=True):
        after_key = posixpath.join(self.prefix, after_key)
        before_key = posixpath.join(self.prefix, before_key)
        after_comparison = '>=' if inclusive_after else '>'
        before_comparison = '<=' if inclusive_before else '<'
        query = '''SELECT key, value FROM kayvee WHERE 
                   key {after_comparison} :after_key AND
                   key {before_comparison} :before_key'''
        query = query.format(
            after_comparison=after_comparison,
            before_comparison=before_comparison,
        )
        if self.prefix != '':
            query += " AND key LIKE '{}/%'".format(self.prefix)
        if ascending == False:
            query += ' ORDER BY key DESC'
        substitutions = {
            'after_key': after_key,
            'before_key': before_key,
        }
        for (key, value, ) in self.cursor.execute(query, substitutions):
            yield key, value
    
    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default
        
    def update(self, other):
        for key, value in other.items():
            self[key] = value
    
    def popitem(self):
        with self.connection:
            if len(self) == 0:
                raise KeyError
            key, value = self.items().next()
            del self[key]
        return (key, value)
    
    def setdefault(key, default=None):
        try:
            return self[key]
        except KeyError:
            return default
    
    def pop(self, key, default=None):
        with self.connection:
            try:
                value = self[key]
                del self[key]
                return value
            except:
                return default
    
    def clear(self):
        query = 'DELETE FROM kayvee'
        if self.prefix != '':
            query += " WHERE key LIKE '{}/%'".format(self.prefix)
        self.cursor.execute(query)
    
    def __delitem__(self, key):
        key = posixpath.join(self.prefix, key)
        with self.connection:
            try:
                # first ensure key exists
                if key not in self:
                    raise KeyError
                # if we got to here, key exists; delete
                query = 'DELETE FROM kayvee WHERE key = :key'
                self.cursor.execute(query, {'key': key})
            except:
                raise KeyError from None
    
    def __iter__(self):
        query = 'SELECT key FROM kayvee'
        if self.prefix != '':
            query += " WHERE key LIKE '{}/%'".format(self.prefix)
        for (key, ) in self.cursor.execute(query):
            yield key
    
    def keys(self):
        for key in self:
            yield key
    
    def values(self):
        query = 'SELECT value FROM kayvee'
        if self.prefix != '':
            query += " WHERE key LIKE '{}/%'".format(self.prefix)
        for (value, ) in self.cursor.execute(query):
            yield value
    
    def items(self):
        query = 'SELECT key, value FROM kayvee'
        if self.prefix != '':
            query += " WHERE key LIKE '{}/%'".format(self.prefix)
        for (key, value, ) in self.cursor.execute(query):
            yield key, value
    
    def __len__(self):
        query = 'SELECT count(1) FROM kayvee'
        if self.prefix != '':
            query += " WHERE key LIKE '{}/%'".format(self.prefix)
        (length, ), = self.cursor.execute(query)
        return length
    
    def __enter__(self):
        return self.connection.__enter__()
    
    def __exit__(self, *args, **kwargs):
        return self.connection.__exit__(*args, **kwargs)
    
    def __repr__(self):
        return '{}.{}(path={}, prefix={}, sync={}, timeout={})'.format(
            self.__module__,
            self.__class__.__name__,
            repr(self.path),
            repr(self.prefix),
            repr(self.sync),
            repr(self.timeout),
        )
