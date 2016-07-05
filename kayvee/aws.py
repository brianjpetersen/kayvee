# standard libraries
import os
import posixpath
import collections
# third party libraries
import boto3
import botocore
# first party libraries
pass


__where__ = os.path.dirname(os.path.abspath(__file__))


class Store(collections.abc.MutableMapping):
    
    def __init__(self, bucket, access_key_id, secret_access_key, 
                 prefix='', region='us-east-1'):
        self.prefix = prefix
        connection = self.connect(bucket, access_key_id, secret_access_key, region)
        self.bucket = connection.Bucket(bucket)
    
    def get_prefix(self):
        return self._prefix
    
    def set_prefix(self, prefix):
        if prefix.endswith('/') == False:
            prefix = prefix + '/'
        self._prefix = prefix
    
    prefix = property(get_prefix, set_prefix)
    
    @staticmethod
    def connect(bucket, access_key_id, secret_access_key, region):
        session = boto3.session.Session(
            aws_access_key_id=access_key_id, 
            aws_secret_access_key=secret_access_key,
            region_name=region,
        )
        resource = connection = session.resource('s3')
        return resource
    
    @classmethod
    def create(cls, bucket, access_key_id, secret_access_key, 
               prefix='/', region='us-east-1'):
        connection = cls.connect(access_key_id, secret_access_key, region)
        connection.create_bucket(Bucket=bucket, ACL='private')
        return cls(bucket, access_key_id, secret_access_key, prefix, region)
    
    def get_response(self, key):
        path = posixpath.join(self.prefix, key.lstrip('/'))
        try:
            response = self.bucket.Object(path).get()
        except botocore.exceptions.ClientError as exception:
            message, = exception.args
            if 'NoSuchKey' in message:
                raise KeyError
            else:
                raise
        return response
    
    def set(self, key, value):
        path = posixpath.join(self.prefix, key.lstrip('/'))
        response = self.bucket.Object(path).put(
            Body=value, 
            ServerSideEncryption='AES256', 
            ACL='private', 
            StorageClass='STANDARD',
        )
        return response
    
    def delete(self, key):
        path = posixpath.join(self.prefix, key.lstrip('/'))
        try:
            response = bucket.delete_object(path)
        except botocore.exceptions.ClientError as exception:
            message, = exception.args
            if 'NoSuchKey' in message:
                raise KeyError
            else:
                raise
        return response
    
    def __repr__(self):
        return '{}.{}({}, prefix={})'.format(
            self.__module__,
            self.__class__.__name__,
            repr(self.name),
            repr(self.prefix)
        )
    
    def __len__(self):
        return sum(1 for key in self)
    
    def __iter__(self):
        for response in self.bucket.objects.filter(Prefix=self.prefix):
            yield response.key.lstrip(self.prefix)
    
    def __getitem__(self, key):
        response = self.get_response(key)
        item = response['Body'].read()
        return item
    
    def __setitem__(self, key, value):
        self.set(key, value)
    
    def __delitem__(self, key):
        self.delete(key)
