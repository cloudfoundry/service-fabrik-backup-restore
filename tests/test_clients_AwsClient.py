from tests.utils.utilities import create_start_patcher, stop_all_patchers
import tests.utils.setup_constants
import os
import pytest
from unittest.mock import patch
import boto3
from botocore.config import Config
import botocore
from lib.clients.AwsClient import AwsClient
from lib.clients.BaseClient import BaseClient
from lib.models.Snapshot import Snapshot
from lib.models.Volume import Volume
from pprint import pprint
import json

#Test data
valid_container = 'backup-container'
invalid_container = 'invalid-container'
valid_snapshot = 'valid-snapshot'
valid_snapshot_size = 40
valid_snapshot_state = 'READY'
invalid_snapshot = 'invalid-snapshot'
valid_volume = 'valid-volume'
invalid_volume = 'invalid-volume'
valid_volume_size = 40
valid_volume_state = 'READY'
valid_volume_device = '/dev/xvda'

configuration = {
    'credhub_url' : None,
    'type' : 'online',
    'backup_guid' : 'backup-guid',
    'instance_id' : 'vm-id',
    'secret' : 'xyz',
    'job_name' : 'service-job-name',
    'trigger' : 'on-demand-or-scheduled',
    'container' : valid_container,
    'access_key_id' : 'key-id',
    'secret_access_key' : 'secret-key',
    'region_name' : 'xyz'
}

configuration_blob_ops = {
    'credhub_url' : None,
    'type' : 'online',
    'container' : valid_container,
    'access_key_id' : 'key-id',
    'secret_access_key' : 'secret-key',
    'region_name' : 'xyz'
}

directory_persistent = '/var/vcap/store'
directory_work_list = '/tmp'
log_dir = 'tests'
poll_delay_time = 10
poll_maximum_time = 60
operation_name = 'backup'
operation_name_blob_ops = 'blob_operation'
availability_zone = 'abc'

#helper functions
def loadJsonToObject(path, object):
    response = json.load(open(path))
    for key in response:
        setattr(object, key, response[key])
    
def loadJsonToVolumeObject(path, volume_id, object):
    response = json.load(open(path))[volume_id]
    for key in response:
        setattr(object, key, response[key])

def mock_shell(command):
    print(command)
    if command == ('cat /proc/mounts | grep '+ directory_persistent):
        return valid_volume_device

# Defintions of dummy clients and resources.
# More details to be added as new tests will be added 
class Ec2ConfigDummy:
    def __init__(self):
        pass

class Ec2Dummy:
    class Snapshot:
        def __init__(self,snapshot_id):
            if snapshot_id != valid_snapshot:
                raise Exception('No such snapshot')

            loadJsonToObject('tests/data/aws/snapshot.init.json', self)

    class Volume:
        def __init__(self,volume_id):
            if volume_id == invalid_volume:
                raise Exception('No such volume')
            else:
                loadJsonToVolumeObject('tests/data/aws/volumes.init.json', volume_id, self)

    class Instance:
        def __init__(self,instance_id):
            loadJsonToObject('tests/data/aws/instance.init.json', self)
            self.volumes = CollectionsDummy([Ec2Dummy.Volume(v['id']) for v in self.volumes_list])

        def load(self):
            pass

class EC2ClientDummy:
    def __init__(self):
        pass

class S3Dummy:
    class Bucket:
        def __init__(self, name):
            self.name = name

        def put_object(self,Key):
            if self.name == valid_container:
                return
            else:
                client = boto3.client('s3')
                response = json.load(open('tests/data/aws/bucket.put.nosuchbucket.json'))
                exception = client.exceptions.NoSuchBucket(error_response=response,operation_name='PutObject')
                raise exception

        def delete_objects(self,Delete):
            pass


class S3ClientDummy:
    def __init__(self):
        pass

class AwsSessionDummy:
    def __init__(self):
        pass

    def resource(self, type, config=None):
        if type == 'ec2':
            return Ec2Dummy()
        elif type == 's3':
            return S3Dummy()

    def client(self, type, config=None):
        if type == 'ec2':
            return EC2ClientDummy()
        elif type == 's3':
            return S3ClientDummy()

def get_dummy_aws_session():
    return AwsSessionDummy()

# EC2 instance objects have 'collection' of volumes. Collcetion is boto specific data structure
# and exposes it's own methods (https://boto3.readthedocs.io/en/latest/guide/collections.html#guide-collections).
# Hence we need to have it's mocked version also.
class CollectionsDummy:
    def __init__(self, object_list):
        self.objects = list(object_list)

    def all(self):
        return self.objects

#Tests
class TestAwsClient:
    patchers = []
    @classmethod
    def setup_class(self):
        self.patchers.append(create_start_patcher(patch_function='create_aws_session',patch_object=AwsClient,side_effect=get_dummy_aws_session)['patcher'])
        self.patchers.append(create_start_patcher(patch_function='last_operation', patch_object=BaseClient)['patcher'])
        self.patchers.append(create_start_patcher(patch_function='shell', patch_object=BaseClient, side_effect=mock_shell)['patcher'])
        os.environ['SF_BACKUP_RESTORE_LOG_DIRECTORY'] = log_dir
        os.environ['SF_BACKUP_RESTORE_LAST_OPERATION_DIRECTORY'] = log_dir

        self.testAwsClient = AwsClient(operation_name, configuration, directory_persistent, directory_work_list,poll_delay_time, poll_maximum_time)
        self.testAwsClientBlobOps = AwsClient(operation_name_blob_ops, configuration_blob_ops, directory_persistent, directory_work_list,poll_delay_time, poll_maximum_time)

    @classmethod
    def teardown_class(self):
        stop_all_patchers(self.patchers)

    def test_create_aws_client(self):
        assert isinstance(self.testAwsClient.ec2, Ec2Dummy)
        assert isinstance(self.testAwsClient.s3, S3Dummy)
        assert hasattr(self.testAwsClient, 'ec2')
        assert hasattr(self.testAwsClient, 'availability_zone')
        assert isinstance(self.testAwsClient.ec2.client, EC2ClientDummy)
        assert isinstance(self.testAwsClient.s3.client, S3ClientDummy)
        assert self.testAwsClient.availability_zone == availability_zone

    def test_create_aws_client_blob_ops(self):
        assert isinstance(self.testAwsClientBlobOps.s3, S3Dummy)
        assert isinstance(self.testAwsClientBlobOps.s3.client, S3ClientDummy)
        assert not hasattr(self.testAwsClientBlobOps, 'ec2')
        assert not hasattr(self.testAwsClientBlobOps, 'availability_zone')

    def test_get_container_exception(self):
        with pytest.raises(Exception):
            container = self.testAwsClient.s3.Bucket(invalid_container)
            assert container is None
