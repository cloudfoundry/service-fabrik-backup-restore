import os
import pytest
from unittest.mock import patch
import boto3
from botocore.config import Config
from lib.clients.AwsClient import AwsClient
from lib.clients.BaseClient import BaseClient

#Test data
valid_container = 'backup-container'
invalid_container = 'invalid-container'
configuration = {
    'credhub_url' : None,
    'type' : 'online',
    'backup_guid' : 'backup-guid',
    'instance_id' : 'vm-id',
    'secret' : 'xyz',
    'job_name' : 'service-job-name',
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
availability_zone = 'abc'

# Defintions of dummy clients and resources.
# More details to be added as new tests will be added 
class Ec2ConfigDummy:
    def __init__(self):
        pass

class Ec2Dummy:
    class Instance:
        def __init__(self,instance_id):
            self.instance_id = instance_id
            self.placement = {}
            self.placement['AvailabilityZone'] = 'abc'

        def load(self):
            pass

class EC2ClientDummy:
    def __init__(self):
        pass

class S3Dummy:
    class Bucket:
        def __init__(self, name):
            if name ==valid_container:
                self.name = name
                return
            else:
                raise Exception('Container not found')

        def put_object(self,Key):
            pass

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

def create_start_patcher(patch_function, patch_object=None, return_value=None, side_effect=None):
    if patch_object != None:
        patcher = patch.object(patch_object, patch_function)
    else:
        patcher = patch(patch_function)

    patcher_start = patcher.start()
    if return_value != None:
        patcher_start.return_value = return_value
    
    if side_effect != None:
        patcher_start.side_effect = side_effect
    
    return patcher

def stop_all_patchers(patchers):
    for patcher in patchers:
        patcher.stop()

#Tests
class TestAwsClient:
    patchers = []
    @classmethod
    def setup_class(self):
        self.patchers.append(create_start_patcher(patch_function='create_aws_session',patch_object=AwsClient,side_effect=get_dummy_aws_session))
        self.patchers.append(create_start_patcher(patch_function='last_operation', patch_object=BaseClient))

        os.environ['SF_BACKUP_RESTORE_LOG_DIRECTORY'] = log_dir
        os.environ['SF_BACKUP_RESTORE_LAST_OPERATION_DIRECTORY'] = log_dir

        self.testAwsClient = AwsClient(operation_name, configuration, directory_persistent, directory_work_list,poll_delay_time, poll_maximum_time)

    @classmethod
    def teardown_class(self):
        stop_all_patchers(self.patchers)

    def test_create_aws_client(self):
        assert isinstance(self.testAwsClient.ec2, Ec2Dummy)
        assert isinstance(self.testAwsClient.s3, S3Dummy)
        assert isinstance(self.testAwsClient.ec2.client, EC2ClientDummy)
        assert isinstance(self.testAwsClient.s3.client, S3ClientDummy)
        assert self.testAwsClient.availability_zone == availability_zone

    def test_get_container_exception(self):
        with pytest.raises(Exception):
            container = self.testAwsClient.s3.Bucket(invalid_container)
            assert container is None

