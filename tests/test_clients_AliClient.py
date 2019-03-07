from tests.utils.utilities import create_start_patcher, stop_all_patchers

from lib.clients.AliClient import AliClient
from lib.clients.BaseClient import BaseClient

import oss2
import os
import pytest

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
    'endpoint': 'endpoint-name',
    'region_name' : 'xyz'
}

directory_persistent = '/var/vcap/store'
directory_work_list = '/tmp'
log_dir = 'tests'
poll_delay_time = 10
poll_maximum_time = 60

operation_name = 'backup'

def mock_shell(command):
    print(command)
    if command == ('cat /proc/mounts | grep '+ directory_persistent):
        return valid_volume_device

class OssClientDummy:
    def __init__(self):
        pass

class OssDummy:
    class Bucket:
        def __init__(self, name):
            self.name = name

        def put_object(self,Key):
            if self.name == valid_container:
                return
            else:
                auth = oss2.Auth(configuration['access_key_id'], configuration['secret_access_key'])
                client = oss2.Bucket(auth, configuration['endpoint'], self.name)
                response = json.load(open('tests/data/aws/bucket.put.nosuchbucket.json'))
                exception = client.exceptions.NoSuchBucket(error_response=response,operation_name='PutObject')
                raise exception

class AliSessionDummy:
    def __init__(self):
        pass

    def resource(self, type, config=None):
        if type == 'oss':
            return OssDummy()

    def client(self, type, config=None):
        if type == 's3':
            return OssClientDummy()

def get_dummy_ali_session():
    return AliSessionDummy()

def get_dummy_container(auth, endpoint):
    return OssDummy.Bucket(configuration['container'])

class TestAwsClient:
    patchers = []
    @classmethod
    def setup_class(self):
        #self.patchers.append(create_start_patcher(patch_function='__init__',patch_object=AliClient,side_effect=get_dummy_ali_session)['patcher'])
        self.patchers.append(create_start_patcher(patch_function='get_container',patch_object=AliClient,side_effect=get_dummy_container)['patcher'])
        self.patchers.append(create_start_patcher(patch_function='last_operation', patch_object=BaseClient)['patcher'])
        self.patchers.append(create_start_patcher(patch_function='shell', patch_object=BaseClient, side_effect=mock_shell)['patcher'])
        os.environ['SF_BACKUP_RESTORE_LOG_DIRECTORY'] = log_dir
        os.environ['SF_BACKUP_RESTORE_LAST_OPERATION_DIRECTORY'] = log_dir

        self.testAliClient = AliClient(operation_name, configuration, directory_persistent, directory_work_list,poll_delay_time, poll_maximum_time)

    @classmethod
    def teardown_class(self):
        stop_all_patchers(self.patchers)

    def test_create_aws_client(self):
       assert isinstance(self.testAliClient.container, OssDummy.Bucket)

    def test_get_container_exception(self):
       with pytest.raises(Exception):
            container = self.testAliClient.OssDummy.Bucket(invalid_container)
            assert container is None
