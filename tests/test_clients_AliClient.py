from tests.utils.utilities import create_start_patcher, stop_all_patchers

from lib.clients.AliClient import AliClient
from lib.clients.BaseClient import BaseClient
from lib.models.Snapshot import Snapshot
from lib.models.Volume import Volume
import unittest.mock
from unittest.mock import patch
from unittest.mock import Mock

import oss2
import os
import pytest

#Test data
valid_container = 'backup-container'
valid_disk_name = 'disk-id'
invalid_container = 'invalid-container'
availability_zone = 'eu-central-1a'
endpoint = 'endpoint-name'
secret = 'xyz'
access_key = 'key-id'
secret_access_key = 'secret-key'
region_id = 'xyz'
configuration = {
    'credhub_url' : None,
    'type' : 'online',
    'backup_guid' : 'backup-guid',
    'instance_id' : 'vm-id',
    'secret' : 'xyz',
    'job_name' : 'service-job-name',
    'container' : valid_container,
    'access_key_id' : access_key,
    'secret_access_key' : secret_access_key,
    'endpoint': endpoint,
    'region_name' : region_id
}
snapshot_id = 'snapshot-id'
snapshot_delete_id = 'snapshot-delete-id'
source_disk_size = '20'
snapshot_creation_time = '2019-04-04T08:12:53Z'
valid_vm_id = 'vm-id'
ephemeral_disk_device_id = '/dev/xvdb'
ephemeral_disk_id = 'ed1'
ephemeral_disk_size = '20'

persistent_disk_device_id = '/dev/xvdc'
persistent_disk_mount_device_id = '/dev/vdc'
persistent_disk_id = 'pd1'
persistent_disk_size = '20'

disk_delete_id = 'deletedisk1'
disk_detach_id = 'detachdisk1'

invalid_vm_id = 'invalid-vm-id'

directory_persistent = '/var/vcap/store'
directory_work_list = '/tmp'
log_dir = 'tests'
poll_delay_time = 10
poll_maximum_time = 60

operation_name = 'backup'

bucket = {
    'kind': 'storage#bucket',
    'id': valid_container,
    'selfLink': 'https://something.com/storage/v1/b/' + valid_container,
    'projectNumber': '11111',
    'name': valid_container,
    'timeCreated': '2017-12-24T10:23:50.348Z',
    'updated': '2017-12-24T10:23:50.348Z',
    'metageneration': '1',
    "location": 'EUROPE-CENTRAL1',
    'storageClass': 'REGIONAL',
    'etag': 'CAE='
}

def mock_shell(command):
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


class CR:
    def __init__(self):
        self.params = {}
        self.action = None
        self.domain = None
        self.version = None
    def set_domain(self, domain):
        self.domain = domain
    def set_version(self, version):
        self.version = version
    def set_action_name(self, action):
        self.action = action
    def add_query_param(self, key, value):
        self.params[key] = value

class ComputeClient:
        def do_action_with_exception(self, req):
            action = req.action
            params = req.params
            response = None
            if action == 'DescribeInstances':
                response = '{"Instances":{"Instance":[{"ZoneId":"'+availability_zone+'"}]}}'
                return response.encode('utf-8')
            elif action == 'CreateSnapshot':
                assert params['DiskId'] == valid_disk_name
                assert params['SnapshotName'] is not None
                assert params['Description'] == 'test-backup'

                response = '{"SnapshotId": "'+snapshot_id+'"}'
                return response.encode('utf-8')
            elif action == 'DescribeSnapshots':
                assert params['PageSize'] == 10
                assert params['RegionId'] == region_id
                assert snapshot_id in params['SnapshotIds'] or snapshot_delete_id in params['SnapshotIds'] 
                if(params['SnapshotIds'][0] == snapshot_delete_id):
                    response = '{"Snapshots":{"Snapshot":[]}}'
                else:
                    response = '{"Snapshots":{"Snapshot":[{"SnapshotId":"'\
                        +snapshot_id+'", "SourceDiskSize": '+source_disk_size+', "CreationTime": "'+snapshot_creation_time+'", "Status":"accomplished"}]}}'
                return response.encode('utf-8')
            elif action == 'DeleteSnapshot':
                assert params['SnapshotId'] in (snapshot_id, snapshot_delete_id)
                return
            elif action == 'DescribeDisks':
                assert params['PageSize'] == 10
                assert params['RegionId'] == region_id
                assert "InstanceId" in params or "DiskIds" in params
                if "InstanceId" in params:
                    assert params['InstanceId'] == valid_vm_id
                    response = '{"Disks":{"Disk":[{"DiskId":"'\
                            +ephemeral_disk_id+'", "Size": '+ephemeral_disk_size+', "Device": "'+ephemeral_disk_device_id+'", "Status":"In_use"}, {"DiskId":"'\
                            +persistent_disk_id+'", "Size": '+persistent_disk_size+', "Device": "'+persistent_disk_device_id+'", "Status":"In_use"}]}}'
                if "DiskIds" in params:
                    disk_id = params['DiskIds'][0]
                    status = 'Available' if disk_id == disk_detach_id else 'In_use'
                    assert disk_id in (ephemeral_disk_id, disk_delete_id, disk_detach_id)
                    if (disk_id == disk_delete_id):
                        response = '{"Disks":{"Disk":[]}}'
                    else:
                        response = '{"Disks":{"Disk":[{"DiskId":"'\
                            +disk_id+'", "Size": '+ephemeral_disk_size+', "Device": "'+ephemeral_disk_device_id+'", "Status":"'+status+'"}]}}'
                return response.encode('utf-8')
            elif action == 'CreateDisk':
                assert params['RegionId'] == region_id
                assert params['ZoneId'] == availability_zone
                assert 'DiskName' in params
                assert params['DiskCategory'] == 'cloud_ssd'
                assert params['Encrypted'] == True
                assert params['Size'] == 20
                response = '{"DiskId": "'+ephemeral_disk_id+'"}'
                return response.encode('utf-8')
            elif action == 'DeleteDisk':
                assert params['DiskId'] == disk_delete_id
                return
            elif action == 'AttachDisk':
                assert params['InstanceId'] == valid_vm_id
                assert params['DiskId'] == ephemeral_disk_id
                return
            elif action == 'DetachDisk':
                assert params['InstanceId'] == valid_vm_id
                assert params['DiskId'] == disk_detach_id
                return

            return response
class StorageClient:
    def Auth(access_key, secret_key):
        if access_key == 'key-id' and secret_key == 'secret-key':
            return 'storage-client'
        else:
            raise Exception('Invalid credentials')
    def Bucket(storage_client, endpoint, container):
        if container == valid_container:
            return bucket
        elif container == invalid_container:
            raise NotFound()
class Bucket:
    def __init__(self, name):
        self.name = name
    def put_object(self, key, value):
        if self.name == valid_container:
            return
    def delete_object(self, key):
        if self.name == valid_container:
            return

def mock_shell(command, log_command=True):
    if command == ('cat /proc/mounts | grep ' + directory_persistent):
        return persistent_disk_mount_device_id + '1 ' + directory_persistent + ' ext4 rw,relatime,data=ordered 0 0'

def get_device_of_volume(volume_id):
    if volume_id == persistent_disk_id:
        return persistent_disk_device_id
    else:
        return None


class TestAliClient:
    # Store all patchers
    patchers = []
    @classmethod
    def setup_class(self):
        # self.patchers.append(create_start_patcher(patch_function='__init__',patch_object=AliClient,side_effect=get_dummy_ali_session)['patcher'])
        # self.patchers.append(create_start_patcher(patch_function='get_container',patch_object=AliClient,side_effect=get_dummy_container)['patcher'])
        # self.patchers.append(create_start_patcher(patch_function='last_operation', patch_object=BaseClient)['patcher'])
        # self.patchers.append(create_start_patcher(patch_function='shell', patch_object=BaseClient, side_effect=mock_shell)['patcher'])
        self.patchers.append(create_start_patcher(
            patch_function='lib.clients.AliClient.AcsClient', return_value=ComputeClient()))
        self.patchers.append(create_start_patcher(
            patch_function='lib.clients.AliClient.oss2.Auth', return_value=StorageClient()))
        self.patchers.append(create_start_patcher(
            patch_function='lib.clients.AliClient.oss2.Bucket', return_value=Bucket(valid_container)))
        self.patchers.append(create_start_patcher(
            patch_function='lib.clients.AliClient.CommonRequest', return_value=CR()))
        self.patchers.append(create_start_patcher(
            patch_function='shell', patch_object=BaseClient, side_effect=mock_shell))
        self.patchers.append(create_start_patcher(
            patch_function='_get_device_of_volume', patch_object=BaseClient, side_effect=get_device_of_volume))
        os.environ['SF_BACKUP_RESTORE_LOG_DIRECTORY'] = log_dir
        os.environ['SF_BACKUP_RESTORE_LAST_OPERATION_DIRECTORY'] = log_dir
        self.aliClient = AliClient(operation_name, configuration, directory_persistent, directory_work_list,
                                   poll_delay_time, poll_maximum_time)

    @classmethod
    def teardown_class(self):
        patcher_names = []
        for patcher in self.patchers:
            patcher_names.append(patcher['patcher'])
        stop_all_patchers(patcher_names)

    def test_creates_ali_client_successfully(self):
        assert self.aliClient.compute_client is not None
        assert self.aliClient.storage_client is not None
        assert self.aliClient.container.name == valid_container
        assert self.aliClient.endpoint == endpoint
        assert self.aliClient.availability_zone == availability_zone
        self.patchers[0]['patcher_start'].assert_called_once()
        self.patchers[0]['patcher_start'].assert_called_with(access_key, secret_access_key, secret, auto_retry=True,
            max_retry_time=10, timeout=30)
        self.patchers[1]['patcher_start'].assert_called_once()
        self.patchers[1]['patcher_start'].assert_called_with(access_key, secret_access_key)
        self.patchers[2]['patcher_start'].assert_called_once()
        self.patchers[2]['patcher_start'].assert_called_with(self.aliClient.storage_client, self.aliClient.endpoint, self.aliClient.CONTAINER)
        self.patchers[3]['patcher_start'].assert_called_once()
        self.patchers[3]['patcher_start'].assert_called_with()
    
    def test_gets_container_successfully(self):
        assert self.aliClient.get_container() is  self.aliClient.container
        assert self.patchers[2]['patcher_start'].call_count == 2
    
    def test_creates_snapshot_successfully(self):
        snapshot = self.aliClient._create_snapshot(
            valid_disk_name, 'test-backup')
        assert snapshot.id == snapshot_id
        assert snapshot.status == 'accomplished'
        assert snapshot.start_time == snapshot_creation_time
        assert snapshot.size == 20
    
    def test_is_snapshot_ready_returns_true(self):
        is_ready = self.aliClient._is_snapshot_ready(
            snapshot_id)
        assert is_ready == True
    

    def test_deletes_snapshot_successfully(self):
        deleted = self.aliClient._delete_snapshot(
            snapshot_delete_id)
        assert deleted == True
    
    def test_gets_snapshot_successfully(self):
        expected_snapshot = Snapshot(snapshot_id, 20, snapshot_creation_time, 'accomplished')
        snapshot = self.aliClient.get_snapshot(
            snapshot_id)
        assert snapshot.id == expected_snapshot.id
        assert snapshot.status == expected_snapshot.status
        assert snapshot.start_time == expected_snapshot.start_time
        assert snapshot.size == expected_snapshot.size
    
    def test_gets_attached_volumes_for_instance(self):
        volume_list = self.aliClient.get_attached_volumes_for_instance(valid_vm_id)

        assert volume_list[0].id == ephemeral_disk_id
        assert volume_list[0].status == 'In_use'
        assert volume_list[0].size == int(ephemeral_disk_size)
        assert volume_list[0].device == ephemeral_disk_device_id
        assert volume_list[1].id == persistent_disk_id
        assert volume_list[1].status == 'In_use'
        assert volume_list[1].size == int(persistent_disk_size)
        assert volume_list[1].device == persistent_disk_device_id

    def test_gets_persistent_volume_for_instance(self):
        volume = self.aliClient.get_persistent_volume_for_instance(valid_vm_id)
        
        assert volume.id == persistent_disk_id
        assert volume.status == 'In_use'
        assert volume.size == int(persistent_disk_size)
        assert volume.device == persistent_disk_device_id
        self.patchers[4]['patcher_start'].call_count == 2
    
    def test_gets_volume_successfully(self):
        volume = self.aliClient.get_volume(ephemeral_disk_id)

        assert volume.id == ephemeral_disk_id
        assert volume.status == 'In_use'
        assert volume.size == int(ephemeral_disk_size)

    def test_creates_volume_successfully(self):
        volume = self.aliClient._create_volume(20)

        assert volume.id == ephemeral_disk_id
        assert volume.status == 'In_use'
        assert volume.size == int(ephemeral_disk_size)

    def test_deletes_volume_successfully(self):
        deleted = self.aliClient._delete_volume(disk_delete_id)

        assert deleted == True

    def test_create_attachment_successfully(self):
        attachment = self.aliClient._create_attachment(ephemeral_disk_id, valid_vm_id)

        assert attachment.volume_id == ephemeral_disk_id
        assert attachment.instance_id == valid_vm_id
    
    def test_deletes_attachment_successfully(self):
        deleted = self.aliClient._delete_attachment(disk_detach_id, valid_vm_id)

        assert deleted == True

    def test_gets_mounpoint_successfully(self):
        assert self.aliClient.get_mountpoint(persistent_disk_id) == persistent_disk_mount_device_id