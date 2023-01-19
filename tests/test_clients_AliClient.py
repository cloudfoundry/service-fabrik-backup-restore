from tests.utils.utilities import create_start_patcher, stop_all_patchers

from lib.clients.AliClient import AliClient
from aliyunsdkcore.acs_exception.exceptions import ServerException
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
invalid_disk_name = 'invalid-disk-id'
invalid_exc_disk = 'invalid_exc_disk'
invalid_duplicate_disk = 'invalid_duplicate_disk'
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
    'trigger' : 'on-demand-or-scheduled',
    'container' : valid_container,
    'access_key_id' : access_key,
    'secret_access_key' : secret_access_key,
    'endpoint': endpoint,
    'region_name' : region_id
}
valid_blob_path = '/tmp/valid-blob.txt'
invalid_blob_path = '/tmp/invalid-blob.txt'


snapshot_id = 'snapshot-id'
snapshot_404_id = 'snapshot_404_id'
snapshot_exc_id = 'snapshot_exc_id'
snapshot_delete_id = 'snapshot-delete-id'
snapshot_failed_id = 'snapshot_failed_id'
snapshot_duplicate_id = 'snapshot_duplicate_id'
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
disk_duplicate_id = 'duplicatedisk1'
disk_create_duplicate_id = 'disk_create_duplicate_id'
disk_exc_id = 'disk_exc_id'
disk_404_id = 'disk_404_id'
disk_attach_error_id = 'disk_attach_error_id'

invalid_vm_id = 'invalid-vm-id'
notfound_vm_id = 'notfound-vm-id'

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
        # reset params on each _get_common_compute_request call
        self.params = {}
    def set_version(self, version):
        self.version = version
    def set_action_name(self, action):
        self.action = action
    def add_query_param(self, key, value):
        self.params[key] = value

class ComputeClient:
    def __init__(self):
        self.describe_failed_delete_snapshot_call_count = 0
        self.describe_duplicate_delete_snapshot_call_count = 0
        self.create_duplicate_disk_call_count = 0
    def do_action_with_exception(self, req):
        action = req.action
        params = req.params
        response = None
        if action == 'DescribeInstances':
            response = '{"Instances":{"Instance":[{"ZoneId":"'+availability_zone+'"}]}}'
            return response.encode('utf-8')
        elif action == 'CreateSnapshot':
            assert params['DiskId'] in (valid_disk_name, invalid_disk_name, invalid_exc_disk, invalid_duplicate_disk)
            assert params['SnapshotName'] is not None
            assert params['Description'] == 'test-backup'
            if params['DiskId'] == invalid_exc_disk:
                raise Exception('Failed to create snapshot')
            snapshot_return_id = snapshot_id if params['DiskId'] == valid_disk_name else snapshot_duplicate_id if params['DiskId'] == invalid_duplicate_disk else snapshot_failed_id
            response = '{"SnapshotId": "'+snapshot_return_id+'"}'
            return response.encode('utf-8')
        elif action == 'DescribeSnapshots':
            assert params['PageSize'] == 10
            assert params['RegionId'] == region_id
            assert len(params['SnapshotIds']) == 1
            assert params['SnapshotIds'][0] in (snapshot_id, snapshot_delete_id, snapshot_failed_id, snapshot_duplicate_id)
            if params['SnapshotIds'][0] == snapshot_delete_id:
                response = '{"Snapshots":{"Snapshot":[]}}'
            elif params['SnapshotIds'][0] == snapshot_failed_id:
                # to handle failed snapshot and then delete case
                response = '{"Snapshots":{"Snapshot":[{"SnapshotId":"'\
                    +snapshot_failed_id+'", "SourceDiskSize": '+source_disk_size+', "CreationTime": "'+snapshot_creation_time+'", "Status":"failed"}]}}'
                response = response if self.describe_failed_delete_snapshot_call_count <=1 else '{"Snapshots":{"Snapshot":[]}}'
                self.describe_failed_delete_snapshot_call_count += 1
            elif params['SnapshotIds'][0] == snapshot_duplicate_id:
                response = '{"Snapshots":{"Snapshot":[{}, {}]}}'
                response = response if self.describe_duplicate_delete_snapshot_call_count <=1 else '{"Snapshots":{"Snapshot":[]}}'
                self.describe_duplicate_delete_snapshot_call_count += 1
            else:
                response = '{"Snapshots":{"Snapshot":[{"SnapshotId":"'\
                    +snapshot_id+'", "SourceDiskSize": '+source_disk_size+', "CreationTime": "'+snapshot_creation_time+'", "Status":"accomplished"}]}}'
            return response.encode('utf-8')
        elif action == 'DeleteSnapshot':
            assert params['SnapshotId'] in (snapshot_id, snapshot_delete_id, snapshot_failed_id, snapshot_duplicate_id, snapshot_exc_id, snapshot_404_id)
            if params['SnapshotId'] == snapshot_exc_id:
                raise ServerException('SDK.InvalidRequest','Failed to delete snapshot', 500)
            elif params['SnapshotId'] == snapshot_404_id:
                raise ServerException('SDK.InvalidRequest','Failed to delete snapshot', 404)
            return
        elif action == 'DescribeDisks':
            assert params['PageSize'] == 10
            assert params['RegionId'] == region_id
            assert "InstanceId" in params or "DiskIds" in params
            if "InstanceId" in params:
                assert params['InstanceId'] in (valid_vm_id, invalid_vm_id)
                response = '{"Disks":{"Disk":[{"DiskId":"'\
                        +ephemeral_disk_id+'", "Size": '+ephemeral_disk_size+', "Device": "'+ephemeral_disk_device_id+'", "Status":"In_use"}, {"DiskId":"'\
                        +persistent_disk_id+'", "Size": '+persistent_disk_size+', "Device": "'+persistent_disk_device_id+'", "Status":"In_use"}]}}'
                if params['InstanceId'] == invalid_vm_id:
                    raise Exception('Failed to get disk information')
            elif "DiskIds" in params:
                disk_id = params['DiskIds'][0]
                status = 'Available' if disk_id == disk_detach_id else 'In_use'
                assert disk_id in (ephemeral_disk_id, disk_delete_id, disk_detach_id, disk_duplicate_id, disk_exc_id, disk_create_duplicate_id, disk_attach_error_id)
                if disk_id == disk_delete_id:
                    response = '{"Disks":{"Disk":[]}}'
                elif disk_id == disk_attach_error_id:
                    response = '{"Disks":{"Disk":[{"DiskId":"'\
                        +disk_id+'", "Size": '+ephemeral_disk_size+', "Device": "", "Status":"In_use"}]}}'
                elif disk_id == disk_exc_id:
                    raise Exception('Failed to get disk information')
                elif disk_id == disk_duplicate_id:
                    response = '{"Disks":{"Disk":[{}, {}]}}'
                elif disk_id == disk_create_duplicate_id:
                    response = '{"Disks":{"Disk":[{}, {}]}}'
                    response = response if self.create_duplicate_disk_call_count <=1 else '{"Disks":{"Disk":[]}}'
                    self.create_duplicate_disk_call_count += 1
                else:
                    response = '{"Disks":{"Disk":[{"DiskId":"'\
                        +disk_id+'", "Size": '+ephemeral_disk_size+', "Device": "'+ephemeral_disk_device_id+'", "Status":"'+status+'"}]}}'
            return response.encode('utf-8')
        elif action == 'CreateDisk':
            disk_category = params['DiskCategory']
            assert params['RegionId'] == region_id
            assert params['ZoneId'] == availability_zone
            assert 'DiskName' in params
            assert disk_category in ('cloud_ssd', 'duplicate')
            assert params['Encrypted'] == True
            assert params['Size'] == 20
            response = '{"DiskId": "'+ephemeral_disk_id+'"}'
            if disk_category == 'duplicate':
                response = '{"DiskId": "'+disk_create_duplicate_id+'"}'
            return response.encode('utf-8')
        elif action == 'DeleteDisk':
            assert params['DiskId'] in (disk_delete_id, disk_create_duplicate_id, disk_exc_id, disk_404_id)
            if params['DiskId'] == disk_exc_id:
                raise ServerException('SDK.InvalidRequest','Failed to delete disk', 500)
            elif params['DiskId'] == disk_404_id:
                raise ServerException('SDK.InvalidRequest','Failed to delete disk', 404)
            return
        elif action == 'AttachDisk':
            assert params['InstanceId'] in (valid_vm_id, invalid_vm_id)
            assert params['DiskId'] in (ephemeral_disk_id, disk_attach_error_id)
            if params['InstanceId'] == invalid_vm_id:
                raise Exception('Failed to create attachment')
            return
        elif action == 'DetachDisk':
            assert params['InstanceId'] in (valid_vm_id, invalid_vm_id, notfound_vm_id)
            assert params['DiskId'] in (disk_detach_id,disk_attach_error_id)
            if params['InstanceId'] == invalid_vm_id:
                raise ServerException('SDK.InvalidRequest','Failed to delete attachment', 500)
            elif params['InstanceId'] == notfound_vm_id:
                raise ServerException('SDK.InvalidRequest','Failed to delete attachment', 404)
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
    def read(self):
        return
    def put_object_from_file(self, blob_target, blob_upload_path, headers):
        assert blob_upload_path in (valid_blob_path, invalid_blob_path)
        assert blob_target == 'blob'
        if (blob_upload_path == invalid_blob_path):
            raise Exception('Invalid blob upload path')
    def get_object(self, blob_target, blob_target_path):
        assert blob_target_path in (valid_blob_path, invalid_blob_path)
        assert blob_target == 'blob'
        if (blob_target_path == invalid_blob_path):
            raise Exception('Invalid blob target path')
        return self
    # added this method because of issue in travice
    # in travis python interpretor was replacing get_object() with get_object_to_file
    def get_object_to_file(self, blob_target, blob_target_path):
        assert blob_target_path in (valid_blob_path, invalid_blob_path)
        assert blob_target == 'blob'
        if (blob_target_path == invalid_blob_path):
            raise Exception('Invalid blob target path')
        return self
    def put_object(self, key, value):
        if self.name == valid_container:
            return
    def delete_object(self, key):
        if self.name == valid_container:
            return

class RequestHeader:
    def set_server_side_encryption(self, enc_type):
        assert enc_type == "AES256"
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
        self.patchers.append(create_start_patcher(
            patch_function='lib.clients.AliClient.RequestHeader', return_value=RequestHeader()))
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
    
    # @pytest.fixture(autouse=True)
    # def setup_method(self):
    #     self.aliClient.compute_client.describe_failed_delete_snapshot_call_count = 0
    #     self.aliClient.compute_client.describe_duplicate_delete_snapshot_call_count = 0
    #     self.aliClient.compute_client.create_duplicate_disk_call_count = 0
    #     self.patchers[3]['patcher_start'].stop()
    #     self.patchers[3] = create_start_patcher(
    #         patch_function='lib.clients.AliClient.CommonRequest', return_value=CR())



    def test_ali_creates_client_successfully(self):
        assert self.aliClient.compute_client is not None
        assert self.aliClient.storage_client is not None
        assert self.aliClient.container.name == valid_container
        assert self.aliClient.endpoint == endpoint
        assert self.aliClient.availability_zone == availability_zone
        self.patchers[0]['patcher_start'].call_count == 1
        self.patchers[0]['patcher_start'].assert_called_with(access_key, secret_access_key, secret, auto_retry=True,
            max_retry_time=10, timeout=30)
        self.patchers[1]['patcher_start'].call_count == 1
        self.patchers[1]['patcher_start'].assert_called_with(access_key, secret_access_key)
        self.patchers[2]['patcher_start'].call_count == 1
        self.patchers[2]['patcher_start'].assert_called_with(self.aliClient.storage_client, self.aliClient.endpoint, self.aliClient.CONTAINER)
    
    def test_ali_uploads_to_blobstore(self):
        assert self.aliClient._upload_to_blobstore(valid_blob_path, 'blob') == True
        assert self.patchers[6]['patcher_start'].call_count == 1
    
    def test_ali_upload_to_blobstore_raises_exception(self):
        try:
            self.aliClient._upload_to_blobstore(invalid_blob_path, 'blob')
        except Exception as error:
            assert "Invalid blob upload path" in str(error)
    
    def test_ali_downloads_from_blobstore(self):
        assert self.aliClient._download_from_blobstore('blob', valid_blob_path) == True

    def test_ali_download_to_blobstore_raises_exception(self):
        try:
            self.aliClient._download_from_blobstore('blob', invalid_blob_path)
        except Exception as error:
            assert "Invalid blob target path" in str(error)

    def test_ali_gets_container_successfully(self):
        assert self.aliClient.get_container() is  self.aliClient.container
        assert self.patchers[2]['patcher_start'].call_count == 2
    
    def test_ali_creates_snapshot_successfully(self):
        snapshot = self.aliClient._create_snapshot(
            valid_disk_name, 'test-backup')
        assert snapshot.id == snapshot_id
        assert snapshot.status == 'accomplished'
        assert snapshot.start_time == snapshot_creation_time
        assert snapshot.size == 20
    
    def test_ali_create_snapshot_fails_with_status_failed(self):
        try:
            self.aliClient._create_snapshot(invalid_disk_name, 'test-backup')
        except Exception as error:
            assert "status=failed" in str(error)
    def test_ali_create_snapshot_throws_exception(self):
        try:
            self.aliClient._create_snapshot(invalid_exc_disk, 'test-backup')
        except Exception as error:
            assert "Failed to create snapshot" in str(error)
    
    def test_ali_create_snapshot_throws_exception_for_duplicate(self):
        try:
            self.aliClient._create_snapshot(invalid_duplicate_disk, 'test-backup')
        except Exception as error:
            assert "More than 1 snapshot found with id snapshot_duplicate_id" in str(error)
    def test_ali_is_snapshot_ready_returns_true(self):
        is_ready = self.aliClient._is_snapshot_ready(
            snapshot_id)
        assert is_ready == True
    def test_ali_is_snapshot_ready_returns_false_on_error(self):
        is_ready = self.aliClient._is_snapshot_ready(
            snapshot_delete_id)
        assert is_ready == False

    def test_ali_deletes_snapshot_successfully(self):
        deleted = self.aliClient._delete_snapshot(
            snapshot_delete_id)
        assert deleted == True
    
    def test_ali_delete_snapshot_throws_404_exception_on_error(self):
        try:
            deleted = self.aliClient._delete_snapshot(snapshot_404_id)
            assert deleted
        except Exception as error:
            assert False #error should not be raised

    def test_ali_delete_snapshot_throws_exception_on_error(self):
        try:
            self.aliClient._delete_snapshot(snapshot_exc_id)
            assert False #error should be raised
        except Exception as error:
            assert "Failed to delete snapshot" in str(error)
    
    def test_ali_gets_snapshot_successfully(self):
        expected_snapshot = Snapshot(snapshot_id, 20, snapshot_creation_time, 'accomplished')
        snapshot = self.aliClient._get_snapshot(
            snapshot_id)
        assert snapshot.id == expected_snapshot.id
        assert snapshot.status == expected_snapshot.status
        assert snapshot.start_time == expected_snapshot.start_time
        assert snapshot.size == expected_snapshot.size
    
    def test_ali_get_snapshot_throws_exception_on_error(self):
        try:
            self.aliClient._get_snapshot(snapshot_delete_id)
        except Exception as error:
            assert 'Snapshot with id {} is not found'.format(snapshot_delete_id) in str(error)

    def test_ali_gets_attached_volumes_for_instance(self):
        volume_list = self.aliClient.get_attached_volumes_for_instance(valid_vm_id)

        assert volume_list[0].id == ephemeral_disk_id
        assert volume_list[0].status == 'In_use'
        assert volume_list[0].size == int(ephemeral_disk_size)
        assert volume_list[0].device == ephemeral_disk_device_id
        assert volume_list[1].id == persistent_disk_id
        assert volume_list[1].status == 'In_use'
        assert volume_list[1].size == int(persistent_disk_size)
        assert volume_list[1].device == persistent_disk_device_id
    
    def test_ali_get_attached_volumes_returns_empty_list_on_error(self):
        volume_list = self.aliClient.get_attached_volumes_for_instance(invalid_vm_id)

        assert volume_list == []


    def test_ali_gets_persistent_volume_for_instance(self):
        volume = self.aliClient.get_persistent_volume_for_instance(valid_vm_id)
        
        assert volume.id == persistent_disk_id
        assert volume.status == 'In_use'
        assert volume.size == int(persistent_disk_size)
        assert volume.device == persistent_disk_device_id
        self.patchers[4]['patcher_start'].call_count == 2
    
    def test_ali_gets_persistent_volume_for_instance_returns_none_on_error(self):
        assert self.aliClient.get_persistent_volume_for_instance(invalid_vm_id) is None
    
    def test_ali_gets_volume_successfully(self):
        volume = self.aliClient._get_volume(ephemeral_disk_id)
        assert volume.id == ephemeral_disk_id
        assert volume.status == 'In_use'
        assert volume.size == int(ephemeral_disk_size)

    def test_ali_get_volume_returns_none_on_duplicate(self):
        try:
            self.aliClient._get_volume(disk_duplicate_id)
        except Exception as error:
            assert "More than 1 volumes found for with id duplicatedisk1" in str(error)
    
    def test_ali_get_volume_returns_none_if_notfound(self):
        try:
            self.aliClient._get_volume(disk_delete_id)
        except Exception as error:
            assert "Volume with id deletedisk1 is not found" in str(error)

    def test_ali_is_volume_ready_returns_true(self):
        assert self.aliClient._is_volume_ready(ephemeral_disk_id) is True
    
    def test_ali_is_volume_ready_returns_false_on_error(self):
        assert self.aliClient._is_volume_ready(disk_exc_id) is False

    def test_ali_creates_volume_successfully(self):
        volume = self.aliClient._create_volume(20)

        assert volume.id == ephemeral_disk_id
        assert volume.status == 'In_use'
        assert volume.size == int(ephemeral_disk_size)
    
    def test_ali_create_volume_throws_exception_on_error(self):
        try:
            self.aliClient._create_volume(20, volume_type='invalid')
        except Exception as error:
            assert "[VOLUME] [CREATE] ERROR: size=20" in str(error)

    def test_ali_create_volume_throws_exception_if_duplicate(self):
        try:
            # Passing volumetype as duplicate to mock duplicate disk id
            self.aliClient._create_volume(20, volume_type='duplicate')
        except Exception as error:
            assert "More than 1 volumes found for with id disk_create_duplicate_id" in str(error)

    def test_ali_deletes_volume_successfully(self):
        deleted = self.aliClient._delete_volume(disk_delete_id)

        assert deleted == True

    def test_ali_delete_volume_throws_exception_on_error(self):
        try:
            self.aliClient._delete_volume(disk_exc_id)
        except Exception as error:
            assert "Failed to delete disk" in str(error)

    def test_ali_delete_volume_throws_404_exception_on_error(self):
        try:
            deleted = self.aliClient._delete_volume(disk_404_id)
            assert deleted
        except Exception as error:
            assert False #error should not be raised

    def test_ali_find_volume_device_returns_none_on_error(self):
        assert self.aliClient._find_volume_device(disk_exc_id) is None

    def test_ali_find_volume_device_returns_none_if_not_found(self):
        assert self.aliClient._find_volume_device(disk_delete_id) is None
    
    def test_ali_find_volume_device_returns_none_on_duplicate(self):
        assert self.aliClient._find_volume_device(disk_duplicate_id) is None

    def test_ali_create_attachment_successfully(self):
        attachment = self.aliClient._create_attachment(ephemeral_disk_id, valid_vm_id)

        assert attachment.volume_id == ephemeral_disk_id
        assert attachment.instance_id == valid_vm_id
    
    def test_ali_create_attachment_throws_exception_on_error(self):
        try:
            self.aliClient._create_attachment(ephemeral_disk_id, invalid_vm_id)
        except Exception as error:
            assert "Failed to create attachment" in str(error)
    
    def test_ali_create_attachment_throws_exception_if_device_is_None(self):
        try:
            self.aliClient._create_attachment(disk_attach_error_id, valid_vm_id)
        except Exception as error:
            assert "Device returned for volume-id=disk_attach_error_id is None" in str(error)
    
    def test_ali_deletes_attachment_successfully(self):
        deleted = self.aliClient._delete_attachment(disk_detach_id, valid_vm_id)

        assert deleted == True

    def test_ali_delete_attachment_throws_exception_on_error(self):
        try:
            self.aliClient._delete_attachment(disk_detach_id, invalid_vm_id)
        except Exception as error:
            assert "Failed to delete attachment" in str(error)

    def test_ali_delete_attachment_throws_404_exception_on_error(self):
        try:
            deleted = self.aliClient._delete_attachment(disk_detach_id, notfound_vm_id)
            assert deleted
        except Exception as error:
            assert False

    def test_ali_gets_mounpoint_successfully(self):
        assert self.aliClient.get_mountpoint(persistent_disk_id) == persistent_disk_mount_device_id

    