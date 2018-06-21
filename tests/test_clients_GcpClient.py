import os
import unittest.mock
import pytest
import json
import glob
from lib.clients.GcpClient import GcpClient
from lib.clients.BaseClient import BaseClient
from unittest.mock import patch
from googleapiclient.http import HttpRequest
from googleapiclient.model import JsonModel
from googleapiclient.http import HttpMock
from google.cloud.exceptions import NotFound
from google.cloud.exceptions import GoogleCloudError
from google.cloud.storage import Blob
from lib.models.Snapshot import Snapshot
from lib.models.Volume import Volume
from unittest.mock import Mock, patch

operation_name = 'backup'
project_id = 'gcp-dev'
valid_container = 'backup-container'
invalid_container = 'invalid-container'
configuration = {
    'credhub_url': None,
    'type': 'online',
    'backup_guid': 'backup-guid',
    'instance_id': 'vm-id',
    'secret': 'xyz',
    'job_name': 'service-job-name',
    'container': valid_container,
    'projectId': project_id,
    'credentials': '{ "type": "service_account", "project_id": "gcp-dev", "client_id": "123", "private_key_id": "2222",  "private_key": "-----BEGIN PRIVATE KEY-----\\nMIIEFatI0=\\n-----END PRIVATE KEY-----\\n", "client_email": "user@gcp-dev.com", "client_id": "6666", "auth_uri": "auth_uri", "token_uri": "token_uri", "auth_provider_x509_cert_url": "cert_url", "client_x509_cert_url": "cert_url"  }'
}
directory_persistent = '/var/vcap/store'
directory_work_list = '/tmp'
log_dir = 'tests'
poll_delay_time = 10
poll_maximum_time = 60
availability_zone = 'europe-west1-b'
gcpClient = None
bucket = {
    'kind': 'storage#bucket',
    'id': valid_container,
    'selfLink': 'https://something.com/storage/v1/b/' + valid_container,
    'projectNumber': '11111',
    'name': valid_container,
    'timeCreated': '2017-12-24T10:23:50.348Z',
    'updated': '2017-12-24T10:23:50.348Z',
    'metageneration': '1',
    "location": 'EUROPE-WEST1',
    'storageClass': 'REGIONAL',
    'etag': 'CAE='
}
valid_snapshot_name = 'snapshot-id'
not_found_snapshot_name = 'notfound-snapshot-id'
invalid_snapshot_name = 'invalid-snapshot-id'
delete_snapshot_name = 'delete-snapshot-id'
valid_disk_name = 'disk-id'
not_found_disk_name = 'notfound-disk-id'
invalid_disk_name = 'invalid-disk-id'
delete_disk_name = 'delete-disk-id'
some_disk_name = 'some-disk-id'
ephemeral_disk_name = 'vm-id'  # ephemeral disk name is by default vm name
ephemeral_disk_id = 'persistent-disk-0'
ephemeral_disk_device_path = '/dev/disk/by-id/google-persistent-disk-0'
ephemeral_disk_device_id = '/dev/sda'
persistent_disk_device_path = '/dev/disk/by-id/google-disk-id'
persistent_disk_device_id = '/dev/sdb'
valid_vm_id = 'vm-id'
invalid_vm_id = 'invalid-vm-id'
valid_operation_id = 'operation-id'
pending_operation_id = 'running-operation-id'
invalid_operation_id = 'invalid-operation-id'
error_operation_id = 'error-operation-id'
valid_blob_path = '/tmp/valid-blob.txt'
invalid_blob_path = '/tmp/invalid-blob.txt'


class ComputeClient:
    class instances:
        def aggregatedList(self, project, filter=None):
            http = HttpMock(
                'tests/data/gcp/instances.aggregatedList.json', {'status': '200'})
            model = JsonModel()
            uri = 'some_uri'
            method = 'GET'
            return HttpRequest(
                http,
                model.response,
                uri,
                method=method,
                headers={}
            )

        def aggregatedList_next(previous_request, previous_response):
            return None

        def get(self, project, zone, instance):
            if instance == valid_vm_id:
                http = HttpMock(
                    'tests/data/gcp/instances.get.json', {'status': '200'})
                model = JsonModel()
                uri = 'some_uri'
                method = 'GET'
                return HttpRequest(
                    http,
                    model.response,
                    uri,
                    method=method,
                    headers={}
                )
            else:
                return None

        def attachDisk(self, project, zone, instance, body):
            if instance == valid_vm_id:
                http = HttpMock(
                    'tests/data/gcp/instances.attachDisk.json', {'status': '200'})
                model = JsonModel()
                uri = 'some_uri'
                method = 'POST'
                return HttpRequest(
                    http,
                    model.response,
                    uri,
                    method=method,
                    headers={}
                )

        def detachDisk(self, project, zone, instance, deviceName):
            if instance == valid_vm_id:
                http = HttpMock(
                    'tests/data/gcp/instances.detachDisk.json', {'status': '200'})
                model = JsonModel()
                uri = 'some_uri'
                method = 'POST'
                return HttpRequest(
                    http,
                    model.response,
                    uri,
                    method=method,
                    headers={}
                )

    class snapshots:
        def get(self, project, snapshot):
            if snapshot == valid_snapshot_name:
                http = HttpMock(
                    'tests/data/gcp/snapshots.get.json', {'status': '200'})
                model = JsonModel()
                uri = 'some_uri'
                method = 'GET'
                return HttpRequest(
                    http,
                    model.response,
                    uri,
                    method=method,
                    headers={}
                )
            elif snapshot == not_found_snapshot_name:
                http = HttpMock(
                    'tests/data/gcp/snapshots.get.notfound.json', {'status': '404'})
                model = JsonModel()
                uri = 'some_uri'
                method = 'GET'
                return HttpRequest(
                    http,
                    model.response,
                    uri,
                    method=method,
                    headers={}
                )
            elif snapshot == invalid_snapshot_name:
                http = HttpMock(
                    'tests/data/gcp/snapshots.get.forbidden.json', {'status': '403'})
                model = JsonModel()
                uri = 'some_uri'
                method = 'GET'
                return HttpRequest(
                    http,
                    model.response,
                    uri,
                    method=method,
                    headers={}
                )
            if snapshot == delete_snapshot_name:
                http = HttpMock(
                    'tests/data/gcp/snapshots.get.notfound.json', {'status': '404'})
                model = JsonModel()
                uri = 'some_uri'
                method = 'GET'
                return HttpRequest(
                    http,
                    model.response,
                    uri,
                    method=method,
                    headers={}
                )

        def delete(self, project, snapshot):
            if snapshot == delete_snapshot_name or snapshot == valid_snapshot_name:
                http = HttpMock(
                    'tests/data/gcp/snapshots.delete.json', {'status': '200'})
                model = JsonModel()
                uri = 'some_uri'
                method = 'DELETE'
                return HttpRequest(
                    http,
                    model.response,
                    uri,
                    method=method,
                    headers={}
                )

    class disks:
        def get(self, project, zone, disk):
            if disk == valid_disk_name:
                http = HttpMock('tests/data/gcp/disks.get.json',
                                {'status': '200'})
                model = JsonModel()
                uri = 'some_uri'
                method = 'GET'
                return HttpRequest(
                    http,
                    model.response,
                    uri,
                    method=method,
                    headers={}
                )
            if disk == some_disk_name:
                http = HttpMock(
                    'tests/data/gcp/disks.get1.json', {'status': '200'})
                model = JsonModel()
                uri = 'some_uri'
                method = 'GET'
                return HttpRequest(
                    http,
                    model.response,
                    uri,
                    method=method,
                    headers={}
                )
            elif disk == not_found_disk_name:
                http = HttpMock(
                    'tests/data/gcp/disks.get.notfound.json', {'status': '404'})
                model = JsonModel()
                uri = 'some_uri'
                method = 'GET'
                return HttpRequest(
                    http,
                    model.response,
                    uri,
                    method=method,
                    headers={}
                )
            elif disk == invalid_disk_name:
                http = HttpMock(
                    'tests/data/gcp/disks.get.forbidden.json', {'status': '403'})
                model = JsonModel()
                uri = 'some_uri'
                method = 'GET'
                return HttpRequest(
                    http,
                    model.response,
                    uri,
                    method=method,
                    headers={}
                )
            elif disk == ephemeral_disk_name:
                http = HttpMock(
                    'tests/data/gcp/disks.get.ephemeral.json', {'status': '200'})
                model = JsonModel()
                uri = 'some_uri'
                method = 'GET'
                return HttpRequest(
                    http,
                    model.response,
                    uri,
                    method=method,
                    headers={}
                )
            elif disk == delete_disk_name:
                http = HttpMock(
                    'tests/data/gcp/disks.get.notfound.json', {'status': '404'})
                model = JsonModel()
                uri = 'some_uri'
                method = 'GET'
                return HttpRequest(
                    http,
                    model.response,
                    uri,
                    method=method,
                    headers={}
                )

        def delete(self, project, zone, disk):
            if disk == delete_disk_name or disk == valid_disk_name:
                http = HttpMock(
                    'tests/data/gcp/disks.delete.json', {'status': '200'})
                model = JsonModel()
                uri = 'some_uri'
                method = 'DELETE'
                return HttpRequest(
                    http,
                    model.response,
                    uri,
                    method=method,
                    headers={}
                )

        def createSnapshot(self, project, zone, disk, body):
            if disk == valid_disk_name:
                http = HttpMock(
                    'tests/data/gcp/disks.createSnapshot.json', {'status': '200'})
                model = JsonModel()
                uri = 'some_uri'
                method = 'POST'
                return HttpRequest(
                    http,
                    model.response,
                    uri,
                    method=method,
                    headers={}
                )
            elif disk == invalid_disk_name:
                http = HttpMock(
                    'tests/data/gcp/disks.createSnapshot1.json', {'status': '200'})
                model = JsonModel()
                uri = 'some_uri'
                method = 'POST'
                return HttpRequest(
                    http,
                    model.response,
                    uri,
                    method=method,
                    headers={}
                )

        def insert(self, project, zone, body):
            if body['sourceSnapshot'] == 'global/snapshots/{}'.format(
                    valid_snapshot_name):
                http = HttpMock(
                    'tests/data/gcp/disks.insert.json', {'status': '200'})
                model = JsonModel()
                uri = 'some_uri'
                method = 'POST'
                return HttpRequest(
                    http,
                    model.response,
                    uri,
                    method=method,
                    headers={}
                )
            elif body['sourceSnapshot'] == 'global/snapshots/{}'.format(
                    invalid_snapshot_name):
                http = HttpMock(
                    'tests/data/gcp/disks.insert1.json', {'status': '200'})
                model = JsonModel()
                uri = 'some_uri'
                method = 'POST'
                return HttpRequest(
                    http,
                    model.response,
                    uri,
                    method=method,
                    headers={}
                )

    class zoneOperations:
        def get(self, project, zone, operation):
            if operation == valid_operation_id:
                http = HttpMock(
                    'tests/data/gcp/zoneOperations.get.json', {'status': '200'})
                model = JsonModel()
                uri = 'some_uri'
                method = 'GET'
                return HttpRequest(
                    http,
                    model.response,
                    uri,
                    method=method,
                    headers={}
                )
            elif operation == pending_operation_id:
                http = HttpMock(
                    'tests/data/gcp/zoneOperations.get.running.json', {'status': '200'})
                model = JsonModel()
                uri = 'some_uri'
                method = 'GET'
                return HttpRequest(
                    http,
                    model.response,
                    uri,
                    method=method,
                    headers={}
                )
            elif operation == error_operation_id:
                http = HttpMock(
                    'tests/data/gcp/zoneOperations.get.error.json', {'status': '200'})
                model = JsonModel()
                uri = 'some_uri'
                method = 'GET'
                return HttpRequest(
                    http,
                    model.response,
                    uri,
                    method=method,
                    headers={}
                )
            else:
                return None

    class globalOperations:
        def get(self, project, operation):
            if operation == valid_operation_id:
                http = HttpMock(
                    'tests/data/gcp/globalOperations.get.json', {'status': '200'})
                model = JsonModel()
                uri = 'some_uri'
                method = 'GET'
                return HttpRequest(
                    http,
                    model.response,
                    uri,
                    method=method,
                    headers={}
                )
            else:
                return None


class StorageClient:
    def get_bucket(self, container):
        if container == valid_container:
            return bucket
        elif container == invalid_container:
            raise NotFound()


def upload_from_filename(filename):
    if filename == valid_blob_path:
        pass
    else:
        raise GoogleCloudError('Error')


def download_to_filename(filename):
    if filename == valid_blob_path:
        pass
    else:
        raise GoogleCloudError("Error")


def shell(command):
    if command == ('readlink -e ' + ephemeral_disk_device_path):
        return ephemeral_disk_device_id
    elif command == ('readlink -e ' + persistent_disk_device_path):
        return persistent_disk_device_id
    elif command == ('cat /proc/mounts | grep ' + directory_persistent):
        return persistent_disk_device_id + '1 ' + directory_persistent + ' ext4 rw,relatime,data=ordered 0 0'


def mockglob(path):
    if (valid_disk_name in path) or (ephemeral_disk_id in path):
        return [path]
    else:
        return [path, 'dummy/' + path]


def generate_name_by_prefix(prefix):
    if prefix == 'sf-snapshot':
        return valid_snapshot_name
    elif prefix == 'sf-disk':
        return valid_disk_name


def get_device_of_volume(volume_id):
    if volume_id == valid_disk_name:
        return persistent_disk_device_id
    else:
        return None


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


class TestGcpClient:
    # Store all patchers
    patchers = []

    @classmethod
    def setup_class(self):
        self.patchers.append(create_start_patcher(
            patch_function='_get_device_of_volume', patch_object=BaseClient, side_effect=get_device_of_volume))
        self.patchers.append(create_start_patcher(
            patch_function='delete_snapshot', patch_object=BaseClient, return_value=True))
        self.patchers.append(create_start_patcher(
            patch_function='delete_volume', patch_object=BaseClient, return_value=True))
        self.patchers.append(create_start_patcher(
            patch_function='glob', patch_object=glob, side_effect=mockglob))
        self.patchers.append(create_start_patcher(patch_function='generate_name_by_prefix',
                                                  patch_object=BaseClient, side_effect=generate_name_by_prefix))
        self.patchers.append(create_start_patcher(
            patch_function='shell', patch_object=BaseClient, side_effect=shell))
        self.patchers.append(create_start_patcher(
            patch_function='last_operation', patch_object=BaseClient))
        self.patchers.append(create_start_patcher(
            patch_function='google.oauth2.service_account.Credentials.from_service_account_info'))
        self.patchers.append(create_start_patcher(
            patch_function='googleapiclient.discovery.build', return_value=ComputeClient()))
        self.patchers.append(create_start_patcher(
            patch_function='google.cloud.storage.Client', return_value=StorageClient()))
        self.patchers.append(create_start_patcher(
            patch_function='google.cloud.storage.Blob.upload_from_string'))
        self.patchers.append(create_start_patcher(
            patch_function='google.cloud.storage.Blob.delete'))
        self.patchers.append(create_start_patcher(
            patch_function='upload_from_filename', patch_object=Blob, side_effect=upload_from_filename))
        self.patchers.append(create_start_patcher(
            patch_function='download_to_filename', patch_object=Blob, side_effect=download_to_filename))

        os.environ['SF_BACKUP_RESTORE_LOG_DIRECTORY'] = log_dir
        os.environ['SF_BACKUP_RESTORE_LAST_OPERATION_DIRECTORY'] = log_dir

        self.gcpClient = GcpClient(operation_name, configuration, directory_persistent, directory_work_list,
                                   poll_delay_time, poll_maximum_time)

    @classmethod
    def teardown_class(self):
        stop_all_patchers(self.patchers)

    def test_create_gcp_client(self):
        assert self.gcpClient.project_id == project_id
        assert self.gcpClient.compute_client is not None
        assert self.gcpClient.storage_client is not None
        assert self.gcpClient.container == bucket
        assert self.gcpClient.availability_zone == availability_zone

    def test_get_container_exception(self):
        self.gcpClient.CONTAINER = invalid_container
        assert self.gcpClient.get_container() is None
        self.gcpClient.CONTAINER = valid_container

    def test_get_snapshot(self):
        expected_snapshot = Snapshot(valid_snapshot_name, '40', 'READY')
        snapshot = self.gcpClient.get_snapshot(valid_snapshot_name)
        assert expected_snapshot.id == snapshot.id
        assert expected_snapshot.size == snapshot.size
        assert expected_snapshot.status == snapshot.status

    def test_get_snapshot_exception(self):
        pytest.raises(Exception, self.gcpClient.get_snapshot,
                      invalid_snapshot_name)

    def test_snapshot_exists(self):
        assert self.gcpClient.snapshot_exists(valid_snapshot_name) == True

    def test_snapshot_exists_not_found(self):
        assert self.gcpClient.snapshot_exists(not_found_snapshot_name) == False

    def test_snapshot_exists_forbidden(self):
        pytest.raises(Exception, self.gcpClient.snapshot_exists,
                      invalid_snapshot_name)

    def test_get_volume(self):
        expected_volume = Volume(valid_disk_name, 'READY', '40')
        volume = self.gcpClient.get_volume(valid_disk_name)
        assert expected_volume.id == volume.id
        assert expected_volume.size == volume.size
        assert expected_volume.status == volume.status

    def test_get_volume_exception(self):
        self.gcpClient.get_volume(invalid_disk_name) is None

    def test_volume_exists(self):
        assert self.gcpClient.volume_exists(valid_disk_name) == True

    def test_volume_exists_not_found(self):
        assert self.gcpClient.volume_exists(not_found_disk_name) == False

    def test_volume_exists_forbidden(self):
        pytest.raises(Exception, self.gcpClient.volume_exists,
                      invalid_disk_name)

    def test_get_attached_volumes_for_instance(self):
        volume_list = self.gcpClient.get_attached_volumes_for_instance(
            valid_vm_id)
        assert volume_list[0].id == 'vm-id'
        assert volume_list[0].status == 'READY'
        assert volume_list[0].size == '10'
        assert volume_list[0].device == ephemeral_disk_device_id
        assert volume_list[1].id == 'disk-id'
        assert volume_list[1].status == 'READY'
        assert volume_list[1].size == '40'
        assert volume_list[1].device == persistent_disk_device_id

    def test_get_attached_volumes_for_instance_returns_empty(self):
        assert self.gcpClient.get_attached_volumes_for_instance(
            invalid_vm_id) == []

    def test_get_persistent_volume_for_instance(self):
        volume = self.gcpClient.get_persistent_volume_for_instance(valid_vm_id)
        assert volume.id == 'disk-id'
        assert volume.status == 'READY'
        assert volume.size == '40'
        assert volume.device == persistent_disk_device_id

    def test_get_persistent_volume_for_instance_returns_none(self):
        assert self.gcpClient.get_persistent_volume_for_instance(
            invalid_vm_id) is None

    def test_copy_snapshot(self):
        snapshot = self.gcpClient._copy_snapshot(valid_snapshot_name)
        assert snapshot.id == valid_snapshot_name
        assert snapshot.status == 'READY'
        assert snapshot.size == '40'

    def test_create_snapshot(self):
        snapshot = self.gcpClient._create_snapshot(
            valid_disk_name, 'test-backup')
        assert snapshot.id == valid_snapshot_name
        assert snapshot.status == 'READY'
        assert snapshot.size == '40'
        self.gcpClient.output_json['snapshotId'] = valid_snapshot_name

    def test_create_snapshot_exception(self):
        pytest.raises(Exception, self.gcpClient._create_snapshot,
                      invalid_disk_name)

    def test_delete_snapshot(self):
        assert self.gcpClient._delete_snapshot(delete_snapshot_name) == True

    def test_delete_snapshot_exception(self):
        pytest.raises(Exception, self.gcpClient._delete_snapshot,
                      valid_snapshot_name)

    def test_get_mountpoint_returns_none(self):
        assert self.gcpClient.get_mountpoint(invalid_disk_name, "1") is None

    def test_get_mountpoint_without_partition(self):
        assert self.gcpClient.get_mountpoint(
            valid_disk_name) == persistent_disk_device_id

    def test_get_mountpoint(self):
        assert self.gcpClient.get_mountpoint(
            valid_disk_name, "1") == (persistent_disk_device_id + "1")

    def test_find_volume_device(self):
        assert self.gcpClient._find_volume_device(
            valid_disk_name) == persistent_disk_device_id

    def test_find_volume_device_returns_none(self):
        assert self.gcpClient._find_volume_device('invalid_disk_name') is None

    def test_get_operation_status_zonal_operation(self):
        assert self.gcpClient.get_operation_status(
            valid_operation_id, True) == 'DONE'

    def test_get_operation_status_global_operation(self):
        assert self.gcpClient.get_operation_status(
            valid_operation_id, False) == 'DONE'

    def test_get_operation_status_zonal_operation_pending(self):
        assert self.gcpClient.get_operation_status(
            pending_operation_id, True) == 'RUNNING'

    def test_get_operation_status_zonal_operation_error(self):
        pytest.raises(Exception, self.gcpClient.get_operation_status,
                      error_operation_id, True)

    def test_create_volume(self):
        volume = self.gcpClient._create_volume('40', valid_snapshot_name)
        assert volume.id == valid_disk_name
        assert volume.status == 'READY'
        assert volume.size == '40'

    def test_create_volume_exception(self):
        pytest.raises(Exception, self.gcpClient._create_volume,
                      '40', invalid_snapshot_name)

    def test_delete_volume(self):
        assert self.gcpClient._delete_volume(delete_disk_name) == True

    def test_delete_volume_exception(self):
        pytest.raises(Exception, self.gcpClient._delete_volume,
                      valid_disk_name)

    def test_create_attachment(self):
        attachment = self.gcpClient._create_attachment(
            valid_disk_name, valid_vm_id)
        assert attachment.id == 0
        assert attachment.instance_id == valid_vm_id
        assert attachment.volume_id == valid_disk_name

    def test_create_attachment_exception(self):
        pytest.raises(Exception, self.gcpClient._create_attachment,
                      some_disk_name, valid_vm_id)

    def test_delete_attachment(self):
        assert self.gcpClient._delete_attachment(
            valid_disk_name, valid_vm_id) == True

    def test_delete_attachment_exception(self):
        pytest.raises(Exception, self.gcpClient._delete_attachment,
                      invalid_disk_name, invalid_vm_id)

    def test_upload_to_blobstore(self):
        assert self.gcpClient._upload_to_blobstore(
            valid_blob_path, 'blob') == True

    def test_upload_to_blobstore_raises_exception(self):
        pytest.raises(Exception, self.gcpClient._upload_to_blobstore,
                      invalid_blob_path, 'blob')

    def test_upload_to_blobstore_raises_exception_on_invalid_container(self):
        prev_container = self.gcpClient.container
        self.gcpClient.container = None
        pytest.raises(Exception, self.gcpClient._upload_to_blobstore,
                      invalid_blob_path, 'blob')
        self.gcpClient.container = prev_container

    def test_download_from_blobstore(self):
        assert self.gcpClient._download_from_blobstore(
            'blob', valid_blob_path) == True

    def test_download_from_blobstore_raises_exception(self):
        pytest.raises(
            Exception, self.gcpClient._download_from_blobstore, 'blob', invalid_blob_path)

    def test_download_from_blobstore_raises_exception_on_invalid_container(self):
        prev_container = self.gcpClient.container
        self.gcpClient.container = None
        pytest.raises(
            Exception, self.gcpClient._download_from_blobstore, 'blob', invalid_blob_path)
        self.gcpClient.container = prev_container

    @patch('lib.clients.BaseClient.requests.post')
    @patch('lib.clients.BaseClient.requests.get')
    def test_gcp_client_creation(self,  mock_get, mock_post):
        credhub_config = {
            'type': 'online',
            'backup_guid': 'backup-guid',
            'instance_id': 'vm-id',
            'secret': 'xyz',
            'job_name': 'service-job-name',
            'container': valid_container,
            'projectId': project_id,
            'credhub_url': '/credhub',
            'credhub_uaa_url': '/oauth',
            'credhub_key': '/test',
            'credhub_client_id': '1',
            'credhub_client_secret': 'secret',
            'credhub_username': 'name',
            'credhub_user_password': 'pwd'
        }
        auth_response = {
            'access_token': 'auth-token-response'
        }
        mock_post.return_value = Mock(ok=True)
        mock_post.return_value.json.return_value = auth_response
        mock_get.return_value = Mock(ok=True)
        mock_get.return_value.json.return_value = {
            'data': [{
                'value': {
                    'type': 'service_account',
                    'project_id': 'gcp-dev',
                    'private_key_id': '2222',
                    'private_key': '-----BEGIN PRIVATE KEY-----\\nMIIEFatI0=\\n-----END PRIVATE KEY-----\\n',
                    'client_email': 'user@gcp-dev.com',
                    'client_id': '6666',
                    'auth_uri': 'auth_uri',
                    'token_uri': 'token_uri',
                    'auth_provider_x509_cert_url': 'cert_url',
                    'client_x509_cert_url': 'cert_url'}}]}
        GcpClient(operation_name, credhub_config, directory_persistent, directory_work_list,
                  poll_delay_time, poll_maximum_time)


class TestGcpClientExceptions:
    # Store all patchers
    patchers = []

    @classmethod
    def setup_class(self):
        self.patchers.append(create_start_patcher(
            patch_function='google.oauth2.service_account.Credentials.from_service_account_info'))
        self.patchers.append(create_start_patcher(
            patch_function='googleapiclient.discovery.build', return_value=ComputeClient()))
        self.patchers.append(create_start_patcher(
            patch_function='google.cloud.storage.Client', return_value=StorageClient()))

    @classmethod
    def teardown_class(self):
        stop_all_patchers(self.patchers)

    def test_gcp_client_raises_container_not_found_exception(self):
        with pytest.raises(Exception, message='Could not find or access the given container.'):
            GcpClient(operation_name, configuration, directory_persistent, directory_work_list,
                      poll_delay_time, poll_maximum_time)

    def test_gcp_client_raises_availability_zone_exception(self):
        mock_blob_upload_patcher = create_start_patcher(
            patch_function='google.cloud.storage.Blob.upload_from_string')
        mock_blob_delete_patcher = create_start_patcher(
            patch_function='google.cloud.storage.Blob.delete')
        configuration['instance_id'] = invalid_vm_id
        with pytest.raises(Exception, message='Could not retrieve the availability zone of the instance.'):
            GcpClient(operation_name, configuration, directory_persistent, directory_work_list,
                      poll_delay_time, poll_maximum_time)
        mock_blob_upload_patcher.stop()
        mock_blob_delete_patcher.stop()
