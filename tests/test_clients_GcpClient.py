import os
import pytest
from lib.clients.GcpClient import GcpClient
from lib.clients.BaseClient import BaseClient
from unittest.mock import patch
from googleapiclient.http import HttpRequest
from googleapiclient.model import JsonModel
from googleapiclient.http import HttpMock
from google.cloud.exceptions import NotFound
from google.cloud.exceptions import Forbidden
from lib.models.Snapshot import Snapshot
from lib.models.Volume import Volume

operation_name = 'backup'
project_id = 'gcp-dev'
valid_container = 'backup-container'
invalid_container = 'invalid-container'
configuration = {
    'type' : 'online',
    'backup_guid' : 'backup-guid',
    'instance_id' : 'vm-id',
    'secret' : 'xyz',
    'job_name' : 'service-job-name',
    'container' : valid_container,
    'projectId' : project_id,
    'credentials': '{ "type": "service_account", "project_id": "gcp-dev", "client_id": "123", "private_key_id": "2222",  "private_key": "-----BEGIN PRIVATE KEY-----\\nMIIEFatI0=\\n-----END PRIVATE KEY-----\\n", "client_email": "user@gcp-dev.com", "client_id": "6666", "auth_uri": "auth_uri", "token_uri": "token_uri", "auth_provider_x509_cert_url": "cert_url", "client_x509_cert_url": "cert_url"  }'
}
directory_persistent = '/tmp'
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

valid_disk_name = 'disk-id'
not_found_disk_name = 'notfound-disk-id'
invalid_disk_name = 'invalid-disk-id'

class ComputeClient:
    class instances:
        def aggregatedList(self, project, filter=None):
            http = HttpMock('tests/data/gcp/instances.aggregatedList.json', {'status': '200'})
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

    class snapshots:
        def get(self, project, snapshot):
            if snapshot == valid_snapshot_name:
                http = HttpMock('tests/data/gcp/snapshots.get.json', {'status': '200'})
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
                http = HttpMock('tests/data/gcp/snapshots.get.notfound.json', {'status': '404'})
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
                http = HttpMock('tests/data/gcp/snapshots.get.forbidden.json', {'status': '403'})
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

    class disks:
        def get(self, project, zone, disk):
            if disk == valid_disk_name:
                http = HttpMock('tests/data/gcp/disks.get.json', {'status': '200'})
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
                http = HttpMock('tests/data/gcp/disks.get.notfound.json', {'status': '404'})
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
                http = HttpMock('tests/data/gcp/disks.get.forbidden.json', {'status': '403'})
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

class StorageClient:
    def get_bucket(self, container):
        if container == valid_container:
            return bucket
        elif container == invalid_container:
            raise NotFound()

class Blob:
    def upload_from_string(self, data, content_type='text/plain', client=None, predefined_acl=None):
        pass
    
    def delete(self, client=None):
        return self

class TestGcpClient:
    @classmethod
    def setup_class(cls):
        cls.mock_lastop_patcher = patch.object(BaseClient, 'last_operation')
        cls.mock_lastop = cls.mock_lastop_patcher.start()
        cls.mock_sevice_account_patcher = patch('google.oauth2.service_account.Credentials.from_service_account_info')
        cls.mock_sevice_account = cls.mock_sevice_account_patcher.start()
        cls.mock_compute_client_patcher = patch('googleapiclient.discovery.build')
        cls.mock_compute_client = cls.mock_compute_client_patcher.start()
        cls.mock_storage_client_patcher = patch('google.cloud.storage.Client')
        cls.mock_storage_client = cls.mock_storage_client_patcher.start()
        cls.mock_blob_upload_patcher = patch('google.cloud.storage.Blob.upload_from_string')
        cls.mock_blob_upload = cls.mock_blob_upload_patcher.start()
        cls.mock_blob_delete_patcher = patch('google.cloud.storage.Blob.delete')
        cls.mock_blob_delete = cls.mock_blob_delete_patcher.start()
        os.environ['SF_BACKUP_RESTORE_LOG_DIRECTORY'] = log_dir
        os.environ['SF_BACKUP_RESTORE_LAST_OPERATION_DIRECTORY'] = log_dir
        cls.mock_compute_client.return_value = ComputeClient()
        cls.mock_storage_client.return_value = StorageClient()
        data = 'random data'
        blob = Blob()
        cls.mock_blob_upload.return_value = blob.upload_from_string(data)
        cls.mock_blob_delete.return_value = blob.delete()
        cls.gcpClient = GcpClient(operation_name, configuration, directory_persistent, directory_work_list,
            poll_delay_time, poll_maximum_time)

    @classmethod
    def teardown_class(cls):
        cls.mock_lastop_patcher.stop()
        cls.mock_sevice_account_patcher.stop()
        cls.mock_compute_client_patcher.stop()
        cls.mock_storage_client_patcher.stop()
        cls.mock_blob_upload_patcher.stop()
        cls.mock_blob_delete_patcher.stop()

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
        pytest.raises(Exception, self.gcpClient.get_snapshot, invalid_snapshot_name)

    def test_snapshot_exists(self):
        assert self.gcpClient.snapshot_exists(valid_snapshot_name) == True
    
    def test_snapshot_exists_not_found(self):
        assert self.gcpClient.snapshot_exists(not_found_snapshot_name) == False

    def test_snapshot_exists_forbidden(self):
        pytest.raises(Exception, self.gcpClient.snapshot_exists, invalid_snapshot_name)
    
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
        pytest.raises(Exception, self.gcpClient.volume_exists, invalid_disk_name)