from lib.clients.GcpClient import GcpClient
from lib.clients.BaseClient import BaseClient
from unittest.mock import patch
from googleapiclient.http import HttpRequest
from googleapiclient.model import JsonModel
from googleapiclient.http import HttpMock
import os

operation_name = 'backup'
project_id = 'gcp-dev'
container = 'backup-container'
configuration = {
    'type' : 'online',
    'backup_guid' : 'backup-guid',
    'instance_id' : 'vm-id',
    'secret' : 'xyz',
    'job_name' : 'service-job-name',
    'container' : container,
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
 'id': container,
 'selfLink': 'https://something.com/storage/v1/b/' + container,
 'projectNumber': '11111',
 'name': container,
 'timeCreated': '2017-12-24T10:23:50.348Z',
 'updated': '2017-12-24T10:23:50.348Z',
 'metageneration': '1',
 "location": 'EUROPE-WEST1',
 'storageClass': 'REGIONAL',
 'etag': 'CAE='
}

class ComputeClient:
    class instances:
        def aggregatedList(self, project, filter=None):
            http = HttpMock('tests/data/instances.aggregatedList.json', {'status': '200'})
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

class StorageClient:
    def get_bucket(self, container):
        return bucket

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