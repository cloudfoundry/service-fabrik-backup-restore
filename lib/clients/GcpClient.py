from .BaseClient import BaseClient
from googleapiclient import discovery
from google.oauth2 import service_account
from google.cloud import storage
from google.cloud.storage import Blob
from .BaseClient import BaseClient
from ..models.Snapshot import Snapshot
from ..models.Volume import Volume
from ..models.Attachment import Attachment
import json

class GcpClient(BaseClient):
    def __init__(self, operation_name, configuration, directory_persistent, directory_work_list, poll_delay_time,
                 poll_maximum_time):
        super(GcpClient, self).__init__(operation_name, configuration, directory_persistent, directory_work_list,
                                        poll_delay_time, poll_maximum_time)
        
        self.__gcpCredentials = json.loads(configuration['credentials'])
        self.project_id = configuration['projectId']
        self.tags= {
            'instance_id': self.INSTANCE_ID,
            'job_name':self.JOB_NAME
            }

        self.compute_api_name = 'compute'
        self.compute_api_version = 'v1'
        self.snapshot_prefix = 'sf-snapshot'
        self.disk_prefix = 'sf-disk'

        self.compute_client = self.create_compute_client()
        self.storage_client = self.create_storage_client()
        
        # +-> Check whether the given container exists and is accessible
        if (not self.get_container()) or (not self.access_container()):
            msg = 'Could not find or access the given container.'
            self.last_operation(msg, 'failed')
            raise Exception(msg)

        # +-> Get the availability zone of the instance
        self.availability_zone = self._get_availability_zone_of_server(configuration['instance_id'])
        if not self.availability_zone:
            msg = 'Could not retrieve the availability zone of the instance.'
            self.last_operation(msg, 'failed')
            raise Exception(msg)

    def create_compute_client(self):
        try:
            credentials = service_account.Credentials.from_service_account_info(self.__gcpCredentials)
            compute_client = discovery.build(self.compute_api_name, self.compute_api_version, credentials=credentials)
            return compute_client
        except Exception as error:
            raise Exception('Creation of compute client failed: {}'.format(error))

    def create_storage_client(self):
        try:
            credentials = service_account.Credentials.from_service_account_info(self.__gcpCredentials)
            storage_client = storage.Client(self.project_id, credentials)
            return storage_client
        except Exception as error:
            raise Exception('Creation of storage client failed: {}'.format(error))

    def _get_availability_zone_of_server(self, instance_id):
        try:
            expression = 'name eq ' + instance_id
            request = self.compute_client.instances().aggregatedList(project=self.project_id, filter=expression)
            while request is not None:
                response = request.execute()
                for zone, instances_scoped_list in response['items'].items():
                    if 'warning' in instances_scoped_list:
                        pass
                    elif 'instances' in instances_scoped_list:
                        for instance in instances_scoped_list['instances']:
                            if instance['name'] == instance_id:
                                # Assuming the last part of the url is zone-name
                                return instance['zone'].rsplit('/', 1)[1]

                request = self.compute_client.instances().aggregatedList_next(previous_request=request, previous_response=response)

            self.logger.error('[GCP] ERROR: Unable to determine the availability zone of instance{}.\n'.format(instance_id))
            return None
        except Exception as error:
            self.logger.error('[GCP] ERROR: Unable to determine the availability zone of instance {}.\n{}'.format(instance_id, error))
            return None

    def get_container(self):
        try:
            container = self.storage_client.get_bucket(self.CONTAINER)
            return container
        except Exception as error:
            self.logger.error('[GCP] [STORAGE] ERROR: Unable to find container {}.\n{}'.format(
                self.CONTAINER, error))
            return None

    def access_container(self):
        # Test if the container is accessible
        try:
            bucket = self.storage_client.get_bucket(self.CONTAINER)
            blob = Blob('AccessTestByServiceFabrikPythonLibrary', bucket)
            blob.upload_from_string('Sample Message for AccessTestByServiceFabrikPythonLibrary', content_type='text/plain')
            blob.delete()
            return True
        except Exception as error:
            self.logger.error('[GCP] [STORAGE] ERROR: Unable to access/delete container {}.\n{}'.format(
                self.CONTAINER, error))
            return False
    
    
    def get_snapshot(self, snapshot_name):
        try:
            snapshot = self.compute_client.snapshots().get(project=self.project_id, snapshot=snapshot_name).execute()
            return Snapshot(snapshot['name'], snapshot['diskSizeGb'], snapshot['status'])
        except Exception as error:
            message = '[GCP] ERROR: Unable to get snapshot {}.\n{}'.format(
                snapshot_name, error)
            raise Exception(message)

    def snapshot_exists(self, snapshot_name):
        try:
            snapshot = self.compute_client.snapshots().get(project=self.project_id, snapshot=snapshot_name).execute()
            return True
        except Exception as error:
            if self.get_http_error_code(error) == 404:
                return False
            else:
                message = '[GCP] ERROR: Unable to get snapshot {}.\n{}'.format(
                    snapshot_name, error)
                self.logger.error(message)
                raise Exception(message)

    def get_http_error_code(self, error):
        return json.loads(error.content)['error']['code']
        
    '''
    def get_volume(self, volume_name):
        try:
            volume = compute_client.disks().get(project=project, zone=zone, disk=volume_name).execute()
            return Volume(volume['name'], volume['status'], volume['sizeGb'])
        except Exception as error:
            self.logger.error(
                '[GCP] ERROR: Unable to get volume/disk {}.\n{}'.format(
                volume_name, error))
            return None
    '''    
    def get_attached_volumes_for_instance(self, instance_id):
        ## TODO
        pass

    def get_persistent_volume_for_instance(self, instance_id):
        #TODO
        pass
        
    def _create_snapshot(self, volume_id):
        log_prefix = '[SNAPSHOT] [CREATE]'
        snapshot = None
        try:
            snapshot_name = self.generate_name_by_prefix(self.snapshot_prefix)
            snapshot_body = {
                'name': snapshot_name,
                'labels': self.tags,
                'description': 'Service-Fabrik: Automated backup'
            }
            self.logger.info('{} START for volume id {} with tags {} and snapshot name {}'.format(log_prefix, volume_id, self.tags, snapshot_name))
            snapshot_creation_operation = self.compute_client.disks().createSnapshot(project=self.project_id, zone=self.availability_zone, disk=volume_id, body=snapshot_body).execute()
            
            self._wait('Waiting for snapshot {} to get ready...'.format(snapshot_name),
                       (lambda operation_id, zonal_operation: self.wait_for_operation(operation_id, zonal_operation) == 'DONE'),
                       None,
                       snapshot_creation_operation['name'], True)

            snapshot = self.get_snapshot(snapshot_name)
            self._add_snapshot(snapshot.id)
            self.logger.info('{} SUCCESS: snapshot-id={}, volume-id={} with tags {}'.format(log_prefix, snapshot.id, volume_id, self.tags))
            
        except Exception as error:
            message = '{} ERROR: volume-id={} and tags={}\n{}'.format(log_prefix, volume_id, self.tags, error)
            self.logger.error(message)
            self.delete_snapshot(snapshot_name)
            snapshot = None
            raise Exception(message)

        return snapshot

    def _copy_snapshot(self, snapshot_id):
        return self.get_snapshot(snapshot_id)

    def _delete_snapshot(self, snapshot_id):
        log_prefix = '[SNAPSHOT] [DELETE]'

        try:
            snapshot_deletion_operation = self.compute_client.snapshots().delete(project=self.project_id, snapshot=snapshot_id).execute()
            
            self._wait('Waiting for snapshot {} to be deleted...'.format(snapshot_id),
                       (lambda operation_id, zonal_operation: self.wait_for_operation(operation_id, zonal_operation) == 'DONE'),
                       None,
                       snapshot_deletion_operation['name'], False)
            
            snapshot_exists = self.snapshot_exists(snapshot_id)

            # Check if snapshot exists, if not then it is successfully deleted, else raise exception
            if not snapshot_exists:
                self._remove_snapshot(snapshot_id)
                self.logger.info(
                    '{} SUCCESS: snapshot-id={}'.format(
                        log_prefix, snapshot_id))
                return True
            else:
                message = '{} ERROR: snapshot-id={}, snapshot still exists'.format(
                    log_prefix, snapshot_id)
                self.logger.error(message)
                raise Exception(message)
        except Exception as error:
            message = '{} ERROR: snapshot-id={}\n{}'.format(
                log_prefix, snapshot_id, error)
            self.logger.error(message)
            raise Exception(message)

    def wait_for_operation(self, operation_id, zonal_operation):
        if zonal_operation:
            result = self.compute_client.zoneOperations().get(
                project=self.project_id,
                zone=self.availability_zone,
                operation=operation_id).execute()
        else:
            result = self.compute_client.globalOperations().get(
                project=self.project_id,
                operation=operation_id).execute()
        return result['status']
