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
import glob


class GcpClient(BaseClient):
    def __init__(self, operation_name, configuration, directory_persistent, directory_work_list, poll_delay_time,
                 poll_maximum_time):
        super(GcpClient, self).__init__(operation_name, configuration, directory_persistent, directory_work_list,
                                        poll_delay_time, poll_maximum_time)

        self.__gcpCredentials = json.loads(configuration['credentials'])
        self.project_id = configuration['projectId']
        self.tags = {
            'instance_id': self.INSTANCE_ID,
            'job_name': self.JOB_NAME
        }

        self.compute_api_name = 'compute'
        self.compute_api_version = 'v1'
        self.snapshot_prefix = 'sf-snapshot'
        self.disk_prefix = 'sf-disk'
        self.device_path_template = '/dev/disk/by-id/google-{}'

        self.compute_client = self.create_compute_client()
        self.storage_client = self.create_storage_client()

        # +-> Check whether the given container exists and is accessible
        self.container = self.get_container()
        if not self.container:
            msg = 'Could not find or access the given container.'
            self.last_operation(msg, 'failed')
            raise Exception(msg)

        # +-> Get the availability zone of the instance
        self.availability_zone = self._get_availability_zone_of_server(
            configuration['instance_id'])
        if not self.availability_zone:
            msg = 'Could not retrieve the availability zone of the instance.'
            self.last_operation(msg, 'failed')
            raise Exception(msg)

    def create_compute_client(self):
        try:
            credentials = service_account.Credentials.from_service_account_info(
                self.__gcpCredentials)
            compute_client = discovery.build(
                self.compute_api_name, self.compute_api_version, credentials=credentials)
            return compute_client
        except Exception as error:
            raise Exception(
                'Creation of compute client failed: {}'.format(error))

    def create_storage_client(self):
        try:
            credentials = service_account.Credentials.from_service_account_info(
                self.__gcpCredentials)
            storage_client = storage.Client(self.project_id, credentials)
            return storage_client
        except Exception as error:
            raise Exception(
                'Creation of storage client failed: {}'.format(error))

    def _get_availability_zone_of_server(self, instance_id):
        try:
            expression = 'name eq ' + instance_id
            request = self.compute_client.instances().aggregatedList(
                project=self.project_id, filter=expression)
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

                request = self.compute_client.instances().aggregatedList_next(
                    previous_request=request, previous_response=response)

            self.logger.error(
                '[GCP] ERROR: Unable to determine the availability zone of instance{}.\n'.format(instance_id))
            return None
        except Exception as error:
            self.logger.error(
                '[GCP] ERROR: Unable to determine the availability zone of instance {}.\n{}'.format(instance_id, error))
            return None

    def get_container(self):
        try:
            container = self.storage_client.get_bucket(self.CONTAINER)
            # Test if the container is accessible
            blob = Blob('AccessTestByServiceFabrikPythonLibrary', container)
            blob.upload_from_string(
                'Sample Message for AccessTestByServiceFabrikPythonLibrary', content_type='text/plain')
            blob.delete()
            return container
        except Exception as error:
            self.logger.error('[GCP] [STORAGE] ERROR: Unable to find or access container {}.\n{}'.format(
                self.CONTAINER, error))
            return None

    def get_snapshot(self, snapshot_name):
        try:
            snapshot = self.compute_client.snapshots().get(
                project=self.project_id, snapshot=snapshot_name).execute()
            return Snapshot(snapshot['name'], snapshot['diskSizeGb'], snapshot['status'])
        except Exception as error:
            message = '[GCP] ERROR: Unable to get snapshot {}.\n{}'.format(
                snapshot_name, error)
            raise Exception(message)

    def snapshot_exists(self, snapshot_name):
        try:
            snapshot = self.compute_client.snapshots().get(
                project=self.project_id, snapshot=snapshot_name).execute()
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
        try:
            instance = self.compute_client.instances().get(
                project=self.project_id,
                zone=self.availability_zone,
                instance=instance_id
            ).execute()

            volume_list = []
            for disk in instance['disks']:
                # As per https://cloud.google.com/compute/docs/reference/latest/instances/attachDisk
                # deviceName a unique device name of your choice that is reflected into the
                # /dev/disk/by-id/google-* tree of a Linux operating system running within the instance.
                device = None
                device_path = glob.glob(
                    self.device_path_template.format(disk['deviceName']))
                if len(device_path) != 1:
                    raise Exception(
                        'Expected number of device path not matching 1 != {} for disk {}'.format(
                            len(device_path), disk['deviceName']))

                device = self.shell(
                    'readlink -e {}'.format(device_path[0])).rstrip()

                # Assuming the last part of the url is disk-name
                # Also, from https://cloud.google.com/compute/docs/regions-zones/
                # the disk and instance must belong to the same zone.
                # So, we can use instance zone.
                disk_name = disk['source'].rsplit('/', 1)[1]
                disk_details = self.compute_client.disks().get(
                    project=self.project_id,
                    zone=self.availability_zone,
                    disk=disk_name
                ).execute()
                volume_list.append(
                    Volume(disk_details['name'], disk_details['status'], disk_details['sizeGb'], device))
            return volume_list
        except Exception as error:
            self.logger.error(
                '[GCP] ERROR: Unable to find or access attached volume for instance_id {}.{}'.format(
                    instance_id, error))
            return []

    def get_persistent_volume_for_instance(self, instance_id):
        try:
            device = self.shell(
                'cat {} | grep {}'.format(self.FILE_MOUNTS, self.DIRECTORY_PERSISTENT)).split(' ')[0][:8]

            for volume in self.get_attached_volumes_for_instance(instance_id):
                if volume.device == device:
                    self._add_volume_device(volume.id, device)
                    return volume
            return None
        except Exception as error:
            self.logger.error(
                '[ERROR] [GET PRESISTENT VOLUME] Unable to find persistent volume for instance {}.{}'.format(
                    instance_id, error))
            return None

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
            self.logger.info('{} START for volume id {} with tags {} and snapshot name {}'.format(
                log_prefix, volume_id, self.tags, snapshot_name))
            snapshot_creation_operation = self.compute_client.disks().createSnapshot(
                project=self.project_id, zone=self.availability_zone, disk=volume_id, body=snapshot_body).execute()

            self._wait('Waiting for snapshot {} to get ready...'.format(snapshot_name),
                       (lambda operation_id, zonal_operation: self.wait_for_operation(
                           operation_id, zonal_operation) == 'DONE'),
                       None,
                       snapshot_creation_operation['name'], True)

            snapshot = self.get_snapshot(snapshot_name)
            self._add_snapshot(snapshot.id)
            self.logger.info('{} SUCCESS: snapshot-id={}, volume-id={} with tags {}'.format(
                log_prefix, snapshot.id, volume_id, self.tags))

        except Exception as error:
            message = '{} ERROR: volume-id={} and tags={}\n{}'.format(
                log_prefix, volume_id, self.tags, error)
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
            snapshot_deletion_operation = self.compute_client.snapshots().delete(
                project=self.project_id, snapshot=snapshot_id).execute()

            self._wait('Waiting for snapshot {} to be deleted...'.format(snapshot_id),
                       (lambda operation_id, zonal_operation: self.wait_for_operation(
                           operation_id, zonal_operation) == 'DONE'),
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

    def _create_volume(self, size, snapshot_id=None):
        # TODO
        pass

    def _delete_volume(self, volume_id):
        # TODO
        pass

    def _create_attachment(self, volume_id, instance_id):
        # TODO
        pass

    def _delete_attachment(self, volume_id, instance_id):
        # TODO
        pass

    def _find_volume_device(self, volume_id):
        # Nothing to do for AWS as the device name is specified manually while attaching a volume and therefore known
        pass

    def get_mountpoint(self, volume_id, partition=None):
        # TODO
        pass

    def _upload_to_blobstore(self, blob_to_upload_path, blob_target_name, chunk_size=None):
        """Upload file to blobstore.
        :type chunk_size: int
        :param chunk_size: If file size if greater than 5MB, it is recommended that, 
                           resumable uploads should be used.
                           If you wish to use resumable upload, pass chunk_size param to this function.
                           This must be a multiple of 256 KB per the API specification.
        """
        log_prefix = '[Google Cloud Storage] [UPLOAD]'

        if self.container:
            self.logger.info(
                '{} Started to upload the tarball to the object storage.'.format(log_prefix))
            try:
                blob = Blob(blob_target_name, self.container,
                            chunk_size=chunk_size)
                blob.upload_from_filename(blob_to_upload_path)
                self.logger.info('{} SUCCESS: blob_to_upload={}, blob_target_name={}, container={}'
                                 .format(log_prefix, blob_to_upload_path, blob_target_name, self.CONTAINER))
                return True
            except Exception as error:
                message = '{} ERROR: blob_to_upload={}, blob_target_name={}, container={}\n{}'.format(log_prefix,
                                                                                                      blob_to_upload_path, blob_target_name, self.CONTAINER, error)
                self.logger.error(message)
                raise Exception(message)
        else:
            message = '{} ERROR: blob_to_upload={}, blob_target_name={}, container={}\n{}'.format(log_prefix,
                                                                                                  blob_to_upload_path, blob_target_name, self.CONTAINER, "Container not found or accessible")
            self.logger.error(message)
            raise Exception(message)

    def _download_from_blobstore(self, blob_to_download_name, blob_download_target_path):
        # TODO
        pass

    def _download_from_blobstore_and_pipe_to_process(self, process, blob_to_download_name, segment_size):
        # TODO
        pass

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
