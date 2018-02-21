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
        self.device_path_template = '/dev/disk/by-id/google-{}'

        # +-> Create compute and storage clients
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
                    if 'instances' in instances_scoped_list:
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
        return json.loads(error.content.decode('utf-8'))['error']['code']

    def get_volume(self, volume_name):
        try:
            volume = self.compute_client.disks().get(
                project=self.project_id, zone=self.availability_zone, disk=volume_name).execute()
            return Volume(volume['name'], volume['status'], volume['sizeGb'])
        except Exception as error:
            self.logger.error(
                '[GCP] ERROR: Unable to get volume/disk {}.\n{}'.format(
                    volume_name, error))
            return None

    def volume_exists(self, volume_name):
        try:
            volume = self.compute_client.disks().get(
                project=self.project_id, zone=self.availability_zone, disk=volume_name).execute()
            return True
        except Exception as error:
            if self.get_http_error_code(error) == 404:
                return False
            else:
                message = '[GCP] ERROR: Unable to get disk {}.\n{}'.format(
                    volume_name, error)
                self.logger.error(message)
                raise Exception(message)

    def get_attached_volumes_for_instance(self, instance_id):
        try:
            instance = self.compute_client.instances().get(
                project=self.project_id,
                zone=self.availability_zone,
                instance=instance_id
            ).execute()

            volume_list = []
            for disk in instance['disks']:
                device = self._find_volume_device(disk['deviceName'])

                if device is not None:
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
        snapshot_creation_operation = None
        snapshot_name = self.generate_name_by_prefix(self.SNAPSHOT_PREFIX)
        try:
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
                       (lambda operation_id, zonal_operation: self.get_operation_status(
                           operation_id, zonal_operation) == 'DONE'),
                       None,
                       snapshot_creation_operation['name'], True)

            snapshot = self.get_snapshot(snapshot_name)
            if snapshot.status == 'READY':
                self._add_snapshot(snapshot.id)
                self.logger.info('{} SUCCESS: snapshot-id={}, volume-id={}, status={} with tags {}'.format(
                    log_prefix, snapshot.id, volume_id, snapshot.status, self.tags))
            else:
                message = '{} ERROR: snapshot-id={} status={}'.format(
                    log_prefix, snapshot_name, snapshot.status)
                raise Exception(message)
        except Exception as error:
            message = '{} ERROR: volume-id={} and tags={}\n{}'.format(
                log_prefix, volume_id, self.tags, error)
            self.logger.error(message)
            if snapshot or snapshot_creation_operation: 
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
                       (lambda operation_id, zonal_operation: self.get_operation_status(
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
        log_prefix = '[VOLUME] [CREATE]'
        volume = None
        disk_creation_operation = None
        disk_name = self.generate_name_by_prefix(self.DISK_PREFIX)
        try:
            disk_body = {
                'name': disk_name,
                'sizeGb': size,
                'labels': self.tags
                # default typ is pd-standard
            }
            if snapshot_id:
                disk_body['sourceSnapshot'] = 'global/snapshots/{}'.format(
                    snapshot_id)

            self.logger.info('Creating disk with details {}'.format(disk_body))

            disk_creation_operation = self.compute_client.disks().insert(
                project=self.project_id, zone=self.availability_zone, body=disk_body).execute()

            self._wait('Waiting for volume {} to get ready...'.format(disk_name),
                       (lambda operation_id, zonal_operation: self.get_operation_status(
                           operation_id, zonal_operation) == 'DONE'),
                       None,
                       disk_creation_operation['name'], True)

            volume = self.get_volume(disk_name)

            if volume.status == 'READY':
                self._add_volume(volume.id)
                self.logger.info('{} SUCCESS: volume-id={}, status={} with tags = {} '.format(
                    log_prefix, volume.id, volume.status, self.tags))
            else:
                message = '{} ERROR: volume-id={} status={}'.format(
                    log_prefix, volume.id, volume.status)
                raise Exception(message)           
        except Exception as error:
            message = '{} ERROR: volume-id={}, size={}\n{}'.format(
                log_prefix, disk_name, size, error)
            self.logger.error(message)
            if volume or disk_creation_operation: 
                self.delete_volume(disk_name)
                volume = None
            raise Exception(message)

        return volume

    def _delete_volume(self, volume_id):
        log_prefix = '[VOLUME] [DELETE]'
        try:
            disk_deletion_operation = self.compute_client.disks().delete(
                project=self.project_id, zone=self.availability_zone, disk=volume_id).execute()

            self._wait('Waiting for disk {} to be deleted...'.format(volume_id),
                       (lambda operation_id, zonal_operation: self.get_operation_status(
                           operation_id, zonal_operation) == 'DONE'),
                       None,
                       disk_deletion_operation['name'], True)

            volume_exists = self.volume_exists(volume_id)

            # Check if volume exists, if not then it is successfully deleted, else raise exception
            if not volume_exists:
                self._remove_volume(volume_id)
                self.logger.info(
                    '{} SUCCESS: volume-id={}'.format(
                        log_prefix, volume_id))
                return True
            else:
                message = '{} ERROR: volume-id={}, volume still exists'.format(
                    log_prefix, volume_id)
                self.logger.error(message)
                raise Exception(message)
        except Exception as error:
            message = '{} ERROR: volume-id={}\n{}'.format(
                log_prefix, volume_id, error)
            self.logger.error(message)
            raise Exception(message)

    def _create_attachment(self, volume_id, instance_id):
        log_prefix = '[ATTACHMENT] [CREATE]'
        attachment = None       
        try:
            volume = self.compute_client.disks().get(
                project=self.project_id, zone=self.availability_zone, disk=volume_id).execute()
            attached_disk_body = {
                'source': volume['selfLink'],
                'deviceName': volume_id
                # type: the default is PERSISTENT
            }

            disk_attach_operation = self.compute_client.instances().attachDisk(
                project=self.project_id, zone=self.availability_zone, instance=instance_id, body=attached_disk_body).execute()

            self._wait('Waiting for attachment of volume {} to get ready...'.format(volume_id),
                       (lambda operation_id, zonal_operation: self.get_operation_status(
                           operation_id, zonal_operation) == 'DONE'),
                       None,
                       disk_attach_operation['name'], True)

            # Here volume_id is the device name.
            # Raise exception if device returned in None,
            # as it might mean that disk was not attached properly
            device = self._find_volume_device(volume_id)
            if device is not None:
                self.logger.info(
                    'Attached volume-id={}, device={}'.format(volume_id, device))
                self._add_volume_device(volume_id, device)
            else:
                message = '{} ERROR: Device returned for volume-id={} is None'.format(
                    log_prefix, volume_id)
                raise Exception(message)

            attachment = Attachment(0, volume_id, instance_id)
            self._add_attachment(volume_id, instance_id)
            self.logger.info(
                '{} SUCCESS: volume-id={}, instance-id={}'.format(
                    log_prefix, volume_id, instance_id))
        except Exception as error:
            message = '{} ERROR: volume-id={}, instance-id={}\n{}'.format(
                log_prefix, volume_id, instance_id, error)
            self.logger.error(message)

            # The following lines are a workaround in case of inconsistency:
            # The attachment process may end with throwing an Exception,
            # but the attachment has been successful. Therefore, we must
            # check whether the volume is attached and if yes, trigger the detachment
            volume = self.compute_client.disks().get(
                project=self.project_id, zone=self.availability_zone, disk=volume_id).execute()
            if 'users' in volume and len(volume['users']) > 0:
                self.logger.warning('[VOLUME] [DELETE] Volume is attached although the attaching process failed, '
                                    'triggering detachment')
                attachment = True
            if attachment:
                self.delete_attachment(volume_id, instance_id)
                attachment = None
            raise Exception(message)

        return attachment

    def _delete_attachment(self, volume_id, instance_id):
        log_prefix = '[ATTACHMENT] [DELETE]'
        try:
            disk_detach_operation = self.compute_client.instances().detachDisk(
                project=self.project_id, zone=self.availability_zone, instance=instance_id, deviceName=volume_id).execute()

            self._wait('Waiting for attachment of volume {} to be deleted...'.format(volume_id),
                       (lambda operation_id, zonal_operation: self.get_operation_status(
                           operation_id, zonal_operation) == 'DONE'),
                       None,
                       disk_detach_operation['name'], True)

            self._remove_volume_device(volume_id)
            self._remove_attachment(volume_id, instance_id)
            self.logger.info(
                '{} SUCCESS: volume-id={}, instance-id={}'.format(log_prefix, volume_id, instance_id))
            return True
        except Exception as error:
            message = '{} ERROR: volume-id={}, instance-id={}\n{}'.format(
                log_prefix, volume_id, instance_id, error)
            self.logger.error(message)
            raise Exception(message)

    def _find_volume_device(self, volume_id):
        # As per https://cloud.google.com/compute/docs/reference/latest/instances/attachDisk
        # deviceName a unique device name of your choice that is reflected into the
        # /dev/disk/by-id/google-* tree of a Linux operating system running within the instance.
        # Here, volume_id parameter is actually a deviceName for GCP.
        try:
            device = None
            device_path = glob.glob(
                self.device_path_template.format(volume_id))
            if len(device_path) != 1:
                raise Exception(
                    'Expected number of device path not matching 1 != {} for disk {}'.format(
                        len(device_path), volume_id))

            device = self.shell(
                'readlink -e {}'.format(device_path[0])).rstrip()
            return device
        except Exception as error:
            self.logger.error(
                '[GCP] ERROR: Unable to find device for attached volume {}.{}'.format(
                    volume_id, error))
            return None

    def get_mountpoint(self, volume_id, partition=None):
        device = self._get_device_of_volume(volume_id)
        if not device:
            return None
        if partition:
            device += partition
        return device

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
                message = '{} ERROR: blob_to_upload={}, blob_target_name={}, container={}\n{}'.format(
                    log_prefix, blob_to_upload_path, blob_target_name, self.CONTAINER, error)
                self.logger.error(message)
                raise Exception(message)
        else:
            message = '{} ERROR: blob_to_upload={}, blob_target_name={}, container={}\n{}'.format(
                log_prefix, blob_to_upload_path, blob_target_name, self.CONTAINER, "Container not found or accessible")
            self.logger.error(message)
            raise Exception(message)

    def _download_from_blobstore(self, blob_to_download_name, blob_download_target_path, chunk_size=None):
        """Download file from blobstore.
        :type chunk_size: int
        :param chunk_size: If file size if greater than 5MB, it is recommended that, 
                           chunked downloads should be used.
                           To do so, pass chunk_size param to this function.
                           This must be a multiple of 256 KB per the API specification.
        """
        log_prefix = '[Google Cloud Storage] [DOWNLOAD]'
        if self.container:
            self.logger.info('{} Started to download the tarball to target.'.format(
                log_prefix, blob_download_target_path))
            try:
                blob = Blob(blob_to_download_name,
                            self.container, chunk_size=chunk_size)
                blob.download_to_filename(blob_download_target_path)
                self.logger.info('{} SUCCESS: blob_to_download={}, blob_target_name={}, container={}'
                                 .format(log_prefix, blob_to_download_name, self.CONTAINER,
                                         blob_download_target_path))
                return True
            except Exception as error:
                message = '{} ERROR: blob_to_download={}, blob_target_name={}, container={}\n{}'.format(
                    log_prefix, blob_to_download_name, blob_download_target_path, self.CONTAINER, error)
                self.logger.error(message)
                raise Exception(message)
        else:
            message = '{} ERROR: blob_to_download={}, blob_target_name={}, container={}\n{}'.format(
                log_prefix, blob_to_download_name, blob_download_target_path, self.CONTAINER, "Container not found or accessible")
            self.logger.error(message)
            raise Exception(message)

    def get_operation_status(self, operation_id, zonal_operation):
        """Get the operations status.
        The function returns the status of the operation and it can take values as PENDING, RUNNING, or DONE.
        Even after the operation status is returned as DONE, 
        one should check to see if the operation was successful and whether there were any errors.
        It throws an exception if there is any error.
        https://cloud.google.com/compute/docs/api/how-tos/api-requests-responses

        :type operation_id: string
        :param operation_id: The operation_id for the operation to be polled.

        :type zonal_operation: boolean
        :param zonal_operation: If the original request was to mutate a zonal resource, 
                                like an instance or a disk, Compute Engine returns a zoneOperations object. 
                                Similarly, regional and global resources return a regionOperations and globalOperations object.
                                We currently perform only zonal / global operation.
                                Pass zonal_operation = True if it is zonal operation,
                                else it is considered as global operation.
        """
        result = None
        try:
            if zonal_operation:
                result = self.compute_client.zoneOperations().get(
                    project=self.project_id,
                    zone=self.availability_zone,
                    operation=operation_id).execute()
            else:
                result = self.compute_client.globalOperations().get(
                    project=self.project_id,
                    operation=operation_id).execute()
        except Exception as error:
                message = '[Google Cloud Storage] [GET_OPERATION] ERROR: operation id={}\n{}'.format(
                    operation_id, error)
                self.logger.error(message)
                raise Exception(message)

        if result['status'] == 'DONE':
            if 'error' in result:
                message = '[Google Cloud Storage] [GET_OPERATION] ERROR: operation id={}\n{}'.format(
                    operation_id, result['error'])
                self.logger.error(message)
                raise Exception(message)

        return result if result == None else result['status']
