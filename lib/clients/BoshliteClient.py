import os
from random import randrange
from .BaseClient import BaseClient
from ..models.Snapshot import Snapshot
from ..models.Volume import Volume
from ..models.Attachment import Attachment


class BoshliteClient(BaseClient):
    def __init__(self, operation_name, configuration, directory_persistent, directory_work_list, poll_delay_time,
                 poll_maximum_time):
        super(BoshliteClient, self).__init__(operation_name, configuration, directory_persistent, directory_work_list,
                                        poll_delay_time, poll_maximum_time)
        
        # +-> Check whether the given container exists
        self.container = self.get_container()
        if not self.container:
            msg = 'Could not find or access the given container.'
            self.last_operation(msg, 'failed')
            raise Exception(msg)

        # +-> Get the id of the persistent volume attached to this instance
        self.availability_zone = self._get_availability_zone_of_server(configuration['instance_id'])
        if not self.availability_zone:
            msg = 'Could not retrieve the availability zone of the instance.'
            self.last_operation(msg, 'failed')
            raise Exception(msg)


    def _get_availability_zone_of_server(self, instance_id):
        return 'local'


    def get_container(self):
        try:
            path = os.path.join('/tmp/service_fabrik_backup_restore', self.CONTAINER)
            if not os.path.isdir(path):
                self.logger.info('[OBJECTSTORE] Created folder to store objects: {}'.format(self.CONTAINER))
                os.makedirs(path)
            return path
        except Exception as error:
            self.logger.error('[OBJECTSTORE] ERROR: Unable to find or access folder {}.\n{}'.format(self.CONTAINER, error))
            return None


    def get_persistent_volume_for_instance(self, instance_id):
        try:
            device = self.shell('cat /proc/mounts | grep {}'.format(self.DIRECTORY_PERSISTENT)).split(' ')[0][:10]
            volume = Volume(1, 'none', 1, device)
            self._add_volume_device(volume.id, device)
            return volume
        except:
            return None


    def _create_snapshot(self, volume_id):
        log_prefix = '[SNAPSHOT] [CREATE]'
        snapshot = None

        try:
            snapshot = {
                'id': randrange(1, 100),
                'volume_size': 1,
                'state': 'created'
            }

            self._wait('Waiting for snapshot {} to get ready...'.format(snapshot['id']),
                       lambda id: True,
                       None,
                       snapshot)

            snapshot = Snapshot(snapshot['id'], snapshot['volume_size'], snapshot['state'])
            self._add_snapshot(snapshot.id)
            self.logger.info('{} SUCCESS: snapshot-id={}, volume-id={}'.format(log_prefix, snapshot.id, volume_id))
        except Exception as error:
            message = '{} ERROR: volume-id={}\n{}'.format(log_prefix, volume_id, error)
            self.logger.error(message)
            if snapshot:
                self.delete_snapshot(snapshot.id)
                snapshot = None
            raise Exception(message)

        return snapshot


    def _delete_snapshot(self, snapshot_id):
        log_prefix = '[SNAPSHOT] [DELETE]'

        try:
            self._wait('Waiting for snapshot {} to be deleted...'.format(snapshot_id),
                       lambda id: True,
                       None,
                       snapshot_id)

            self._remove_snapshot(snapshot_id)
            self.logger.info('{} SUCCESS: snapshot-id={}'.format(log_prefix, snapshot_id))
            return True
        except Exception as error:
            message = '{} ERROR: snapshot-id={}\n{}'.format(log_prefix, snapshot_id, error)
            self.logger.error(message)
            raise Exception(message)


    def _create_volume(self, size, snapshot_id=None):
        log_prefix = '[VOLUME] [CREATE]'
        volume = None

        try:
            volume = {
                'id': randrange(1, 100),
                'size': size
            }

            self._wait('Waiting for volume {} to get ready...'.format(volume['id']),
                       lambda vol: True,
                       None,
                       volume)

            volume = Volume(volume['id'], 'none', volume['size'])
            self._add_volume(volume.id)
            self.logger.info('{} SUCCESS: volume-id={}'.format(log_prefix, volume.id))
        except Exception as error:
            message = '{} ERROR: size={}\n{}'.format(log_prefix, size, error)
            self.logger.error(message)
            if volume:
                self.delete_volume(volume.id)
                volume = None
            raise Exception(message)

        return volume


    def _delete_volume(self, volume_id):
        log_prefix = '[VOLUME] [DELETE]'

        try:
            self._wait('Waiting for volume {} to be deleted...'.format(volume_id),
                       lambda id: True,
                       None,
                       volume_id)

            self._remove_volume(volume_id)
            self.logger.info('{} SUCCESS: volume-id={}'.format(log_prefix, volume_id))
            return True
        except Exception as error:
            message = '{} ERROR: volume-id={}\n{}'.format(log_prefix, volume_id, error)
            self.logger.error(message)
            raise Exception(message)


    def _create_attachment(self, volume_id, instance_id):
        log_prefix = '[ATTACHMENT] [CREATE]'
        attachment = None

        try:
            device = '/dev/loop{}'.format(randrange(6,10))

            self._wait('Waiting for attachment of volume {} to get ready...'.format(volume_id),
                       lambda vol: True,
                       None,
                       None)

            self._add_volume_device(volume_id, device)
            attachment = Attachment(0, volume_id, instance_id)
            self._add_attachment(volume_id, instance_id)
            self.logger.info('{} SUCCESS: volume-id={}, instance-id={}'.format(log_prefix, volume_id, instance_id))
        except Exception as error:
            message = '{} ERROR: volume-id={}, instance-id={}\n{}'.format(log_prefix, volume_id, instance_id, error)
            self.logger.error(message)
            if attachment:
                self.delete_attachment(volume_id, instance_id)
                attachment = None
            raise Exception(message)

        return attachment


    def _delete_attachment(self, volume_id, instance_id):
        log_prefix = '[ATTACHMENT] [DELETE]'

        try:
            self._wait('Waiting for attachment of volume {} to be removed...'.format(volume_id),
                       lambda vol: True,
                       None,
                       None)

            self._remove_volume_device(volume_id)
            self._remove_attachment(volume_id, instance_id)
            self.logger.info('{} SUCCESS: volume-id={}, instance-id={}'.format(log_prefix, volume_id, instance_id))
            return True
        except Exception as error:
            message = '{} ERROR: volume-id={}, instance-id={}\n{}'.format(log_prefix, volume_id, instance_id, error)
            self.logger.error(message)
            raise Exception(message)


    def get_mountpoint(self, volume_id, partition=None):
        return self._get_device_of_volume(volume_id)


    def format_device(self, device):
        return True


    def mount_device(self, device, directory):
        return True


    def unmount_device(self, device):
        return True


    def _upload_to_blobstore(self, blob_to_upload_path, blob_target_name):
        log_prefix = '[OBJECTSTORE] [UPLOAD]'

        if self.container:
            self.logger.info('{} Started to upload the tarball to the object storage.'.format(log_prefix))

            self.shell('mkdir -p {}/{}'.format(self.container, blob_target_name.split('/', 1)[0]), False)

            if self.shell('cp -R {} {}/{}'.format(blob_to_upload_path, self.container, blob_target_name), False):
                self.logger.info('{} SUCCESS: blob_to_upload={}, blob_target_name={}, container={}'
                                .format(log_prefix, blob_to_upload_path, blob_target_name, self.CONTAINER))
                return True
            else:
                message = '{} ERROR: blob_to_upload={}, blob_target_name={}, container={}'.format(log_prefix,
                          blob_to_upload_path, blob_target_name, self.CONTAINER) 
                self.logger.error(message)
                raise Exception(message)


    def _download_from_blobstore(self, blob_to_download_name, blob_download_target_path):
        log_prefix = '[OBJECTSTORE] [DOWNLOAD]'

        if self.container:
            self.logger.info('{} Started to download the tarball to target.'.format(log_prefix,
                                                                                    blob_download_target_path))
            if self.shell('cp {}/{} {}'.format(self.container, blob_to_download_name, blob_download_target_path), False):
                self.logger.info('{} SUCCESS: blob_to_download={}, blob_target_name={}, container={}'
                                 .format(log_prefix, blob_to_download_name, self.CONTAINER,
                                         blob_download_target_path))
                return True
            else:
                message = '{} ERROR: blob_to_download={}, blob_target_name={}, container={}\n{}'.format(log_prefix,
                          blob_to_download_name, blob_download_target_path, self.CONTAINER, error)
                self.logger.error(message)
                raise Exception(message)
