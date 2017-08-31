from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.compute.models import StorageAccountTypes
from azure.storage.blob import BlockBlobService
from msrestazure.azure_exceptions import CloudError
from azure.mgmt.compute.models import DiskCreateOption
from azure.mgmt.compute.models import DiskCreateOptionTypes
import time
import glob
from .BaseClient import BaseClient
from ..models.Snapshot import Snapshot
from ..models.Volume import Volume
from ..models.Attachment import Attachment


class AzureClient(BaseClient):
    def __init__(self, operation_name, configuration, directory_persistent, directory_work_list, poll_delay_time,
                 poll_maximum_time):
        super(AzureClient, self).__init__(operation_name, configuration, directory_persistent, directory_work_list,
                                          poll_delay_time, poll_maximum_time)
        self.subscription_id = configuration['subscription_id']
        self.__azureCredentials = ServicePrincipalCredentials(
            client_id=configuration['client_id'],
            secret=configuration['client_secret'],
            tenant=configuration['tenant_id']
        )
        self.resource_group = configuration['resource_group']
        self.storage_account_name = configuration['storageAccount']
        self.storage_account_key = configuration['storageAccessKey']

        self.block_blob_service = BlockBlobService(
            account_name=self.storage_account_name, account_key=self.storage_account_key)
        self.compute_client = ComputeManagementClient(
            self.__azureCredentials, self.subscription_id)

        # +-> Check whether the given container exists and accessible
        if (not self.get_container()) or (not self.access_container()):
            msg = 'Could not find or access the given container.'
            self.last_operation(msg, 'failed')
            raise Exception(msg)

        self.snapshot_prefix = 'sf-snapshot'
        self.disk_prefix = 'sf-disk'

    def get_container(self):
        try:
            container_props = self.block_blob_service.get_container_properties(
                self.CONTAINER)

            return container_props
        except Exception as error:
            self.logger.error('[Azure] [STORAGE] ERROR: Unable to find container {}.\n{}'.format(
                self.CONTAINER, error))
            return None

    def access_container(self):
        # Test if the container is accessible
        try:
            key = 'AccessTestByServiceFabrikPythonLibrary'
            self.block_blob_service.create_blob_from_text(
                self.CONTAINER,
                key,
                'This is a sample text'
            )
            self.block_blob_service.delete_blob(
                self.CONTAINER,
                key
            )
            return True
        except Exception as error:
            self.logger.error('[Azure] [STORAGE] ERROR: Unable to access container {}.\n{}'.format(
                self.CONTAINER, error))
            return False

    def get_snapshot(self, snapshot_name):
        try:
            snapshot = self.compute_client.snapshots.get(
                self.resource_group, snapshot_name)
            return Snapshot(snapshot.name, snapshot.disk_size_gb, snapshot.provisioning_state)
        except Exception as error:
            self.logger.error(
                '[Azure] ERROR: Unable to find or access snapshot {}.\n{}'.format(
                    snapshot_name, error))
            return None

    def get_volume(self, volume_name):
        try:
            volume = self.compute_client.disks.get(
                self.resource_group, volume_name)
            return Volume(volume.name, volume.provisioning_state, volume.disk_size_gb)
        except Exception as error:
            self.logger.error(
                '[Azure] ERROR: Unable to find or access volume/disk {}.\n{}'.format(
                    volume_name, error))
            return None

    def get_attached_volumes_for_instance(self, instance_id):
        instance = self.compute_client.virtual_machines.get(
            self.resource_group, instance_id
        )
        try:
            volumeList = []
            for disk in instance.storage_profile.data_disks:
                device = None
                if disk.lun == 0:
                    device = self.shell(
                        'cat {} | grep {} | grep /dev/ '
                        .format(self.FILE_MOUNTS, self.DIRECTORY_DATA)).split(' ')[0][:8]
                elif disk.lun == 1:
                    device = self.shell(
                        'cat {} | grep {}'
                        .format(self.FILE_MOUNTS, self.DIRECTORY_PERSISTENT)).split(' ')[0][:8]

                volumeList.append(
                    Volume(disk.name, 'none', disk.disk_size_gb, device))
            return volumeList
        except Exception as error:
            self.logger.error(
                '[Azure] ERROR: Unable to find or access attached volume for instance_id {}.{}'.format(
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
        self.logger.info(
            '{} START for volume id {}'.format(log_prefix, volume_id))
        try:
            disk_info = self.compute_client.disks.get(
                self.resource_group, volume_id)
            snapshot_name = '{}-{}'.format(self.snapshot_prefix,
                                           time.strftime("%Y%m%d%H%M%S"))
            snapshot_creation_operation = self.compute_client.snapshots.create_or_update(
                self.resource_group,
                snapshot_name,
                {
                    'location': disk_info.location,
                    'creation_data': {
                        'create_option': DiskCreateOption.copy,
                        'source_uri': disk_info.id
                    }
                }
            )

            self._wait('Waiting for snapshot {} to get ready...'.format(snapshot_name),
                       lambda operation: operation.done() is True,
                       None,
                       snapshot_creation_operation)

            snapshot_info = snapshot_creation_operation.result()
            self.logger.info(
                'Snapshot creation response: {}'.format(snapshot_info))
            snapshot = Snapshot(
                snapshot_info.name, snapshot_info.disk_size_gb, snapshot_info.provisioning_state)
            self._add_snapshot(snapshot.id)
            self.logger.info(
                '{} SUCCESS: snapshot-id={}, volume-id={}'.format(log_prefix, snapshot.id, volume_id))
            self.output_json['snapshotId'] = snapshot.id
        except Exception as error:
            message = '{} ERROR: volume-id={}\n{}'.format(
                log_prefix, volume_id, error)
            self.logger.error(message)
            if snapshot:
                self.delete_snapshot(snapshot.id)
                snapshot = None
            raise Exception(message)

        return snapshot

    def _copy_snapshot(self, snapshot_id):
        return self.get_snapshot(snapshot_id)

    def _delete_snapshot(self, snapshot_id):
        log_prefix = '[SNAPSHOT] [DELETE]'

        try:
            snapshot_deletion_operation = self.compute_client.snapshots.delete(
                self.resource_group, snapshot_id)
            # TODO: can be implemented the following wait as 'operation.done() is True'
            self._wait('Waiting for snapshot {} to be deleted...'.format(snapshot_id),
                       lambda id: not self.get_snapshot(id),
                       None,
                       snapshot_id)
            snapshot_delete_response = snapshot_deletion_operation.result()
            self._remove_snapshot(snapshot_id)
            self.logger.info(
                '{} SUCCESS: snapshot-id={}\n{}'.format(
                    log_prefix, snapshot_id, snapshot_delete_response))
            return True
        except Exception as error:
            message = '{} ERROR: snapshot-id={}\n{}'.format(
                log_prefix, snapshot_id, error)
            self.logger.error(message)
            raise Exception(message)

    def _create_volume(self, size, snapshot_id=None):
        log_prefix = '[VOLUME] [CREATE]'
        volume = None

        try:
            disk_creation_operation = None
            disk_name = None
            if snapshot_id is not None:
                snapshot = self.compute_client.snapshots.get(
                    self.resource_group, snapshot_id)
                disk_name = '{}-{}'.format(self.disk_prefix,
                                           time.strftime("%Y%m%d%H%M%S"))
                disk_creation_operation = self.compute_client.disks.create_or_update(
                    self.resource_group,
                    disk_name,
                    {
                        'location': snapshot.location,
                        'creation_data': {
                            'create_option': DiskCreateOption.copy,
                            'source_uri': snapshot.id
                        }
                    }
                )
            else:
                disk_name = '{}-{}'.format(self.disk_prefix,
                                           time.strftime("%Y%m%d%H%M%S"))
                disk_creation_operation = self.compute_client.disks.create_or_update(
                    self.resource_group,
                    disk_name,
                    {
                        'location': snapshot.location,
                        'disk_size_gb': size,
                        'creation_data': {
                            'create_option': DiskCreateOption.empty
                        },
                        'account_type': StorageAccountTypes.standard_lrs
                    }
                )

            self._wait('Waiting for volume {} to get ready...'.format(disk_name),
                       lambda operation: operation.done() is True,
                       None,
                       disk_creation_operation)

            disk = disk_creation_operation.result()
            volume = Volume(disk.name, 'none', disk.disk_size_gb)
            self._add_volume(volume.id)
            self.logger.info(
                '{} SUCCESS: volume-id={}'.format(log_prefix, volume.id))
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
            disk_deletion_operation = self.compute_client.disks.delete(
                self.resource_group, volume_id)

            self._wait('Waiting for volume {} to be deleted...'.format(volume_id),
                       lambda operation: operation.done() is True,
                       None,
                       disk_deletion_operation)
            delete_response = disk_deletion_operation.result()
            self._remove_volume(volume_id)
            self.logger.info(
                '{} SUCCESS: volume-id={}\n{}'.format(
                    log_prefix, volume_id, delete_response))
            return True
        except Exception as error:
            message = '{} ERROR: volume-id={}\n{}'.format(
                log_prefix, volume_id, error)
            self.logger.error(message)
            raise Exception(message)

    def _create_attachment(self, volume_id, instance_id):
        log_prefix = '[ATTACHMENT] [CREATE]'
        attachment = None

        try:
            virtual_machine = self.compute_client.virtual_machines.get(
                self.resource_group,
                instance_id
            )
            volume = self.compute_client.disks.get(
                self.resource_group, volume_id)
            all_data_disks = virtual_machine.storage_profile.data_disks
            # traversing  through all disks and finding next balnk lun
            next_lun = 0
            for disk in all_data_disks:
                if disk.lun == next_lun:
                    next_lun += 1

            existing_devices_path = glob.glob(
                '/sys/bus/scsi/devices/*:*:*:{}/block'.format(next_lun))
            virtual_machine.storage_profile.data_disks.append({
                'lun': next_lun,
                'name': volume.name,
                'create_option': DiskCreateOptionTypes.attach,
                'managed_disk': {
                    'id': volume.id
                }
            })

            disk_attach_operation = self.compute_client.virtual_machines.create_or_update(
                self.resource_group,
                virtual_machine.name,
                virtual_machine
            )

            self._wait('Waiting for attachment of volume {} to get ready...'.format(volume_id),
                       lambda operation: operation.done() is True,
                       None,
                       disk_attach_operation)

            updated_vm = disk_attach_operation.result()
            # TODO: Need to take care how device is handled
            all_devices_path = glob.glob(
                '/sys/bus/scsi/devices/*:*:*:{}/block'.format(next_lun))
            new_devices_path = list(set(all_devices_path) -
                                    set(existing_devices_path))
            if len(new_devices_path) > 1:
                raise Exception(
                    'Found more than one new devices while attaching volume!')
            device = '/dev/{}'.format(self.shell(
                'ls {}'.format(new_devices_path[0])).rstrip())
            self._add_volume_device(volume_id, device)
            attachment = Attachment(0, volume_id, instance_id)
            self._add_attachment(volume_id, instance_id)
            self.logger.info(
                '{} SUCCESS: volume-id={}, instance-id={}\n Updated vm:{}'.format(
                    log_prefix, volume_id, instance_id, updated_vm))
        except Exception as error:
            message = '{} ERROR: volume-id={}, instance-id={}\n{}'.format(
                log_prefix, volume_id, instance_id, error)
            self.logger.error(message)

            # The following lines are a workaround in case of inconsistency:
            # The attachment process may end with throwing an Exception, e.g.
            # 'list index out of range', but the attachment has been successful. Therefore, we must
            # check whether the volume is attached and if yes, trigger the detachment
            # TODO : Following need to take care: volume.status is not in-use in case of Azure
            volume = self.compute_client.disks.get(
                self.resource_group, volume_id)
            if volume.managed_by is not None:
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
            virtual_machine = self.compute_client.virtual_machines.get(
                self.resource_group,
                instance_id
            )

            data_disks = virtual_machine.storage_profile.data_disks
            data_disks[:] = [
                disk for disk in data_disks if disk.name != volume_id]
            disk_detach_operation = self.compute_client.virtual_machines.create_or_update(
                self.resource_group,
                virtual_machine.name,
                virtual_machine
            )

            self._wait('Waiting for attachment of volume {} to be removed...'.format(volume_id),
                       lambda operation: operation.done() is True,
                       None,
                       disk_detach_operation)

            updated_vm = disk_detach_operation.result()
            self._remove_volume_device(volume_id)
            self._remove_attachment(volume_id, instance_id)
            self.logger.info(
                '{} SUCCESS: volume-id={}, instance-id={}\n updated vm: {}'.format(
                    log_prefix, volume_id, instance_id, updated_vm))
            return True
        except Exception as error:
            message = '{} ERROR: volume-id={}, instance-id={}\n{}'.format(
                log_prefix, volume_id, instance_id, error)
            self.logger.error(message)
            raise Exception(message)

    def _find_volume_device(self, volume_id):
        # Nothing to do for Azure as the device name is specified manually while attaching a volume and therefore known
        pass

    def get_mountpoint(self, volume_id, partition=None):
        device = self._get_device_of_volume(volume_id)
        if not device:
            return None
        if partition:
            device += partition
        return device

    def _upload_to_blobstore(self, blob_to_upload_path, blob_target_name):
        log_prefix = '[AZURE STORAGE CONTAINER] [UPLOAD]'
        self.logger.info(
            '{} Started to upload the tarball to the object storage.'.format(log_prefix))
        try:
            self.block_blob_service.create_blob_from_path(
                self.CONTAINER,
                blob_target_name,
                blob_to_upload_path)
            # TODO: need to check above 'blob_target_name'
            self.logger.info('{} SUCCESS: blob_to_upload={}, blob_target_name={}, container={}'.format(
                log_prefix, blob_to_upload_path, blob_target_name, self.CONTAINER))
            return True
        except Exception as error:
            message = '{} ERROR: blob_to_upload={}, blob_target_name={}, container={}\n{}'.format(
                log_prefix, blob_to_upload_path, blob_target_name, self.CONTAINER, error)
            self.logger.error(message)
            raise Exception(message)

    def _download_from_blobstore(self, blob_to_download_name, blob_download_target_path):
        log_prefix = '[AZURE STORAGE CONTAINER] [DOWNLOAD]'
        self.logger.info('{} Started to download the tarball to target {}.'.format(
            log_prefix,
            blob_download_target_path))
        try:
            self.block_blob_service.get_blob_to_path(
                self.CONTAINER, blob_to_download_name, blob_download_target_path)
            self.logger.info('{} SUCCESS: blob_to_download={}, blob_target_name={}, container={}'
                             .format(log_prefix, blob_to_download_name, self.CONTAINER,
                                     blob_download_target_path))
            return True
        except Exception as error:
            message = '{} ERROR: blob_to_download={}, blob_target_name={}, container={}\n{}'.format(
                log_prefix,
                blob_to_download_name, blob_download_target_path, self.CONTAINER, error)
            self.logger.error(message)
            raise Exception(message)

    def _download_from_blobstore_and_pipe_to_process(self, process, blob_to_download_name, segment_size):
        self.block_blob_service.get_blob_to_stream(
            self.CONTAINER, blob_to_download_name, process.stdin,
            snapshot=None, start_range=0, end_range=segment_size - 1)
        return True
