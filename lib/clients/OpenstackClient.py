import time
import os
from keystoneauth1.identity.v3 import Password as KeystonePassword
from keystoneauth1.session import Session as KeystoneSession
from novaclient.client import Client as NovaClient
from cinderclient.client import Client as CinderClient
from swiftclient.client import Connection as SwiftClient
from swiftclient.service import SwiftService, SwiftUploadObject
from .BaseClient import BaseClient
from ..models.Snapshot import Snapshot
from ..models.Volume import Volume
from ..models.Attachment import Attachment


class OpenstackClient(BaseClient):
    def __init__(self, operation_name, configuration, directory_persistent, directory_work_list, poll_delay_time,
                 poll_maximum_time):
        super(OpenstackClient, self).__init__(operation_name, configuration, directory_persistent, directory_work_list,
                                              poll_delay_time, poll_maximum_time)
        self.__keystoneCredentials = {
            'username': configuration['username'],
            'password': configuration['password'],
            'auth_url': configuration['auth_url'],
            'user_domain_name': configuration['user_domain_name'],
            'project_id': configuration['tenant_id'],
            'project_name': configuration['tenant_name']
        }
        # The SAP certificiates required to establish a secure connection to
        # OpenStack are already pre-installed on the VMs (/etc/ssl/certs)
        certificates_path = os.getenv('SF_BACKUP_RESTORE_CERTS')
        self.__certificatesPath = '/etc/ssl/certs' if certificates_path is None else certificates_path
        self.nova = self.create_nova_client()
        self.cinder = self.create_cinder_client()
        self.swift = self.create_swift_client()
        self.swift.service = self.create_swift_service(self.swift.get_auth()[0])

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


    def create_keystone_session(self):
        try:
            auth = KeystonePassword(**self.__keystoneCredentials)
            session = KeystoneSession(auth=auth, verify=self.__certificatesPath)
            session.get_project_id()
            return session
        except Exception as error:
            raise Exception('Connection to Keystone failed: {}'.format(error))


    def create_nova_client(self):
        return NovaClient(version='2',
                                 session=self.create_keystone_session())


    def create_cinder_client(self):
        return CinderClient(version='2',
                                   session=self.create_keystone_session())


    def create_swift_client(self):
        try:
            swift = SwiftClient(auth_version='3',
                                           os_options=self.__keystoneCredentials,
                                           authurl=self.__keystoneCredentials['auth_url'],
                                           user=self.__keystoneCredentials['username'],
                                           key=self.__keystoneCredentials['password'],
                                           cacert=self.__certificatesPath)
            swift.get_auth()
            return swift
        except Exception as error:
            raise Exception('Connection to Swift failed: {}'.format(error))


    def create_swift_service(self, storage_url):
        try:
            return SwiftService(options={
                'user': self.__keystoneCredentials['username'],
                'key': self.__keystoneCredentials['password'],
                'os_project_id': self.__keystoneCredentials['project_id'],
                'os_project_name': self.__keystoneCredentials['project_name'],
                'os_project_domain_name': self.__keystoneCredentials['user_domain_name'],
                'os_tenant_id': self.__keystoneCredentials['project_id'],
                'os_tenant_name': self.__keystoneCredentials['project_name'],
                'os_user_domain_name': self.__keystoneCredentials['user_domain_name'],
                'os_auth_url': self.__keystoneCredentials['auth_url'],
                'os_cacert': self.__certificatesPath,
                'auth_version': '3',
                'os_storage_url': storage_url
            })
        except Exception as error:
            raise Exception('Connection to Swift failed: {}'.format(error))


    def _get_availability_zone_of_server(self, instance_id):
        try:
            return self.nova.servers.get(instance_id).to_dict()["OS-EXT-AZ:availability_zone"]
        except Exception as error:
            self.logger.error('[NOVA] ERROR: Unable to determine the availability zone of instance {}.\n{}'.format(instance_id, error))
            return None


    def get_container(self):
        try:
            return self.swift.head_container(self.CONTAINER)
        except Exception as error:
            self.logger.error('[SWIFT] ERROR: Unable to get container {}.\n{}'.format(self.CONTAINER, error))
            return None


    def get_snapshot(self, snapshot_id):
        try:
            snapshot = self.cinder.volume_snapshots.get(snapshot_id)
            return Snapshot(snapshot.id, snapshot.size, snapshot.status)
        except:
            return None


    def get_volume(self, volume_id):
        try:
            volume = self.cinder.volumes.get(volume_id)
            return Volume(volume.id, volume.status, volume.size)
        except:
            return None


    def get_attached_volumes_for_instance(self, instance_id):
        try:
            volumes = self.nova.volumes.get_server_volumes(instance_id)
            return [Volume(volume.id, 'none', self.cinder.volumes.get(volume.id).size, volume.device)
                    for volume in volumes]
        except:
            return []


    def get_persistent_volume_for_instance(self, instance_id):
        device = self.shell('cat /proc/mounts | grep {}'.format(self.DIRECTORY_PERSISTENT)).split(' ')[0][:-1]
        # Cut the last letter which marks the partition as we only need the 'plain' device name

        for volume in self.get_attached_volumes_for_instance(instance_id):
            if volume.device == device:
                self._add_volume_device(volume.id, device)
                return volume

        return None


    def _create_snapshot(self, volume_id):
        log_prefix = '[SNAPSHOT] [CREATE]'
        snapshot = None

        try:
            snapshot = self.cinder.volume_snapshots.create(
                volume_id,
                force=True,
                name='sf-backup-{}--{}'.format(time.strftime("%Y%m%d%H%M%S"), volume_id),
                description='Service-Fabrik: Automated backup'
            )

            self._wait('Waiting for snapshot {} to get ready...'.format(snapshot.id),
                       lambda snap: self.get_snapshot(snap.id).status == 'available',
                       None,
                       snapshot)

            snapshot = Snapshot(snapshot.id, snapshot.size, snapshot.status)
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
            self.cinder.volume_snapshots.delete(snapshot_id)

            self._wait('Waiting for snapshot {} to be deleted...'.format(snapshot_id),
                       lambda id: not self.get_snapshot(id),
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
            kwargs = {
                'size': size,
                'metadata': {
                    'created_by': 'service-fabrik-backup-restore'
                    },
                'availability_zone': self.availability_zone
            }
            if snapshot_id:
                kwargs['snapshot_id'] = snapshot_id

            volume = self.cinder.volumes.create(**kwargs)

            self._wait('Waiting for volume {} to get ready...'.format(volume.id),
                       lambda id: self.get_volume(id).status == 'available',
                       None,
                       volume.id)

            volume = Volume(volume.id, 'none', size)
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
            self.cinder.volumes.delete(volume_id)

            self._wait('Waiting for volume {} to be deleted...'.format(volume_id),
                       lambda id: not self.get_volume(id),
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
            attachment = self.nova.volumes.create_server_volume(instance_id, volume_id)

            self._wait('Waiting for attachment of volume {} to get ready...'.format(volume_id),
                       lambda id: self.get_volume(id).status == 'in-use',
                       None,
                       volume_id)

            self._add_volume_device(volume_id, self._find_volume_device(volume_id))
            attachment = Attachment(attachment.id, volume_id, instance_id)
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
            self.nova.volumes.delete_server_volume(instance_id, volume_id)

            self._wait('Waiting for attachment of volume {} to be removed...'.format(volume_id),
                       lambda id: self.get_volume(id).status == 'available',
                       None,
                       volume_id)

            self._remove_volume_device(volume_id)
            self._remove_attachment(volume_id, instance_id)
            self.logger.info('{} SUCCESS: volume-id={}, instance-id={}'.format(log_prefix, volume_id, instance_id))
            return True
        except Exception as error:
            message = '{} ERROR: volume-id={}, instance-id={}\n{}'.format(log_prefix, volume_id, instance_id, error)
            self.logger.error(message)
            raise Exception(message)


    def _find_volume_device(self, volume_id):
        self.shell('udevadm trigger', False)
        # create symbol links at /dev/disk/by-id/virtio-<uuid> pointing to the real device name
        time.sleep(3)
        result = self.shell('ls /dev/disk/by-id/ | grep {}'.format(volume_id[:20]), False)
        if result:
            result = '/dev/disk/by-id/{}'.format(result.split('\n')[0])
        return result


    def get_mountpoint(self, volume_id, partition=None):
        device = self._get_device_of_volume(volume_id)
        if not device:
            return None
        if partition:
            device += '-part{}'.format(partition)
        return device


    def _upload_to_blobstore(self, blob_to_upload_path, blob_target_name):
        log_prefix = '[SWIFT] [UPLOAD]'
        segment_size = (1 << 30) # 1 GiB segment size

        if self.container:
            self.logger.info('{} Started to upload the tarball to the object storage.'.format(log_prefix))
            upload_success = True
            try:
                blob_to_upload_object = SwiftUploadObject(blob_to_upload_path, object_name=blob_target_name)
                options = {
                    'segment_size': segment_size,
                    'segment_container': self.CONTAINER
                }
                for response in self.swift.service.upload(self.CONTAINER, [blob_to_upload_object], options):
                    # Swift client will try to create the container in case it is not existing - we want to
                    # hide the error which may occur due to missing priviledges for those operations
                    if response['action'] != 'create_container' and not response['success']:
                        upload_success = False
                        self.logger.error('{} ERROR: blob_to_upload={}, blob_target_name={}, container={}, {}, segment_size={}, action={}\n{}'
                                          .format(log_prefix, blob_to_upload_path, blob_target_name, self.CONTAINER, 
                                                  segment_size, response['action'], response['error']))
                    else:
                        self.logger.info('{} SUCCESS: blob_to_upload={}, blob_target_name={}, container={}, segment_size={}, action={}'
                                         .format(log_prefix, blob_to_upload_path, blob_target_name, self.CONTAINER, 
                                                  segment_size, response['action']))
                    self.logger.info('{} Waiting for next swift operation to finish...'.format(log_prefix))
            except Exception as error:
                upload_success = False
                self.logger.error('{} ERROR: blob_to_upload={}, blob_target_name={}, container={}, error={}'
                                  .format(log_prefix, blob_to_upload_path, blob_target_name, self.CONTAINER, error))

            if upload_success:
                self.logger.info('{} SUCCESS: blob_to_upload={}, blob_target_name={}, container={}'
                                 .format(log_prefix, blob_to_upload_path, blob_target_name, self.CONTAINER))
                return True

            message = '{} ERROR: blob={} could not be uploaded to container={}.'.format(log_prefix,
                      blob_to_upload_path, self.CONTAINER)
            self.logger.error(message)
            raise Exception(message)


    def _download_from_blobstore(self, blob_to_download_name, blob_download_target_path):
        log_prefix = '[SWIFT] [DOWNLOAD]'
        segment_size = 65536 # 64 KiB  

        if self.container:
            self.logger.info('{} Started to download the tarball to target {}.'.format(log_prefix,
                                                                                       blob_download_target_path))
            try:
                swift_object = self.swift.get_object(self.CONTAINER, blob_to_download_name, resp_chunk_size=segment_size)
                with open(blob_download_target_path, 'wb') as downloaded_file:
                    for chunk in swift_object[1]:
                        downloaded_file.write(chunk)

                self.logger.info('{} SUCCESS: blob_to_download={}, blob_target_name={}, container={}'
                                 .format(log_prefix, blob_to_download_name, blob_download_target_path,
                                         self.CONTAINER))
                return True
            except Exception as error:
                message = '{} ERROR: blob_to_download={}, blob_target_name={}, container={}\n{}'.format(log_prefix,
                          blob_to_download_name, blob_download_target_path, self.CONTAINER, error)
                self.logger.error(message)
                raise Exception(message)


    def _download_from_blobstore_and_pipe_to_process(self, process, blob_to_download_name, segment_size):
        swift_object = self.swift.get_object(self.CONTAINER, blob_to_download_name, resp_chunk_size=segment_size)
        for chunk in swift_object[1]:
            process.stdin.write(chunk)

        return True
