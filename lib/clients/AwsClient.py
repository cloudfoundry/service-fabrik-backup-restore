import boto3
from botocore.config import Config
from .BaseClient import BaseClient
from ..models.Snapshot import Snapshot
from ..models.Volume import Volume
from ..models.Attachment import Attachment


class AwsClient(BaseClient):
    def __init__(self, operation_name, configuration, directory_persistent, directory_work_list, poll_delay_time,
                 poll_maximum_time):
        super(AwsClient, self).__init__(operation_name, configuration, directory_persistent, directory_work_list,
                                        poll_delay_time, poll_maximum_time)
        if configuration['credhub_url'] is None:
            self.__setCredentials(
                configuration['access_key_id'], configuration['secret_access_key'], configuration['region_name'])
        else:
            self.logger.info('fetching creds from credhub')
            credentials = self._get_credentials_from_credhub(configuration)
            self.__setCredentials(
                credentials['access_key_id'], credentials['secret_access_key'], credentials['region_name'])

        self.max_retries = (configuration.get('max_retries') if
                            type(configuration.get('max_retries'))
                            == int else 10)
        # skipping some actions for blob operation
        if operation_name != 'blob_operation':
            self.ec2_config = Config(retries={'max_attempts': self.max_retries})
            self.ec2 = self.create_ec2_resource()
            self.ec2.client = self.create_ec2_client()
            self.formatted_tags = self.format_tags()
        self.s3 = self.create_s3_resource()
        self.s3.client = self.create_s3_client()
        # +-> Check whether the given container exists
        self.container = self.get_container(operation_name)
        if not self.container:
            msg = 'Could not find or access the given container.'
            self.last_operation(msg, 'failed')
            raise Exception(msg)

        # +-> Get the id of the persistent volume attached to this instance
        if operation_name != 'blob_operation':
            self.availability_zone = self._get_availability_zone_of_server(
                configuration['instance_id'])
            if not self.availability_zone:
                msg = 'Could not retrieve the availability zone of the instance.'
                self.last_operation(msg, 'failed')
                raise Exception(msg)

    def __setCredentials(self, access_key_id, secret_access_key, region_name):
        self.__awsCredentials = {
            'access_key_id': access_key_id,
            'secret_access_key': secret_access_key,
            'region_name': region_name
        }

    def format_tags(self):
        return [{'Key': key, 'Value': value} for key, value in self.tags.items()]

    def create_aws_session(self):
        return boto3.Session(
            aws_access_key_id=self.__awsCredentials['access_key_id'],
            aws_secret_access_key=self.__awsCredentials['secret_access_key'],
            region_name=self.__awsCredentials['region_name']
        )

    def create_ec2_resource(self):
        return self.create_aws_session().resource('ec2', config=self.ec2_config)

    def create_ec2_client(self):
        try:
            client = self.create_aws_session().client('ec2', config=self.ec2_config)
            return client
        except Exception as error:
            raise Exception('Connection to AWS EC2 failed: {}'.format(error))

    def create_s3_resource(self):
        return self.create_aws_session().resource('s3')

    def create_s3_client(self):
        return self.create_aws_session().client('s3')

    def _get_availability_zone_of_server(self, instance_id):
        try:
            instance = self.ec2.Instance(instance_id)
            instance.load()
            return instance.placement['AvailabilityZone']
        except Exception as error:
            self.logger.error(
                '[EC2] ERROR: Unable to determine the availability zone of instance {}.\n{}'.format(instance_id, error))
            return None

    def get_container(self, operation_name):
        try:
            container = self.s3.Bucket(self.CONTAINER)
            # Test if the container is accessible
            key = '{}/{}'.format(self.GUID, 'AccessTestByServiceFabrikPythonLibrary')
            container.put_object(Key=key)
            container.delete_objects(Delete={
               'Objects': [{
                    'Key': key
               }]
            })
            return container
        except Exception as error:
            self.logger.error('[S3] ERROR: Unable to find or access container {}.\n{}'.format(
                self.CONTAINER, error))
            return None

    def get_snapshot(self, snapshot_id):
        try:
            snapshot = self.ec2.Snapshot(snapshot_id)
            return Snapshot(snapshot.id, snapshot.volume_size, snapshot.start_time, snapshot.state)
        except:
            return None

    def get_volume(self, volume_id):
        try:
            volume = self.ec2.Volume(volume_id)
            return Volume(volume.id, volume.state, volume.size)
        except:
            return None

    def get_attached_volumes_for_instance(self, instance_id):
        instance = self.ec2.Instance(instance_id)
        try:
            return [Volume(details['VolumeId'], 'none', self.ec2.Volume(details['VolumeId']).size, details['Device'])
                    for volumes in instance.volumes.all()
                    for details in volumes.attachments]
        except:
            return []

    def get_persistent_volume_for_instance(self, instance_id):
        device = self.shell(
            'cat /proc/mounts | grep {}'.format(self.DIRECTORY_PERSISTENT)).split(' ')[0][:9]
        # http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/device_naming.html
        # --> /dev/xvdk on machine will be /dev/sdk on AWS
        device = device.replace('xv', 's')

        for volume in self.get_attached_volumes_for_instance(instance_id):
            if volume.device == device:
                self._add_volume_device(volume.id, device)
                return volume

        return None

    def _create_snapshot(self, volume_id, description='Service-Fabrik: Automated backup'):
        log_prefix = '[SNAPSHOT] [CREATE]'
        snapshot = None
        self.logger.info('{} START for volume id {} with tags {}'.format(
            log_prefix, volume_id, self.formatted_tags))
        try:
            snapshot = self.ec2.create_snapshot(
                VolumeId=volume_id,
                Description=description
            )

            self._wait('Waiting for snapshot {} to get ready...'.format(snapshot.id),
                       lambda snap: snap.state == 'completed',
                       snapshot.reload,
                       snapshot)

            snapshot = Snapshot(
                snapshot.id, snapshot.volume_size, snapshot.start_time, snapshot.state)
            self._add_snapshot(snapshot.id)

            self.ec2.create_tags(
                Resources=[
                    snapshot.id
                ],
                Tags=self.formatted_tags
            )

            self.logger.info('{} SUCCESS: snapshot-id={}, volume-id={} with tags {}'.format(
                log_prefix, snapshot.id, volume_id, self.formatted_tags))
        except Exception as error:
            message = '{} ERROR: volume-id={} and tags={}\n{}'.format(
                log_prefix, volume_id, self.formatted_tags, error)
            self.logger.error(message)
            if snapshot:
                self.delete_snapshot(snapshot.id)
                snapshot = None
            raise Exception(message)

        return snapshot

    def _copy_snapshot(self, snapshot_id):
        log_prefix = '[SNAPSHOT] [COPY]'
        snapshot = None
        ec2_snapshot = self.ec2.Snapshot(snapshot_id)
        try:
            snapshot = ec2_snapshot.copy(
                DryRun=False,
                SourceRegion=self.__awsCredentials['region_name'],
                Description='Service-Fabrik: Encrypted Backup',
                Encrypted=True
            )
            new_snapshot = self.ec2.Snapshot(snapshot['SnapshotId'])

            self._wait('Waiting for snapshot {} to get ready...'.format(new_snapshot.id),
                       lambda snap: snap.state == 'completed',
                       new_snapshot.reload,
                       new_snapshot)

            snapshot = Snapshot(
                new_snapshot.id, new_snapshot.volume_size, new_snapshot.start_time, new_snapshot.state)
            self.logger.info('{} SUCCESS: snapshot-id={}, unencrypted-snapshot_id={}'.format(
                log_prefix, snapshot.id, snapshot_id))
            self.output_json['snapshotId'] = snapshot.id

            self.ec2.create_tags(
                Resources=[
                    snapshot.id
                ],
                Tags=self.formatted_tags
            )

        except Exception as error:
            message = '{} ERROR: snapshot-id={}\n{}'.format(
                log_prefix, snapshot_id, error)
            self.logger.error(message)
            if snapshot:
                self.delete_snapshot(snapshot.id)
                snapshot = None
            raise Exception(message)

        return snapshot

    def _delete_snapshot(self, snapshot_id):
        log_prefix = '[SNAPSHOT] [DELETE]'

        try:
            self.ec2.client.delete_snapshot(
                SnapshotId=snapshot_id
            )

            self._wait('Waiting for snapshot {} to be deleted...'.format(snapshot_id),
                       lambda id: not self.get_snapshot(id),
                       None,
                       snapshot_id)

            self._remove_snapshot(snapshot_id)
            self.logger.info(
                '{} SUCCESS: snapshot-id={}'.format(log_prefix, snapshot_id))
            return True
        except Exception as error:
            message = '{} ERROR: snapshot-id={}\n{}'.format(
                log_prefix, snapshot_id, error)
            self.logger.error(message)
            raise Exception(message)

    def _create_volume(self, size, snapshot_id=None, volume_type='standard'):
        log_prefix = '[VOLUME] [CREATE]'
        volume = None

        try:
            kwargs = {
                'Size': size,
                'AvailabilityZone': self.availability_zone,
                'VolumeType': volume_type
            }
            if snapshot_id:
                kwargs['SnapshotId'] = snapshot_id

            volume = self.ec2.create_volume(**kwargs)

            self._wait('Waiting for volume {} to get ready...'.format(volume.id),
                       lambda vol: vol.state == 'available',
                       volume.reload,
                       volume)

            volume = Volume(volume.id, 'none', volume.size)
            self._add_volume(volume.id)

            self.ec2.create_tags(
                Resources=[
                    volume.id
                ],
                Tags=self.formatted_tags
            )

            self.logger.info('{} SUCCESS: volume-id={} volume-type={} with tags = {} '.format(
                log_prefix, volume.id, volume_type, self.formatted_tags))
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
            self.ec2.client.delete_volume(
                VolumeId=volume_id
            )

            self._wait('Waiting for volume {} to be deleted...'.format(volume_id),
                       lambda id: not self.get_volume(id),
                       None,
                       volume_id)

            self._remove_volume(volume_id)
            self.logger.info(
                '{} SUCCESS: volume-id={}'.format(log_prefix, volume_id))
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
            volume = self.ec2.Volume(volume_id)
            device = self._get_free_device()
            volume.attach_to_instance(
                InstanceId=instance_id,
                Device=device
            )

            self._wait('Waiting for attachment of volume {} to get ready...'.format(volume_id),
                       lambda vol: vol.attachments[0]['State'] == 'attached',
                       volume.reload,
                       volume)

            self._add_volume_device(volume_id, device)
            attachment = Attachment(0, volume_id, instance_id)
            self._add_attachment(volume_id, instance_id)
            self.logger.info(
                '{} SUCCESS: volume-id={}, instance-id={}'.format(log_prefix, volume_id, instance_id))
        except Exception as error:
            message = '{} ERROR: volume-id={}, instance-id={}\n{}'.format(
                log_prefix, volume_id, instance_id, error)
            self.logger.error(message)

            # The following lines are a workaround for a boto3 bug:
            # The attachment process (see _create_attachment() method) may end with throwing an Exception, e.g.
            # 'list index out of range', but the attachment has been successful. Therefore, we must
            # check whether the volume is attached and if yes, trigger the detachment
            volume = self.get_volume(volume_id)
            if volume.status == 'in-use':
                self.logger.warning('[VOLUME] [DELETE] Volume is in state {} although the attaching process failed, '
                                    'triggering detachment'.format(volume.status))
                attachment = True

            if attachment:
                self.delete_attachment(volume_id, instance_id)
                attachment = None
            raise Exception(message)

        return attachment

    def _delete_attachment(self, volume_id, instance_id):
        log_prefix = '[ATTACHMENT] [DELETE]'

        try:
            volume = self.ec2.Volume(volume_id)
            volume.detach_from_instance(
                InstanceId=instance_id,
                Force=True
            )

            self._wait('Waiting for attachment of volume {} to be removed...'.format(volume_id),
                       lambda vol: len(vol.attachments) == 0,
                       volume.reload,
                       volume)

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
        # Nothing to do for AWS as the device name is specified manually while attaching a volume and therefore known
        pass

    def get_mountpoint(self, volume_id, partition=None):
        device = self._get_device_of_volume(volume_id)
        if not device:
            return None
        # http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/device_naming.html
        # --> /dev/sdk on AWS will be /dev/xvdk on machine
        device = device.replace('s', 'xv')
        if partition:
            device += partition
        return device

    def _upload_to_blobstore(self, blob_to_upload_path, blob_target_name):
        log_prefix = '[S3] [UPLOAD]'

        if self.container:
            self.logger.info(
                '{} Started to upload the tarball to the object storage.'.format(log_prefix))
            try:
                self.container.upload_file(
                    blob_to_upload_path, blob_target_name)
                self.logger.info('{} SUCCESS: blob_to_upload={}, blob_target_name={}, container={}'
                                 .format(log_prefix, blob_to_upload_path, blob_target_name, self.CONTAINER))
                return True
            except Exception as error:
                message = '{} ERROR: blob_to_upload={}, blob_target_name={}, container={}\n{}'.format(
                    log_prefix, blob_to_upload_path, blob_target_name, self.CONTAINER, error)
                self.logger.error(message)
                raise Exception(message)

    def _download_from_blobstore(self, blob_to_download_name, blob_download_target_path):
        log_prefix = '[S3] [DOWNLOAD]'

        if self.container:
            self.logger.info('{} Started to download the tarball to target{}.'
                             .format(log_prefix, blob_download_target_path))
            try:
                self.container.download_file(
                    blob_to_download_name, blob_download_target_path)
                self.logger.info('{} SUCCESS: blob_to_download={}, blob_target_name={}, container={}'.format(
                    log_prefix, blob_to_download_name, self.CONTAINER, blob_download_target_path))
                return True
            except Exception as error:
                message = '{} ERROR: blob_to_download={}, blob_target_name={}, container={}\n{}'.format(
                    log_prefix, blob_to_download_name, blob_download_target_path, self.CONTAINER, error)
                self.logger.error(message)
                raise Exception(message)

    def _download_from_blobstore_and_pipe_to_process(self, process, blob_to_download_name, segment_size):
        s3_object_body = self.s3.Object(
            self.CONTAINER, blob_to_download_name).get()['Body']
        chunk = s3_object_body.read(segment_size)
        while chunk:
            process.stdin.write(chunk)
            chunk = s3_object_body.read(segment_size)

        return True
