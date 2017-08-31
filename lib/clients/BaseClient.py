import datetime
import json
import signal
import subprocess
import os
import sys
import time
import types
from retrying import retry
from ..logger import create_logger
from ..config import initialize


class BaseClient:
    def __init__(self, operation_name, configuration, directory_persistent, directory_work_list, poll_delay_time,
                 poll_maximum_time):
        self.OPERATION = operation_name
        self.GUID = configuration['backup_guid']
        self.CONTAINER = configuration['container']
        self.SECRET = configuration['secret']
        self.TYPE = configuration['type']
        self.JOB_NAME = configuration['job_name']
        self.DIRECTORY_PERSISTENT = directory_persistent
        self.DIRECTORY_WORK_LIST = directory_work_list
        self.DIRECTORY_DATA = '/var/vcap/data'
        self.FILE_MOUNTS = '/proc/mounts'
        assert len(
            self.OPERATION) > 0, 'No operation name (backup or restore) given.'
        assert len(
            self.DIRECTORY_PERSISTENT) > 0, 'Directory of the persistent volume not given.'
        assert len(self.DIRECTORY_WORK_LIST) > 0, 'Directory Worklist not given.'
        assert len(self.CONTAINER) > 0, 'No container given.'
        assert len(self.SECRET) > 0, 'No encryption secret given.'
        assert len(self.JOB_NAME) > 0, 'No service job name given.'

        # Handling abort signals
        self.__ABORT = False
        signal.signal(signal.SIGINT, self.__schedule_abortion)
        signal.signal(signal.SIGTERM, self.__schedule_abortion)

        # Writing the last operation file
        initialize(operation_name)
        self.LAST_OPERATION_DIRECTORY = os.getenv(
            'SF_BACKUP_RESTORE_LAST_OPERATION_DIRECTORY')
        self.LOG_DIRECTORY = os.getenv('SF_BACKUP_RESTORE_LOG_DIRECTORY')
        self.last_operation(
            'Initializing Backup & Restore Library ...', 'processing')
        self.logger = create_logger(self)
        self.output_json = dict()
        self.json_output()

        # Further initializations
        self.configuration = {
            'poll_delay_time': poll_delay_time if poll_delay_time is not None else 5,
            'poll_maximum_time': poll_maximum_time if poll_maximum_time is not None else 300
        }
        self.__snapshots_ids = []
        self.__volumes_ids = []
        self.__volumes_attached_ids = []
        self.__mounted_devices = []
        self.__devices = {}

    def __schedule_abortion(self, signum, frame):
        if self.__ABORT:
            self.logger.info(
                '[ABORT] REQUEST REJECTED: An abortion has already been scheduled.')
        else:
            self.logger.info('[ABORT] REQUEST ACCEPTED: Received SIGINT/SIGTERM ({}). Preparing a safe abortion...'
                             .format(signum), 'aborting')
            self.__ABORT = True

    def __abort(self):
        self.__ABORT = False
        # Prevent multiple abortion requests
        signal.signal(signal.SIGINT, lambda *args: None)
        signal.signal(signal.SIGTERM, lambda *args: None)

        # Abort
        self.logger.info(
            '[ABORT] It is now safe to abort the execution. Triggering the clean-up...')
        self.clean_up()
        self.start_service_job()
        self.wait_for_service_job_status('running')
        self.logger.info('[ABORT] Clean-up finished. Aborting now.')
        self.last_operation(
            'SIGINT/SIGTERM received: Abortion completed.', 'aborted')
        sys.exit()

    def __getattribute__(self, attr):
        method = object.__getattribute__(self, attr)
        #   Defining the methods which should check (BEFORE they get executed) whether the script was asked to abort its
        # execution. Basically, this list contains all methods which are used in backup.py or restore.py scripts. This is
        # done to ensure a 'safe' abortion process in terms of 'correctly cleaning up created resources'. All other methods
        # except those listed below will ignore the demand to abort as it may possibly be not safe in their current state.
        methods_allow_aborting = [
            'get_persistent_volume_for_instance', 'copy_snapshot', 'create_snapshot', 'create_volume',
            'create_attachment', 'get_mountpoint', 'copy_directory', 'delete_directory', 'create_directory', 'format_device',
            'mount_device', 'create_and_encrypt_tarball_of_directory', 'encrypt_file', 'upload_to_blobstore', 'unmount_device', 'delete_attachment',
            'delete_volume', 'delete_snapshot', 'download_from_blobstore', 'decrypt_and_extract_tarball_of_directory', 'decrypt_file'
        ]
        if isinstance(method, types.MethodType) and attr in methods_allow_aborting and self.__ABORT:
            self.__abort()
        else:
            return method

    def __retry(self, function, args):
        try:
            return self.__retry_rescuer(function, args)
        except Exception as error:
            return None

    # Retrying configuration parameters currently hard-coded - can be made configurable in future (if needed by anybody)
    @retry(stop_max_attempt_number=5, stop_max_delay=600000, wait_fixed=10000)
    def __retry_rescuer(self, function, args):
        try:
            return function(*args)
        except Exception as error:
            self.logger.error(error)
            raise error

    def _wait(self, log_message, success_condition_function, update_function, *success_condition_arguments):
        timeout = time.time() + self.configuration['poll_maximum_time']
        while not success_condition_function(*success_condition_arguments):
            if time.time() > timeout:
                raise Exception('Maximum polling time exceeded.')
            self.logger.info(log_message)
            time.sleep(self.configuration['poll_delay_time'])
            if update_function:
                update_function()

    def _add_volume_device(self, volume_id, device):
        self.__devices[volume_id] = device

    def _remove_volume_device(self, volume_id):
        self.__devices.pop(volume_id, None)

    def _add_snapshot(self, snapshot_id):
        self.__snapshots_ids.append(snapshot_id)

    def _remove_snapshot(self, snapshot_id):
        self.__snapshots_ids.remove(snapshot_id)

    def _add_volume(self, volume_id):
        self.__volumes_ids.append(volume_id)

    def _remove_volume(self, volume_id):
        self.__volumes_ids.remove(volume_id)

    def _add_attachment(self, volume_id, instance_id):
        self.__volumes_attached_ids.append((volume_id, instance_id))

    def _remove_attachment(self, volume_id, instance_id):
        self.__volumes_attached_ids.remove((volume_id, instance_id))

    def _add_mounted_device(self, device):
        self.__mounted_devices.append(device)

    def _remove_mounted_device(self, device):
        self.__mounted_devices.remove(device)

    def _get_free_device(self):
        sorted_devices = sorted(self.__devices.items(), key=lambda x: x[1])
        largest_device = sorted_devices[-1][1]
        return largest_device[:-1] + chr(ord(largest_device[-1:]) + 1)

    def _get_device_of_volume(self, volume_id):
        if volume_id in self.__devices:
            return self.__devices[volume_id]
        return None

    def _find_volume_device(self, volume_id):
        raise NotImplementedError()

    def initialize(self, message=None):
        """Write initial log statements and set the last operation state to 'processing'.

        :param message: (optional) a log statement

        :Example:
            ::

                iaas_client.initialize()
                iaas_client.initialize('A log messages before the program begins.')
        """
        self.logger.info('[{}] [START] (backup-type={})] Time: {}'.format(
            self.OPERATION.upper(), self.TYPE, time.strftime("%Y-%m-%dT%H-%M-%SZ")))
        self.logger.info('Backup guid: {}, Name of container: {}'.format(
            self.GUID, self.CONTAINER))
        if message:
            self.logger.info(message)

    def finalize(self, message=None):
        """Write final log statements and set the last operation state to 'succeeded'.

        :param message: (optional) a log statement

        :Example:
            ::

                iaas_client.finalize()
                iaas_client.finalize('A log messages before the program ends.')
        """
        # write stored json to file
        self.json_output()
        if message:
            self.logger.info(message)
        self.logger.info('Backup guid: {}, Name of container: {}'.format(
            self.GUID, self.CONTAINER))
        self.logger.info('[{}] [FINISH] (backup-type={})] Time: {}'.format(
            self.OPERATION.upper(), self.TYPE, time.strftime("%Y-%m-%dT%H-%M-%SZ")))
        self.last_operation('{} completed successfully'.format(
            self.OPERATION.title()), 'succeeded')

    def exit(self, message):
        """Clean up all created resources, start the service job and exit the process.

        :param message: a log statement

        :Example:
            ::

                iaas_client.exit('Something unpredictable happened.')
        """
        self.logger.error(message)
        self.clean_up()
        self.start_service_job()
        self.wait_for_service_job_status('running')
        self.last_operation(message, 'failed')
        sys.exit()

    def json_output(self):
        """Write the current output_json into file.

        :Example:
            ::

                iaas_client.json_output()
        """
        filepath = os.path.join(
            self.LOG_DIRECTORY, self.OPERATION + '.output.json')
        with open(filepath, 'w') as json_output_file:
            json_output_file.write(json.dumps(self.output_json))

    def last_operation(self, stage, state=None):
        """Write the current state and the current stage to the file storing information about the last operation.

        :param stage: a string with an explanation of the last operation
        :param state: (optional) change the current state (processing, aborting, succeeded, failed, aborted), default: processing


        :Example:
            ::

                iaas_client.last_operation('Creating Volume')
                iaas_client.last_operation('Backup completed successfully', 'succeeded')
        """
        SYMLINK = os.path.join(self.LAST_OPERATION_DIRECTORY,
                               self.OPERATION + '.lastoperation.json')
        BLUE = os.path.join(self.LAST_OPERATION_DIRECTORY,
                            self.OPERATION + '.lastoperation.blue.json')
        GREEN = os.path.join(self.LAST_OPERATION_DIRECTORY,
                             self.OPERATION + '.lastoperation.green.json')

        def read_link():
            return os.readlink(SYMLINK)

        def set_link(target):
            return self.shell('ln -sf {} {}'.format(target, SYMLINK), False)

        if state:
            self.last_operation_state = state
        content = json.dumps({
            'state': self.last_operation_state,
            'stage': stage,
            'updated_at': datetime.datetime.utcfromtimestamp(time.time()).strftime('%Y-%m-%dT%H:%M:%SZ')
        })
        filename = GREEN if read_link() == BLUE else BLUE
        with open(filename, 'w') as last_operation_file:
            last_operation_file.write(content)
        set_link(filename)

    def shell(self, command, log_command=True):
        """Execute a shell command.

        :param command: the shell command to be executed
        :param log_command: a boolean indicating whether the result of the command should be logged
        :returns: None if an error occurred, or the result string in case of success (if the result is None, then True is returned)

        :Example:
            ::

                iaas_client.shell('ps aux | grep blueprint', False)
        """
        try:
            result = subprocess.check_output(command, shell=True).decode()
            if log_command:
                self.logger.info(
                    '[SHELL] "{}" returned "{}".'.format(command, result))
            if not result:
                result = True
        except Exception as error:
            self.logger.error(
                '[SHELL ERROR] "{}" returned "{}".'.format(command, error))
            result = None
        return result

    def start_service_job(self):
        """Start the service job (basically executes 'monit start <name>').

        :Example:
            ::

                iaas_client.start_service_job()
        """
        return self.shell('monit start {}'.format(self.JOB_NAME))

    def stop_service_job(self):
        """Stop the service job (basically executes 'monit stop <name>').

        :Example:
            ::

                iaas_client.stop_service_job()
        """
        return self.shell('monit stop {}'.format(self.JOB_NAME))

    def get_service_job_status(self):
        status = self.shell('monit summary | grep \\\'{}\\\' | awk \'{{$1="";$2=""; sub("  ","")}}1\''
                            .format(self.JOB_NAME), False)
        status = 'Failed' if status is True else status.replace('\n', '')
        return status

    def wait_for_service_job_status(self, status):
        """Wait for the service job's monit status to be as provided in the 'status' parameter.

        :param status: the desired status to wait for
        :returns: True if the service job's monit status is as desired, and False otherwise.
        :rtype: Boolean

        :Example:
            ::

                iaas_client.wait_for_service_job_status('running')
        """
        timeout = time.time() + self.configuration['poll_maximum_time']
        while True:
            job_status = self.get_service_job_status().lower()
            if time.time() > timeout:
                raise Exception('Maximum polling time exceeded.')
            if job_status == status:
                self.logger.info(
                    '[SERVICE JOB] Job "{}" now has status "{}".'.format(self.JOB_NAME, status))
                return True
            elif job_status.find('fail') != -1 or job_status.find('does not') != -1 or job_status.find('timeout') != -1:
                self.logger.error('[SERVICE JOB] ERROR: monit status of job "{}" is "{}"'
                                  .format(self.JOB_NAME, job_status))
                return False
            else:
                self.logger.info(
                    'Waiting for job "{}" to have status "{}"...'.format(self.JOB_NAME, status))
                time.sleep(self.configuration['poll_delay_time'])

    def clean_up(self):
        """Detach and remove all volumes and snapshots.

        :Example:
            ::

                iaas_client.clean_up()
        """
        self.logger.info(
            '[CLEAN-UP] Begin cleaning up all created resources ...')
        for device in self.__mounted_devices[:]:
            self.logger.info(
                '[CLEAN-UP] Unmounting device with name {}'.format(device))
            self.unmount_device(device)
        for directory in self.DIRECTORY_WORK_LIST:
            self.logger.info(
                '[CLEAN-UP] Removing work directory {}'.format(directory))
            self.delete_directory(directory)
        for (volume_id, instance_id) in self.__volumes_attached_ids[:]:
            self.logger.info(
                '[CLEAN-UP] Removing attachment of volume {}'.format(volume_id))
            self.delete_attachment(volume_id, instance_id)
        for volume_id in self.__volumes_ids[:]:
            self.logger.info(
                '[CLEAN-UP] Removing volume with id {}'.format(volume_id))
            self.delete_volume(volume_id)
        for snapshot_id in self.__snapshots_ids[:]:
            self.logger.info(
                '[CLEAN-UP] Removing snapshot with id {}'.format(snapshot_id))
            self.delete_snapshot(snapshot_id)
        self.logger.info('[CLEAN-UP] ... finished.')

    def create_directory(self, directory):
        """Create a directory.

        :param directory: the name/path of the directory

        :Example:
            ::

                iaas_client.create_directory('/tmp/backup/files')
        """
        return self.shell('mkdir -p {}'.format(directory))

    def delete_directory(self, directory):
        """Delete a directory.

        :param directory: the name/path of the directory

        :Example:
            ::

                iaas_client.delete_directory('/tmp/backup/files')
        """
        return self.shell('rm -rf {}'.format(directory))

    def copy_directory(self, source, dest):
        """copy a directory.

        :param source: the name/path of the directory or file
        :param dest: the name/path of the directory or file

        :Example:
            ::

                iaas_client.copy_directory('/tmp/backup/files', '/var/vcap/store/files')
        """
        return self.shell('cp -r {} {}'.format(source, dest))

    def format_device(self, device):
        """Create an ext4 filesystem on a volume identified by its device name.

        :param device: the device name of the volume

        :Example:
            ::

                mountpoint_volume = iaas_client.get_mountpoint(volume.id)
                iaas_client.format_device(mountpoint_volume)
        """
        return self.shell('mkfs.ext4 {}'.format(device))

    def mount_device(self, device, directory):
        """Mount a volume to the filesystem.

        :param device: the device name of the volume
        :param directory: a directory in the filesystem to mount the device to

        :Example:
            ::

                mountpoint_volume = iaas_client.get_mountpoint(volume.id)
                iaas_client.mount_device(mountpoint_volume, '/tmp/backup')
        """
        cmd = self.shell('mount -t ext4 {} {}'.format(device, directory))
        if cmd:
            self._add_mounted_device(device)
        return cmd

    def unmount_device(self, device):
        """Unmount a volume from the filesystem.

        :param device: the device name of the volume

        :Example:
            ::

                mountpoint_volume = iaas_client.get_mountpoint(volume.id)
                iaas_client.unmount_device(mountpoint_volume)
        """
        cmd = self.shell('umount -l {}'.format(device))
        if cmd:
            self._remove_mounted_device(device)
        return cmd

    def create_and_encrypt_tarball_of_directory(self, directory_to_encrypt, encrypted_tarball_name):
        """Create a tarball of a directory and encrypt it with the secret provided at class instantiation.

        :param directory_to_encrypt: the path to the directory to be archived and encrypted
        :param encrypted_tarball_name: the path where to store the resulting archive

        :Example:
            ::

                iaas_client.create_and_encrypt_tarball_of_directory('/var/vcap/store/blueprint/files', '/tmp/backup/files.tar.gz')
        """
        self.logger.info(
            '[ENCRYPTION] Started creating, encrypting and copying a tarball ...')
        result = self.shell('tar -cpz -C {} . | gpg --symmetric --no-use-agent --cipher-algo aes256 --passphrase {} -o {}'
                            .format(directory_to_encrypt, self.SECRET, encrypted_tarball_name), False)
        self.logger.info('[ENCRYPTION] ... finished.')
        return result

    def decrypt_and_extract_tarball_of_directory(self, encrypted_tarball_name, directory_to_extract):
        """Decrypt with the secret provided at class instantiation, and extract an encrypted tarball of a directory.

        :param encrypted_tarball_name: the path to the directory to be decrypted and extracted
        :param directory_to_extract: the path to a directory where to move the extracted files

        :Example:
            ::

                iaas_client.decrypt_and_extract_tarball_of_directory('/tmp/restore/files.tar.gz', '/var/vcap/store/blueprint/files')
        """
        if not directory_to_extract or len(directory_to_extract) == 0:
            return None
        self.logger.info(
            '[DECRYPTION] Started cleaning the directory\'s old contents ...')
        if self.shell('rm -rf {}/*'.format(directory_to_extract)):
            self.logger.info(
                '[DECRYPTION] ... finished. Started decrypting and extracting a tarball ...')
            result = self.shell('gpg --no-use-agent --passphrase {} -d {} | tar -xzf - -C {}/'
                                .format(self.SECRET, encrypted_tarball_name, directory_to_extract), False)
            self.logger.info('[DECRYPTION] ... finished.')
            return result
        return None

    def encrypt_file(self, file_to_encrypt, encrypted_file_name):
        """Encrypt a given file with the secret provided at class instantiation.

        :param file_to_encrypt: the path to the file to be encrypted
        :param encrypted_file_name: the path where to store the encrypted result

        :Example:
            ::

                iaas_client.create_and_encrypt_file('/var/vcap/store/blueprint/list.txt', '/tmp/backup/list.txt.enc')
        """
        self.logger.info(
            '[ENCRYPTION] Started encrypting and copying a file ...')
        result = self.shell('gpg --symmetric --no-use-agent --cipher-algo aes256 --passphrase {} -o {} {}'
                            .format(self.SECRET, encrypted_file_name, file_to_encrypt), False)
        self.logger.info('[ENCRYPTION] ... finished.')
        return result

    def decrypt_file(self, encrypted_file_name, decrypted_file_name):
        """Decrypt an encrypted file with the secret provided at class instantiation.

        :param encrypted_file_name: the path of the encrypted file
        :param decrypted_file_name: the path of the decrypted file

        :Example:
            ::

                iaas_client.decrypt_and_extract_file('/tmp/restore/list.txt.enc', '/var/vcap/store/blueprint/list.txt')
        """
        if not decrypted_file_name or len(decrypted_file_name) == 0:
            return None
        self.logger.info('[DECRYPTION] Started removing the old file ...')
        if self.shell('rm -f {}'.format(decrypted_file_name)):
            self.logger.info(
                '[DECRYPTION] ... finished. Started decrypting a file ...')
            result = self.shell('gpg --no-use-agent --passphrase {} -d -o {} {}'
                                .format(self.SECRET, decrypted_file_name, encrypted_file_name), False)
            self.logger.info('[DECRYPTION] ... finished.')
            return result
        return None

    def get_container(self):
        """Retrieve the container provided at class instantiation.

        :returns: the container (in case of success) or None (in case of errors)

        :Example:
            ::

                iaas_client.get_container()
        """
        raise NotImplementedError()

    def get_availability_zone_of_server(self, instance_id):
        """Retrieve the availability zone of a server.

        :param instance_id: the IaaS id of the instance
        :returns: the availability zone
        :rtype: String

        :Example:
        ::

            iaas_client.get_availability_zone_of_server('948fb2e4-6e7f-11e6-8b77-86f30ca893d3')
        """
        raise NotImplementedError()

    def get_snapshot(self, snapshot_id):
        """Retrieve a snapshot.

        :param snapshot_id: the snapshot id
        :returns: the snapshot (in case of success) or None (in case of errors)
        :rtype: Snapshot object

        :Example:
            ::

                iaas_client.get_snapshot('ed1258f8-6e80-11e6-8b77-86f30ca893d3')
        """
        raise NotImplementedError()

    def get_volume(self, volume_id):
        """Retrieve a volume.

        :param volume_id: the volume id
        :returns: the volume (in case of success) or None (in case of errors)
        :rtype: Volume object

        :Example:
            ::

                iaas_client.get_volume('948fb2e4-6e7f-11e6-8b77-86f30ca893d3')
        """
        raise NotImplementedError()

    def get_attached_volumes_for_instance(self, instance_id):
        """Retrieve a list of volumes attached to an instance.

        :param instance_id: the instance id
        :returns: the list
        :rtype: List of Volume objects

        :Example:
            ::

                iaas_client.get_attached_volumes_for_instance('254989f8-6e81-11e6-8b77-86f30ca893d3')
        """
        raise NotImplementedError()

    def get_persistent_volume_for_instance(self, instance_id):
        """Retrieve the persistent volume attached to an instance.

        :param instance_id: the IaaS id of the instance
        :returns: the volume
        :rtype: Volume object

        :Example:
            ::

                iaas_client.get_persistent_volume_for_instance('948fb2e4-6e7f-11e6-8b77-86f30ca893d3')
        """
        raise NotImplementedError()

    def get_mountpoint(self, volume_id, partition=None):
        """Retrieve the device name of an attached volume.

        :param volume_id: the volume id
        :param partition: the partition number
        :type partition: String
        :returns: the device name (in case of success) or None (in case of errors)
        :rtype: String

        :Example:
            ::

                iaas_client.get_mountpoint('948fb2e4-6e7f-11e6-8b77-86f30ca893d3', '1')
        """
        raise NotImplementedError()

    def _create_snapshot(self):
        raise NotImplementedError()

    def _copy_snapshot(self):
        raise NotImplementedError()

    def _delete_snapshot(self):
        raise NotImplementedError()

    def _create_volume(self):
        raise NotImplementedError()

    def _delete_volume(self):
        raise NotImplementedError()

    def _create_attachment(self):
        raise NotImplementedError()

    def _delete_attachment(self):
        raise NotImplementedError()

    def _upload_to_blobstore(self):
        raise NotImplementedError()

    def _download_from_blobstore(self):
        raise NotImplementedError()

    def create_snapshot(self, *args):
        """Create a snapshot of a volume.

        :param volume_id: the id of the volume the snapshot should be created from
        :returns: the snapshot (in case of success) or None (in case of errors)
        :rtype: Snapshot object

        :Example:
            ::

                iaas_client.create_snapshot('948fb2e4-6e7f-11e6-8b77-86f30ca893d3')
        """
        return self.__retry(self._create_snapshot, args)

    def copy_snapshot(self, *args):
        """Create a copy of snapshot of a volume.

        :param snapshot_id: the id of the snapshot the new snapshot should be created from
        :returns: the snapshot (in case of success) or None (in case of errors)
        :rtype: Snapshot object

        :Example:
            ::

                iaas_client.create_snapshot('948fb2e4-6e7f-11e6-8b77-86f30ca893d3')
        """
        return self.__retry(self._copy_snapshot, args)

    def delete_snapshot(self, *args):
        """Delete a snapshot of a volume.

        :param snapshot_id: the id of the snapshot to be deleted

        :Example:
            ::

                iaas_client.delete_snapshot('ed1258f8-6e80-11e6-8b77-86f30ca893d3')
        """
        return self.__retry(self._delete_snapshot, args)

    def create_volume(self, *args):
        """Create a volume.

        :param size: the desired size of the volume (in GB)
        :param snapshot_id: (optional) the id of a snapshot to be copied from
        :returns: the volume (in case of success) or None (in case of errors)
        :rtype: Volume object

        :Example:
            ::

                iaas_client.create_volume('1')
                iaas_client.create_volume('1', 'ed1258f8-6e80-11e6-8b77-86f30ca893d3')
        """
        return self.__retry(self._create_volume, args)

    def delete_volume(self, *args):
        """Delete a volume (must be detached from all instances!).

        :param volume_id: the id of the volume to be deleted

        :Example:
            ::

                iaas_client.delete_volume('948fb2e4-6e7f-11e6-8b77-86f30ca893d3')
        """
        return self.__retry(self._delete_volume, args)

    def create_attachment(self, *args):
        """Attach a volume to an instance.

        :param volume_id: the id of the volume to be attached
        :param instance_id: the id of the instance
        :returns: the attachment (in case of success) or None (in case of errors)
        :rtype: Attachment object

        :Example:
            ::

                iaas_client.create_attachment('948fb2e4-6e7f-11e6-8b77-86f30ca893d3', '254989f8-6e81-11e6-8b77-86f30ca893d3')
        """
        return self.__retry(self._create_attachment, args)

    def delete_attachment(self, *args):
        """Detach a volume from an instance.

        :param volume_id: the id of the volume to be detached
        :param instance_id: the id of the instance

        :Example:
            ::

                iaas_client.delete_attachment('948fb2e4-6e7f-11e6-8b77-86f30ca893d3', '254989f8-6e81-11e6-8b77-86f30ca893d3')
        """
        return self.__retry(self._delete_attachment, args)

    def upload_to_blobstore(self, *args):
        """Upload a file to the BLOB storage.

        :param blob_to_upload_path: the path of the file to be uploaded
        :param blob_target_name: the name of the uploaded file in the BLOB storage

        :Example:
            ::

                iaas_client.upload_to_blobstore('/tmp/backup/files.tar.gz', 'files.tar.gz')
        """
        return self.__retry(self._upload_to_blobstore, args)

    def download_from_blobstore(self, *args):
        """Download a file from the BLOB storage.

        :param blob_to_download_name: the name of the file to be downloaded
        :param blob_download_target_path: the path where the file should be downloaded to

        :Example:
            ::

                iaas_client.download_from_blobstore('files.tar.gz', '/tmp/restore/files.tar.gz')
        """
        return self.__retry(self._download_from_blobstore, args)

    def download_from_blobstore_decrypt_extract(self, blob_to_download_name, blob_download_target_path):
        """Download a file from BLOB storage and pipe it to a subprocess for decryption and decompression.

        :param blob_to_download_name: the name of the file to be downloaded
        :param blob_download_target_path: the path where the file should be stored to

        :Example:
            ::

                iaas_client.download_from_blobstore_decrypt_extract('files.tar.gz.enc', '/store/service/data')
        """
        log_prefix = '[DOWNLOAD, DECRYPT, EXTRACT]'
        base_log = 'blob_to_download={}, blob_target_path={}, container={}'.format(
            blob_to_download_name, blob_download_target_path, self.CONTAINER)

        segment_size = 65536  # 64 KiB
        command = 'gpg --batch --cipher-algo aes256 --passphrase {} --decrypt | tar -xzf - -C {}/'.format(
            self.SECRET, blob_download_target_path)

        if self.__retry(self.get_container, []):
            try:
                self.logger.info('{} Started to download, decryt, extract and copy backup to {}.'.format(
                    log_prefix, blob_download_target_path))
                process = subprocess.Popen(
                    command, stdin=subprocess.PIPE, shell=True, bufsize=segment_size, universal_newlines=False)
                args = [process, blob_to_download_name, segment_size]
                self.__retry(
                    self._download_from_blobstore_and_pipe_to_process, args)
                process.stdin.close()
                exitcode = process.wait(timeout=None)
                if exitcode != 0:
                    raise Exception(
                        'Worker subprocess for decryption and extracting returned with non zero exit code.')

                self.logger.info('{} SUCCESS: {}'.format(log_prefix, base_log))
                return True
            except Exception as error:
                message = '{} Error: {}\n{}'.format(
                    log_prefix, base_log, error)
                self.logger.error(error)
                raise Exception(error)
