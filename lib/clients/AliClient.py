import oss2
from oss2.headers import RequestHeader
from .BaseClient import BaseClient

class AliClient(BaseClient):
    def __init__(self, operation_name, configuration, directory_persistent, directory_work_list, poll_delay_time,
                 poll_maximum_time):
        super(AliClient, self).__init__(operation_name, configuration, directory_persistent, directory_work_list,
                                        poll_delay_time, poll_maximum_time)
        if configuration['credhub_url'] is None:
            auth = oss2.Auth(configuration['access_key_id'], configuration['secret_access_key'])
            endpoint = configuration['endpoint']
        else
            credentials = self._get_credentials_from_credhub(configuration)
            auth = oss2.Auth(credentials['access_key_id'], credentials['secret_access_key'])
            endpoint = credentials['endpoint']
        # +-> Check whether the given container exists
        self.container = self.get_container(auth, endpoint)
        if not self.container:
            msg = 'Could not find or access the given container.'
            self.last_operation(msg, 'failed')
            raise Exception(msg)

    def get_container(self, auth, endpoint):
        try:
            container = oss2.Bucket(auth, endpoint, self.CONTAINER)
            # Test if the container is accessible
            key = '{}/{}'.format(self.BLOB_PREFIX, 'AccessTestByServiceFabrikPythonLibrary')
            container.put_object(key, 'This is a sample text')
            container.delete_object(key)
            return container
        except Exception as error:
            self.logger.error('[OSS] ERROR: Unable to find or access container {}.\n{}'.format(
                self.CONTAINER, error))
            return None

    def _upload_to_blobstore(self, blob_to_upload_path, blob_target_name):
        log_prefix = '[OSS] [UPLOAD]'

        if self.container:
            self.logger.info(
                '{} Started to upload the tarball to the object storage.'.format(log_prefix))
            try:
                requestHeader = RequestHeader()
                requestHeader.set_server_side_encryption("AES256")
                self.container.put_object_from_file(blob_target_name, blob_to_upload_path, headers=requestHeader)
                self.logger.info('{} SUCCESS: blob_to_upload={}, blob_target_name={}, container={}'
                                 .format(log_prefix, blob_to_upload_path, blob_target_name, self.CONTAINER))
                return True
            except Exception as error:
                message = '{} ERROR: blob_to_upload={}, blob_target_name={}, container={}\n{}'.format(
                    log_prefix, blob_to_upload_path, blob_target_name, self.CONTAINER, error)
                self.logger.error(message)
                raise Exception(message)

    def _download_from_blobstore(self, blob_to_download_name, blob_download_target_path):
        log_prefix = '[OSS] [DOWNLOAD]'

        if self.container:
            self.logger.info('{} Started to download the tarball to target{}.'
                             .format(log_prefix, blob_download_target_path))
            try:
                self.container.get_object(
                    blob_to_download_name, blob_download_target_path).read()
                self.logger.info('{} SUCCESS: blob_to_download={}, blob_target_name={}, container={}'.format(
                    log_prefix, blob_to_download_name, self.CONTAINER, blob_download_target_path))
                return True
            except Exception as error:
                message = '{} ERROR: blob_to_download={}, blob_target_name={}, container={}\n{}'.format(
                    log_prefix, blob_to_download_name, blob_download_target_path, self.CONTAINER, error)
                self.logger.error(message)
                raise Exception(message) 
